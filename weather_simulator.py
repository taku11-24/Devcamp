import os
import requests
import numpy as np
from datetime import datetime, timezone, timedelta
import time
from typing import List, Dict, Any
from dotenv import load_dotenv

# モジュール読み込み時に一度だけ環境変数をロード
load_dotenv()

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """ Calculate distance (km) between two lat/lon points using Haversine formula """
    R = 6371  # Earth radius (km)
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

# --- Category translation function ---
def wmo_code_to_description(code: int) -> str:
    """ Convert Open-Meteo WMO code into 3 categories: Sunny, Cloudy, Rain """
    # Category 1: Sunny
    if code in [0, 1]:
        return 'Sunny'
    # Category 2: Cloudy (including fog)
    elif code in [2, 3, 45, 48]:
        return 'Cloudy'
    # Category 3: Rain (including drizzle, snow, thunderstorms)
    elif code in [
        51, 53, 55,  # Drizzle
        61, 63, 65,  # Rain
        71, 73, 75,  # Snow
        80, 81, 82,  # Showers
        95, 96, 99   # Thunderstorm
    ]:
        return 'Rain'
    # Default: treat unknown codes as Cloudy
    else:
        return 'Cloudy'

def _get_weather_for_points_yahoo(points: List[Dict]) -> List[Dict]:
    """ [Yahoo! API] Get weather summary and rainfall for multiple points """
    base_url = "https://map.yahooapis.jp/weather/V1/place"
    api_key = os.getenv("YAHOO_API_KEY")
    if not api_key:
        print("Warning: YAHOO_API_KEY is not set. Skipping Yahoo! API processing.")
        for p in points:
            p['weather'] = {'description': 'No forecast', 'rainfall_mm_h': None}
        return points

    chunk_size = 10
    for i in range(0, len(points), chunk_size):
        chunk = points[i:i + chunk_size]
        coordinate_str = " ".join(f"{p['lon']},{p['lat']}" for p in chunk)
        params = {"coordinates": coordinate_str, "output": "json", "appid": api_key, "interval": "10"}

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            weather_data = response.json()

            if 'Feature' not in weather_data:
                raise ValueError("API response does not contain 'Feature' key.")

            for point, weather_feature in zip(chunk, weather_data.get('Feature', [])):
                all_forecasts = weather_feature.get('Property', {}).get('WeatherList', {}).get('Weather', [])
                if not all_forecasts:
                    point['weather'] = {'description': 'No forecast', 'rainfall_mm_h': None}
                    continue

                best_forecast = min(
                    all_forecasts,
                    key=lambda f: abs(point['timestamp'] - datetime.strptime(f['Date'], '%Y%m%d%H%M').timestamp())
                )
                rainfall = best_forecast.get('Rainfall', 0.0)
                point['weather'] = {
                    # Yahoo API only indicates presence of precipitation. "No rain" will be refined later by Open-Meteo.
                    'description': "Rain" if rainfall > 0 else "No rain",
                    'rainfall_mm_h': rainfall,
                }
        except requests.exceptions.RequestException as e:
            print(f"Warning: Failed to fetch data from Yahoo! Weather API: {e}")
            for point in chunk:
                point['weather'] = {'description': 'No forecast', 'rainfall_mm_h': None}
    return points

def _get_open_meteo_data(point: Dict) -> Dict[str, Any] or None:
    """ [Open-Meteo API] Get temperature and weather code (with retry) """
    dt_object_utc = datetime.fromtimestamp(point['timestamp'], tz=timezone.utc)
    is_past = dt_object_utc.date() < datetime.now(timezone.utc).date()
    
    base_url = "https://archive-api.open-meteo.com/v1/archive" if is_past else "https://api.open-meteo.com/v1/forecast"
    params = {'latitude': point['lat'], 'longitude': point['lon'], 'hourly': 'temperature_2m,weather_code', 'timezone': 'auto'}
    if is_past:
        date_str = dt_object_utc.strftime('%Y-%m-%d')
        params.update({'start_date': date_str, 'end_date': date_str})

    for attempt in range(3):
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            weather_data = response.json()
            
            hourly = weather_data.get('hourly', {})
            api_times_str = hourly.get('time', [])
            if not api_times_str: return None
            
            api_times = [datetime.fromisoformat(t) for t in api_times_str]
            target_dt = datetime.fromtimestamp(point['timestamp'], tz=api_times[0].tzinfo)

            closest_time_idx = min(range(len(api_times)), key=lambda i: abs(api_times[i] - target_dt))
            
            return {
                'temperature': hourly['temperature_2m'][closest_time_idx],
                'description': wmo_code_to_description(hourly['weather_code'][closest_time_idx])
            }
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                print(f"Error: Failed to fetch data from Open-Meteo API: {e}")
                return None
            time.sleep(1)
    return None

def _sample_route_by_distance(route_data_with_timestamps: List, interval_km: float) -> List[Dict]:
    """ Sample route at regular distance intervals """
    if not route_data_with_timestamps: return []
    
    sampled_points = []
    cumulative_distance = 0.0
    next_sample_distance = interval_km
    
    first_point = route_data_with_timestamps[0]
    sampled_points.append({'lat': first_point[0], 'lon': first_point[1], 'timestamp': int(first_point[2]), 'distance_km': 0.0})
    
    for i in range(1, len(route_data_with_timestamps)):
        prev_pt, curr_pt = route_data_with_timestamps[i-1], route_data_with_timestamps[i]
        distance = haversine(prev_pt[0], prev_pt[1], curr_pt[0], curr_pt[1])
        
        while cumulative_distance + distance >= next_sample_distance:
            fraction = (next_sample_distance - cumulative_distance) / distance
            interp_lat = prev_pt[0] + fraction * (curr_pt[0] - prev_pt[0])
            interp_lon = prev_pt[1] + fraction * (curr_pt[1] - prev_pt[1])
            interp_ts = prev_pt[2] + fraction * (curr_pt[2] - prev_pt[2])
            
            sampled_points.append({'lat': interp_lat, 'lon': interp_lon, 'timestamp': int(interp_ts), 'distance_km': round(next_sample_distance, 2)})
            next_sample_distance += interval_km
        cumulative_distance += distance
    return sampled_points

def _generate_weather_report(route_data_with_timestamps: List, interval_km: float) -> List[Dict]:
    """ Generate weather report along route using Yahoo & Open-Meteo APIs """
    sampled_points = _sample_route_by_distance(route_data_with_timestamps, interval_km)
    if not sampled_points: return []

    print("[1/2] Fetching basic weather info (precipitation) from Yahoo! API...")
    points_with_base_weather = _get_weather_for_points_yahoo(sampled_points)
    
    print("[2/2] Fetching detailed weather (Sunny/Cloudy/Rain) and temperature from Open-Meteo API...")
    for point in points_with_base_weather:
        open_meteo_data = _get_open_meteo_data(point)
        
        # Step 1: Add temperature from Open-Meteo
        if open_meteo_data:
            point['weather']['temperature'] = open_meteo_data['temperature']
        else:
            point['weather']['temperature'] = None

        # Step 2: Final decision of weather (Sunny / Cloudy / Rain)
        if point['weather'].get('description') == 'Rain':
            final_description = 'Rain'
        elif open_meteo_data:
            final_description = open_meteo_data['description']
        elif point['weather'].get('description') == 'No forecast':
            final_description = 'No forecast'
        else:
            final_description = 'Cloudy'
        
        point['weather']['description'] = final_description

    print("Weather data retrieval completed for all points.")
    return points_with_base_weather

def simulate_journey_and_get_weather(
    ordered_route_data_with_time: List[List[float]],
    start_time: datetime = None
) -> List[Dict[str, Any]]:
    """
    Generate weather info for each point along route with timestamps.
    """
    if not ordered_route_data_with_time:
        print("Error: Route data is empty."); return []
    if start_time is None:
        start_time = datetime.now()

    print(f"Starting report generation... (Simulation start time: {start_time.strftime('%Y-%m-%d %H:%M')})")
    
    start_timestamp = start_time.timestamp()
    route_with_timestamps = []
    for point in ordered_route_data_with_time:
        lat, lon, elapsed_seconds = point
        absolute_timestamp = start_timestamp + elapsed_seconds
        route_with_timestamps.append([lat, lon, absolute_timestamp])
    
    # --- ▼▼▼ ここからが修正箇所 ▼▼▼ ---

    # ルートの総距離を計算し、データに応じた最適なサンプリング間隔を自動設定する
    total_distance_km = 0.0
    if len(route_with_timestamps) > 1:
        for i in range(1, len(route_with_timestamps)):
            prev_pt = route_with_timestamps[i-1]
            curr_pt = route_with_timestamps[i]
            total_distance_km += haversine(prev_pt[0], prev_pt[1], curr_pt[0], curr_pt[1])

    # デフォルトのサンプリング間隔 (km)
    interval_km = 15.0
    
    if total_distance_km > 0:
        # デフォルト間隔(15km)でサンプリングした場合のポイント数を概算（始点を含む）
        num_points = (total_distance_km // interval_km) + 1
        
        # 取得する天気予報ポイント数が極端に少なく/多くならないように調整
        MIN_POINTS = 5  # 少なくともこれくらいの数のポイントは取得したい
        MAX_POINTS = 15 # 多くてもこれくらいの数のポイントに抑えたい

        if num_points < MIN_POINTS:
            # ポイント数が少なすぎる場合、間隔を狭めて MIN_POINTS を確保する
            # (MIN_POINTS - 1)個の区間ができるように間隔を再計算
            interval_km = total_distance_km / (MIN_POINTS - 1)
        elif num_points > MAX_POINTS:
            # ポイント数が多すぎる場合、間隔を広げて MAX_POINTS に抑える
            # (MAX_POINTS - 1)個の区間ができるように間隔を再計算
            interval_km = total_distance_km / (MAX_POINTS - 1)
            
        # APIの過剰な呼び出しや計算負荷を防ぐための絶対的な下限を設定
        HARD_MIN_INTERVAL = 5.0 # 最低でも5km間隔はあける
        interval_km = max(interval_km, HARD_MIN_INTERVAL)

    print(f"Total route distance: {total_distance_km:.2f} km. Auto-adjusted sampling interval to {interval_km:.2f} km.")
    
    # 動的に計算した間隔でレポートを生成
    return _generate_weather_report(route_with_timestamps, interval_km=interval_km)
    # --- ▲▲▲ ここまでが修正箇所 ▲▲▲ ---