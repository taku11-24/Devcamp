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
    """ 2点間の緯度経度から距離（km）を計算する """
    R = 6371  # 地球の半径 (km)
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

def wmo_code_to_description(code: int) -> str:
    """ Open-MeteoのWMOコードを日本語の天気概要に変換する """
    codes = {
        0: '快晴', 1: '晴れ', 2: '一部曇り', 3: '曇り', 45: '霧', 48: '霧氷',
        51: '霧雨', 53: '霧雨', 55: '霧雨', 61: '雨', 63: '雨', 65: '雨',
        71: '雪', 73: '雪', 75: '雪', 80: 'にわか雨', 81: 'にわか雨', 82: 'にわか雨',
        95: '雷雨', 96: '雷雨と雹', 99: '雷雨と雹'
    }
    return codes.get(code, f'不明なコード({code})')

def _get_weather_for_points_yahoo(points: List[Dict]) -> List[Dict]:
    """ [Yahoo! API] 複数の地点の天気概要と降水量をまとめて取得する """
    base_url = "https://map.yahooapis.jp/weather/V1/place"
    api_key = os.getenv("YAHOO_API_KEY")
    if not api_key:
        print("警告: YAHOO_API_KEYが設定されていません。Yahoo! APIの処理をスキップします。")
        for p in points:
             p['weather'] = {'description': '（予報なし）', 'rainfall_mm_h': None}
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
                raise ValueError("APIレスポンスに 'Feature' キーが含まれていません。")

            for point, weather_feature in zip(chunk, weather_data.get('Feature', [])):
                all_forecasts = weather_feature.get('Property', {}).get('WeatherList', {}).get('Weather', [])
                if not all_forecasts:
                    point['weather'] = {'description': '（予報なし）', 'rainfall_mm_h': None}
                    continue

                best_forecast = min(
                    all_forecasts,
                    key=lambda f: abs(point['timestamp'] - datetime.strptime(f['Date'], '%Y%m%d%H%M').timestamp())
                )
                rainfall = best_forecast.get('Rainfall', 0.0)
                point['weather'] = {
                    'description': "雨" if rainfall > 0 else "降水なし",
                    'rainfall_mm_h': rainfall,
                }
        except requests.exceptions.RequestException as e:
            print(f"警告: Yahoo! Weather APIからの情報取得に失敗しました: {e}")
            for point in chunk:
                point['weather'] = {'description': '（予報なし）', 'rainfall_mm_h': None}
    return points

def _get_open_meteo_data(point: Dict) -> Dict[str, Any] or None:
    """ [Open-Meteo API] 気温と天気コードを取得（リトライ機能付き）"""
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
            
            # タイムゾーン情報を付与してdatetimeオブジェクトに変換
            api_times = [datetime.fromisoformat(t) for t in api_times_str]
            # 比較対象のタイムスタンプも同じタイムゾーンに変換
            target_dt = datetime.fromtimestamp(point['timestamp'], tz=api_times[0].tzinfo)

            closest_time_idx = min(range(len(api_times)), key=lambda i: abs(api_times[i] - target_dt))
            
            return {
                'temperature': hourly['temperature_2m'][closest_time_idx],
                'description': wmo_code_to_description(hourly['weather_code'][closest_time_idx])
            }
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                print(f"エラー: Open-Meteo APIから情報を取得できませんでした: {e}")
                return None
            time.sleep(1)
    return None

def _sample_route_by_distance(route_data_with_timestamps: List, interval_km: float) -> List[Dict]:
    """ ルートを一定距離ごとにサンプリングする """
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
    """ 2つの天気APIを使い、ルート上の天気レポートを生成する """
    sampled_points = _sample_route_by_distance(route_data_with_timestamps, interval_km)
    if not sampled_points: return []

    print("[1/2] Yahoo! APIから基本天気情報を取得中...")
    points_with_base_weather = _get_weather_for_points_yahoo(sampled_points)
    
    print("[2/2] Open-Meteo APIから気温等の補完情報を取得中...")
    for point in points_with_base_weather:
        open_meteo_data = _get_open_meteo_data(point)
        
        if open_meteo_data:
            point['weather']['temperature'] = open_meteo_data['temperature']
            # Yahoo!で情報が取れなかった場合のみOpen-Meteoの天気概要で上書き
            if point['weather'].get('description') == '（予報なし）':
                point['weather']['description'] = open_meteo_data['description']
        else:
            point['weather']['temperature'] = None

    print("全地点の天気情報取得が完了しました。")
    return points_with_base_weather

def simulate_journey_and_get_weather(ordered_route_data: List[List[float]], average_speed_kmh: float = 40.0, start_time: datetime = None) -> List[Dict[str, Any]]:
    """
    ルート情報と平均速度から移動シミュレーションを行い、各地点の天気情報を取得する。
    """
    if not ordered_route_data:
        print("エラー: 経路データが空です。"); return []
    if start_time is None: start_time = datetime.now()

    print(f"シミュレーションを開始します... (開始時刻: {start_time.strftime('%Y-%m-%d %H:%M')})")
    
    route_with_timestamps = []
    current_time_ts = start_time.timestamp()
    
    first_point = ordered_route_data[0]
    route_with_timestamps.append([first_point[0], first_point[1], current_time_ts])
    
    for i in range(1, len(ordered_route_data)):
        prev_point, curr_point = ordered_route_data[i-1], ordered_route_data[i]
        distance = haversine(prev_point[0], prev_point[1], curr_point[0], curr_point[1])
        travel_time_seconds = (distance / average_speed_kmh) * 3600
        current_time_ts += travel_time_seconds
        route_with_timestamps.append([curr_point[0], curr_point[1], current_time_ts])
        
    # サンプリング間隔を15kmとして天気レポートを生成
    return _generate_weather_report(route_with_timestamps, interval_km=15.0)