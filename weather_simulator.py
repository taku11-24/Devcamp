# ファイル名: weather_simulator.py

import os
import requests
import numpy as np
from datetime import datetime, timezone
import json
import time
from typing import List, Dict, Any
from dotenv import load_dotenv
import sqlite3

# .envファイルから環境変数を読み込む
load_dotenv()

def haversine(lat1, lon1, lat2, lon2):
    """ 2点間の緯度経度から距離（km）を計算する """
    R = 6371
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

def wmo_code_to_description(code):
    """ Open-MeteoのWMOコードを日本語の天気概要に変換する """
    codes = {
        0: '快晴', 1: '晴れ', 2: '一部曇り', 3: '曇り', 45: '霧', 48: '霧氷',
        51: '霧雨', 53: '霧雨', 55: '霧雨', 61: '雨', 63: '雨', 65: '雨',
        71: '雪', 73: '雪', 75: '雪', 80: 'にわか雨', 81: 'にわか雨', 82: 'にわか雨',
        95: '雷雨', 96: '雷雨と雹', 99: '雷雨と雹'
    }
    return codes.get(code, f'不明({code})')

def _get_weather_for_points_yahoo(points):
    """ [Yahoo! API] 複数の地点の天気概要と降水量をまとめて取得する """
    base_url = "https://map.yahooapis.jp/weather/V1/place"
    api_key = os.getenv("YAHOO_API_KEY") # .envから読み込み
    chunk_size = 10

    chunked_points = [points[i:i + chunk_size] for i in range(0, len(points), chunk_size)]
    for chunk in chunked_points:
        if not chunk: continue

        coordinate_str = " ".join(f"{p['lon']},{p['lat']}" for p in chunk)
        params = {"coordinates": coordinate_str, "output": "json", "appid": api_key, "interval": "10"}

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            weather_data = response.json()

            if 'Feature' not in weather_data or len(weather_data['Feature']) != len(chunk):
                raise ValueError("APIレスポンスの地点数がリクエストと一致しません。")

            for point, weather_feature in zip(chunk, weather_data['Feature']):
                all_forecasts = weather_feature.get('Property', {}).get('WeatherList', {}).get('Weather', [])
                
                if not all_forecasts:
                    point['weather'] = {'description': '（予報なし）', 'rainfall_mm_h': None}
                    continue

                best_forecast = min(
                    all_forecasts,
                    key=lambda f: abs(point['timestamp'] - datetime.strptime(f['Date'], '%Y%m%d%H%M').timestamp()),
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

def _get_open_meteo_data(point):
    """ [Open-Meteo API] 気温と天気コードを取得（リトライ機能付き）"""
    dt_object = datetime.fromtimestamp(point['timestamp'])
    is_past = dt_object.astimezone(timezone.utc).date() < datetime.now(timezone.utc).date()
    
    base_url = "https://archive-api.open-meteo.com/v1/archive" if is_past else "https://api.open-meteo.com/v1/forecast"
    
    params = {'latitude': point['lat'], 'longitude': point['lon'], 'hourly': 'temperature_2m,weather_code', 'timezone': 'auto'}
    if is_past:
        date_str = dt_object.strftime('%Y-%m-%d')
        params.update({'start_date': date_str, 'end_date': date_str})
    
    retries = 3
    for attempt in range(retries):
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
            if attempt == retries - 1:
                print(f"エラー: Open-Meteo APIから情報を取得できませんでした: {e}")
                return None
            time.sleep(1)
    return None

def _sample_route_by_distance(route_data_with_timestamps, interval_km):
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
            
            sampled_points.append({'lat': interp_lat, 'lon': interp_lon, 'timestamp': int(interp_ts), 'distance_km': next_sample_distance})
            next_sample_distance += interval_km
        cumulative_distance += distance
    return sampled_points

def _generate_weather_report(route_data_with_timestamps, interval_km):
    """ 2つのAPIを使い、天気レポートを生成 """
    sampled_points = _sample_route_by_distance(route_data_with_timestamps, interval_km)
    if not sampled_points: return []

    print("\n[1/2] Yahoo! APIから基本情報を取得中...")
    points_with_base_weather = _get_weather_for_points_yahoo(sampled_points)
    
    print("\n[2/2] Open-Meteo APIから気温と補完情報を取得中...")
    for point in points_with_base_weather:
        open_meteo_data = _get_open_meteo_data(point)
        
        if open_meteo_data:
            point['weather']['temperature'] = open_meteo_data['temperature']
            if point['weather'].get('description') == '（予報なし）':
                point['weather']['description'] = open_meteo_data['description']
        else:
            point['weather']['temperature'] = None

    print("\n全地点の天気情報取得が完了しました。")
    return points_with_base_weather

def simulate_journey_and_get_weather(ordered_route_data: List[List[float]], average_speed_kmh: float = 40.0, start_time: datetime = None) -> List[Dict[str, Any]]:
    """ ルート情報からシミュレーションを実行し、天気情報を取得する """
    if not ordered_route_data:
        print("エラー: 経路データが空です。"); return []
    if start_time is None: start_time = datetime.now()

    print(f"\nシミュレーションを開始します... (開始時刻: {start_time.strftime('%Y-%m-%d %H:%M')})")
    
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
        
    return _generate_weather_report(route_with_timestamps, interval_km=15.0)

# --- ここからが新しいDB関連の機能 ---

def setup_demo_database(db_path='locations.db'):
    """ デモ用のSQLiteデータベースとテーブルを作成し、サンプルデータを挿入する """
    if os.path.exists(db_path):
        print(f"データベース '{db_path}' は既に存在します。セットアップをスキップします。")
        return

    print(f"デモ用データベース '{db_path}' を作成します...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE CSV_data (
        件数 INTEGER,
        緯度 REAL,
        経度 REAL
    )
    """)

    sample_data = [
        (101, 35.6895, 139.6917), # 新宿 (ルート周辺)
        (102, 35.5437, 139.7383), # 羽田空港 (ルート周辺)
        (201, 35.1815, 136.9066), # 名古屋城 (ルート周辺)
        (301, 34.6937, 135.5023), # 大阪市中心部 (ルート周辺)
        (302, 34.6525, 135.4331), # USJ (ルート周辺)
        (901, 43.0621, 141.3544), # 札幌 (ルートから遠い)
        (902, 26.2124, 127.6809),  # 那覇 (ルートから遠い)
    ]
    
    cursor.executemany("INSERT INTO CSV_data (件数, 緯度, 経度) VALUES (?, ?, ?)", sample_data)
    
    conn.commit()
    conn.close()
    print("データベースのセットアップが完了しました。")

def filter_csv_data_around_route(route_points: List[List[float]], db_path: str, radius_km: float) -> List[Dict[str, Any]]:
    """
    指定されたルートの周辺にあるDBデータ(CSV_data)を効率的にフィルタリングして取得する
    """
    if not route_points:
        return []

    lats = [p[0] for p in route_points]
    lons = [p[1] for p in route_points]
    
    margin_lat = radius_km / 111.0
    margin_lon = radius_km / (111.0 * np.cos(np.radians(np.mean(lats))))

    min_lat, max_lat = min(lats) - margin_lat, max(lats) + margin_lat
    min_lon, max_lon = min(lons) - margin_lon, max(lons) + margin_lon

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        query = """
        SELECT 件数, 緯度, 経度 
        FROM CSV_data 
        WHERE (緯度 BETWEEN ? AND ?) AND (経度 BETWEEN ? AND ?)
        """
        cursor.execute(query, (min_lat, max_lat, min_lon, max_lon))
        candidate_points = cursor.fetchall()
        conn.close()

        if not candidate_points:
            return []

        nearby_data = []
        for data_point in candidate_points:
            data_count, data_lat, data_lon = data_point
            
            for route_lat, route_lon, *_ in route_points:
                if haversine(route_lat, route_lon, data_lat, data_lon) <= radius_km:
                    nearby_data.append({'件数': data_count, '緯度': data_lat, '経度': data_lon})
                    break 
        
        return nearby_data

    except sqlite3.Error as e:
        print(f"データベースエラーが発生しました: {e}")
        return []