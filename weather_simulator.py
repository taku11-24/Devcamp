import requests
import numpy as np
from datetime import datetime, date, timedelta
import time
import json

# ==============================================================================
# Part 1: ヘルパー関数群 (変更なし)
# ==============================================================================

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad; dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)); return R * c

def wmo_code_to_description(code):
    codes = {0: '快晴', 1: '晴れ', 2: '一部曇り', 3: '曇り', 45: '霧', 48: '霧氷', 51: '霧雨（弱い）', 53: '霧雨（中）', 55: '霧雨（強い）', 61: '雨（弱い）', 63: '雨（中）', 65: '雨（強い）', 71: '雪（弱い）', 73: '雪（中）', 75: '雪（強い）', 80: 'にわか雨（弱い）', 81: 'にわか雨（中）', 82: 'にわか雨（強い）', 95: '雷雨'}
    return codes.get(code, f'不明なコード({code})')

# ==============================================================================
# Part 2: コアロジック関数群 (天気取得関数を修正)
# ==============================================================================

# ★★★ 天気取得関数を、日付に応じてURLを切り替えるように修正 ★★★
def _get_weather_for_point_open_meteo(point):
    """
    Open-Meteo APIを使い、特定の地点・時刻の天気情報を取得する。
    日付に応じて、履歴APIと予報APIを自動で切り替える。
    """
    dt_object = datetime.fromtimestamp(point['timestamp'])
    today = date.today()

    # 問い合わせる日付が過去か、今日・未来かでAPIのURLを決定
    if dt_object.date() < today:
        # 過去のデータは履歴API (archive-api) を使用
        base_url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        # 今日・未来のデータは予報API (api) を使用
        base_url = "https://api.open-meteo.com/v1/forecast"

    date_str = dt_object.strftime('%Y-%m-%d')
    params = {
        'latitude': point['lat'],
        'longitude': point['lon'],
        'hourly': 'temperature_2m,weather_code',
        'timezone': 'auto'
    }
    # 履歴APIはstart_dateとend_dateが必要
    if "archive" in base_url:
        params['start_date'] = date_str
        params['end_date'] = date_str

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        weather_data = response.json()

        hourly_data = weather_data['hourly']
        timestamps = hourly_data['time']
        target_hour_str = dt_object.strftime('%Y-%m-%dT%H:00')
        
        try:
            idx = timestamps.index(target_hour_str)
            temperature = hourly_data['temperature_2m'][idx]
            weather_code = hourly_data['weather_code'][idx]
            return {'temperature': temperature, 'description': wmo_code_to_description(weather_code)}
        except ValueError:
            print(f"警告: 指定時刻 {target_hour_str} のデータが見つかりません。")
            return None
    except requests.exceptions.RequestException as e:
        print(f"エラー: Open-Meteoからの天気情報取得に失敗しました: {e}")
        return None

# (_sample_route_by_distance と _generate_weather_report は前回の全文コードと同じ)
def _sample_route_by_distance(route_data_with_timestamps, interval_km):
    if not route_data_with_timestamps: return []
    sampled_points = []
    cumulative_distance = 0.0; next_sample_distance = 0.0
    first_point = route_data_with_timestamps[0]
    sampled_points.append({'lat': first_point[0], 'lon': first_point[1], 'timestamp': int(first_point[2]), 'distance_km': 0.0})
    next_sample_distance += interval_km
    for i in range(1, len(route_data_with_timestamps)):
        prev_point = route_data_with_timestamps[i-1]; curr_point = route_data_with_timestamps[i]
        distance = haversine(prev_point[0], prev_point[1], curr_point[0], curr_point[1])
        cumulative_distance += distance
        if cumulative_distance >= next_sample_distance:
            sampled_points.append({'lat': curr_point[0], 'lon': curr_point[1], 'timestamp': int(curr_point[2]), 'distance_km': cumulative_distance})
            while next_sample_distance <= cumulative_distance: next_sample_distance += interval_km
    return sampled_points

def _generate_weather_report(route_data_with_timestamps, interval_km):
    sampled_points = _sample_route_by_distance(route_data_with_timestamps, interval_km)
    enriched_results = []
    print("\n各サンプリング地点の天気情報を取得します...")
    for point in sampled_points:
        dt_object = datetime.fromtimestamp(point['timestamp'])
        print(f" -> 距離 {point['distance_km']:.2f}km 地点 (想定日時: {dt_object.strftime('%Y-%m-%d %H:%M')})")
        weather_info = _get_weather_for_point_open_meteo(point)
        point['weather'] = weather_info
        enriched_results.append(point)
    return enriched_results

# ==============================================================================
# Part 3: メイン実行関数 (変更なし)
# ==============================================================================

def simulate_journey_and_get_weather(
    ordered_route_data, 
    average_speed_kmh=40.0, 
    start_time=None):
    if not ordered_route_data:
        print("エラー: 経路データが空です。"); return []
    if start_time is None:
        start_time = datetime.now()
    print(f"シミュレーションを開始します... (開始時刻: {start_time.strftime('%Y-%m-%d %H:%M')}, 平均時速: {average_speed_kmh} km/h)")
    route_with_estimated_timestamps = []
    current_time = start_time
    first_point = ordered_route_data[0]
    route_with_estimated_timestamps.append([first_point[0], first_point[1], int(current_time.timestamp())])
    for i in range(1, len(ordered_route_data)):
        prev_point = ordered_route_data[i-1]; curr_point = ordered_route_data[i]
        distance = haversine(prev_point[0], prev_point[1], curr_point[0], curr_point[1])
        travel_time_hours = distance / average_speed_kmh
        current_time += timedelta(hours=travel_time_hours)
        route_with_estimated_timestamps.append([curr_point[0], curr_point[1], int(current_time.timestamp())])
    print("全地点の想定通過時刻の計算が完了しました。")
    return _generate_weather_report(route_with_estimated_timestamps, interval_km=15.0)

# ==============================================================================
# Part 4: 実行ブロック (2つのテストケース)
# ==============================================================================

if __name__ == '__main__':
    received_ordered_data = [
        [35.0, 135.0], [35.01, 135.01], [35.02, 135.02], [35.03, 135.03],
        [35.05, 135.05], [35.06, 135.06], [35.07, 135.07], [35.08, 135.08],
        [35.10, 135.10], [35.11, 135.11], [35.12, 135.12], [35.13, 135.13],
        [35.15, 135.15], [35.16, 135.16], [35.17, 135.17], [35.18, 135.18],
        [35.20, 135.20], [35.21, 135.21], [35.22, 135.22], [35.23, 135.23],
        [35.25, 135.25], [35.26, 135.26], [35.27, 135.27], [35.28, 135.28],
        [35.30, 135.30],
    ]

    # --- ケース1: 「今から」出発する未来の予報シミュレーション ---
    print("="*50)
    print("ケース1: 未来の天気予報シミュレーション")
    print("="*50)
    future_report = simulate_journey_and_get_weather(
        ordered_route_data=received_ordered_data, 
        average_speed_kmh=40.0
    )
    if future_report:
        for point in future_report:
            point['estimated_time_str'] = datetime.fromtimestamp(point['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        print("\n--- 未来シミュレーション結果 (JSON形式) ---")
        print(json.dumps(future_report, indent=2, ensure_ascii=False))

    print("\n\n")

    # --- ケース2: 「過去のある時点」に出発したと仮定した履歴シミュレーション ---
    print("="*50)
    print("ケース2: 過去の天気履歴シミュレーション")
    print("="*50)
    past_start_time = datetime(2024, 8, 1, 9, 0, 0) # 2024年8月1日 朝9時
    past_report = simulate_journey_and_get_weather(
        ordered_route_data=received_ordered_data, 
        average_speed_kmh=40.0,
        start_time=past_start_time
    )
    if past_report:
        for point in past_report:
            point['estimated_time_str'] = datetime.fromtimestamp(point['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        print("\n--- 過去シミュレーション結果 (JSON形式) ---")
        print(json.dumps(past_report, indent=2, ensure_ascii=False))