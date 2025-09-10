# ファイル名: main.py (修正後)

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel  # ← ここを修正しました
from typing import List, Dict, Any

# 天気シミュレーション関連の関数をインポート
from weather_simulator import simulate_journey_and_get_weather
# 事故データ関連の関数をインポート
from csv_DB import get_accident_data_from_postgres
# 急ブレーキデータ取得の関数をインポート
from braking_data_handler import get_nearest_braking_events

# FastAPIアプリケーションのインスタンスを作成
app = FastAPI(
    title="Weather and Accident Information API",
    description="走行ルートの天気予報と周辺の事故・急ブレーキ情報をシミュレーションするAPIです。"
)

# --- フロントエンドから受け取るデータ形式 ---
class RouteData(BaseModel):
    points: List[List[float]]


# --- APIエンドポイントの定義 ---

@app.post("/weather/simulation", summary="天気と事故・急ブレーキ情報のシミュレーション")
async def run_weather_simulation(route_data: RouteData) -> Dict[str, Any]:
    """
    フロントエンドから経路データを受け取り、以下の処理を実行して結果を返す。
    1. ルート上の各地点の天気情報をシミュレーションする。
    2. ルート周辺の事故データをデータベースから取得する。
    3. ルートの開始地点周辺の急ブレーキデータを取得する。
    """
    if not route_data.points:
        return {"status": "error", "message": "経路データが空です。"}

    print(f"POST /weather/simulation: {len(route_data.points)} 地点のシミュレーションを開始します。")

    # 1. 天気シミュレーションを実行
    weather_report = simulate_journey_and_get_weather(
        ordered_route_data=route_data.points,
        average_speed_kmh=40.0
    )

    # 2. ルート周辺の事故データをDATABASE_URLのDBから取得
    print("データベースからルート周辺の事故データを検索します...")
    nearby_accident_data = get_accident_data_from_postgres(
        points=route_data.points,
        buffer=0.5
    )

    # 3. ルートの開始地点周辺の急ブレーキデータを取得
    print("データベースからルート開始地点周辺の急ブレーキデータを検索します...")
    start_point = route_data.points[0]
    start_lat, start_lon = start_point[0], start_point[1]
    nearby_braking_events = get_nearest_braking_events(
        target_lat=start_lat,
        target_lon=start_lon
    )

    # 4. 全ての結果を統合してレスポンスとして返す
    return {
        "status": "success",
        "report": weather_report,
        "nearby_accident_data": nearby_accident_data,
        "nearby_braking_events": nearby_braking_events
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)