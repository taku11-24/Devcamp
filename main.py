# main.py

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import traceback

# 各機能を提供するモジュールから関数をインポート
from weather_simulator import simulate_journey_and_get_weather
from csv_DB import get_accident_data_from_postgres
from braking_data_handler import get_nearest_braking_events

# --- Pydanticモデルの定義 ---
class RouteData(BaseModel):
    """フロントエンドから受け取る経路データの形式"""
    # [[lat, lon, elapsed_seconds], ...] の形式を受け取る
    points: List[List[float]]

# --- FastAPIアプリケーションのインスタンスを作成 ---
app = FastAPI(
    title="Weather and Accident Information API",
    description="走行ルートの天気予報と周辺の事故・急ブレーキ情報をシミュレーションするAPIです。"
)

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
        raise HTTPException(status_code=400, detail="経路データが空です。")

    # 入力データが [緯度, 経度, 時間] の3つの要素を持っているか確認
    if len(route_data.points[0]) != 3:
        raise HTTPException(status_code=400, detail="各地点のデータは [緯度, 経度, 経過秒数] の形式である必要があります。")

    print(f"POST /weather/simulation: {len(route_data.points)} 地点のシミュレーションを開始します。")

    try:
        # 1. 天気シミュレーションを実行
        weather_report = simulate_journey_and_get_weather(
            ordered_route_data_with_time=route_data.points
        )

        # 2. ルート周辺の事故データを取得
        print("データベースからルート周辺の事故データを検索します...")
        points_lat_lon = [[p[0], p[1]] for p in route_data.points]
        nearby_accident_data = get_accident_data_from_postgres(
            points=points_lat_lon,
            buffer_meters=500.0  # 半径500m
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
    except Exception as e:
        print(f"サーバー内部でエラーが発生しました: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="サーバー内部でエラーが発生しました。")


if __name__ == "__main__":
    # このファイル単体で実行した場合、Uvicornサーバーを起動
    # Render.comのような本番環境では、この部分は使われず、直接GunicornやUvicornのコマンドで起動されます。
    uvicorn.run(app, host="0.0.0.0", port=8000)