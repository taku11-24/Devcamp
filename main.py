# ファイル名: main.py

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

# weather_simulator.pyから必要な全ての関数をインポート
from weather_simulator import (
    simulate_journey_and_get_weather,
    setup_demo_database,
    filter_csv_data_around_route
)

# --- グローバル設定 ---
SQLITE_DB_PATH = 'locations.db'

# FastAPIアプリケーションのインスタンスを作成
app = FastAPI()

# --- アプリケーション起動時のイベント ---
@app.on_event("startup")
def on_startup():
    """
    サーバー起動時に一度だけ実行される。
    デモ用のSQLiteデータベースとテーブル、サンプルデータを準備する。
    """
    setup_demo_database(SQLITE_DB_PATH)


# --- フロントエンドから受け取るデータ形式 ---
class RouteData(BaseModel):
    points: List[List[float]]


# --- APIエンドポイントの定義 ---

@app.post("/weather/simulation")
async def run_weather_simulation(route_data: RouteData):
    """
    フロントエンドから経路データを受け取り、
    1. 天気シミュレーションの実行
    2. ルート周辺の事故データ(SQLiteから)の取得
    を行い、両方の結果をまとめて返す。
    """
    print(f"POST /weather/simulation: {len(route_data.points)} 地点のシミュレーションを開始します。")
    
    # 1. お客様のロジック：天気シミュレーションを実行
    weather_report = simulate_journey_and_get_weather(
        ordered_route_data=route_data.points,
        average_speed_kmh=40.0 
    )

    # 2. お客様の新しいロジック：ルート周辺の事故データをSQLiteから取得
    print(f"SQLiteデータベース({SQLITE_DB_PATH})からルート周辺のデータを検索します...")
    # 検索半径はここで指定（例: 50km）
    search_radius_km = 50.0
    nearby_accident_data = filter_csv_data_around_route(
        route_points=route_data.points,
        db_path=SQLITE_DB_PATH,
        radius_km=search_radius_km
    )

    # 3. 結果を統合してレスポンス
    return {
        "status": "success", 
        "report": weather_report,
        "nearby_accident_data": nearby_accident_data
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)