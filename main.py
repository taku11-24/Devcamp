# ファイル名: main.py

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

# お客様の完全なロジックをインポート
from weather_simulator import simulate_journey_and_get_weather
# DBからデータを取得するロジックをインポート
from csv_DB import csv_data_format

app = FastAPI()

class RouteData(BaseModel):
    points: List[List[float]]

@app.get("/map_info")
def map_info():
    """DBから【全ての】事故データを取得して返すエンドポイント。"""
    print("GET /map_info: CSVデータを取得します。")
    # 引数なしで呼び出し、全件取得
    return csv_data_format()

@app.post("/weather/simulation")
async def run_weather_simulation(route_data: RouteData):
    """
    フロントエンドから経路データを受け取り、
    気象シミュレーション結果と、【経路周辺の】事故データ【最大20件】をまとめて返す。
    """
    print(f"POST /weather/simulation: {len(route_data.points)} 地点のシミュレーションを開始します。")
    
    # お客様のロジック①：天気シミュレーションを実行
    weather_report = simulate_journey_and_get_weather(
        ordered_route_data=route_data.points,
        average_speed_kmh=40.0 
    )

    # ★★★ここからが修正部分★★★
    # お客様のロジック②：DBから【経路周辺の】データを【最大20件】取得
    # csv_data_formatに関数の引数として経路データ(points)を渡す
    accident_data = csv_data_format(points=route_data.points)
    # ★★★修正部分ここまで★★★

    # 結果を統合してレスポンス
    return {
        "status": "success", 
        "report": weather_report,
        "accident_data": accident_data
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)