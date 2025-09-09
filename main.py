import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

# --- 外部モジュールからのインポート ---

# 天気シミュレーションのロジックをまとめたファイルからメイン関数をインポート
from weather_simulator import simulate_journey_and_get_weather

# お客様の既存コードで使われているCSV関連の関数をインポート
# （この`csv_DB.py`ファイルが同じディレクトリに存在する必要があります）
from csv_DB import csv_data_format


# FastAPIアプリケーションのインスタンスを作成
app = FastAPI()


# --- フロントエンドから受け取るデータ形式をPydanticモデルで定義 ---
class RouteData(BaseModel):
    """
    天気シミュレーションAPIが受け取るリクエストボディの形式を定義。
    [[lat, lon], [lat, lon], ...] という形式のリストを想定。
    """
    points: List[List[float]]


# ==============================================================================
# APIエンドポイントの定義
# ==============================================================================

# --- お客様の既存のCSVデータ取得用エンドポイント ---
@app.get("/map_info")
def map_info():
    """
    既存の、CSVからデータを取得して返すエンドポイント。
    """
    print("GET /map_info: CSVデータを取得します。")
    csv_data = csv_data_format()
    return csv_data


# --- 新しく追加した天気シミュレーション用エンドポイント ---
@app.post("/weather/simulation")
async def run_weather_simulation(route_data: RouteData):
    """
    フロントエンドから走行順の経路データを受け取り、
    時速40kmと仮定した天気シミュレーション結果を返す。
    """
    print(f"POST /weather/simulation: {len(route_data.points)} 地点のシミュレーションを開始します。")
    
    # Pydanticモデルから座標リストを取得
    ordered_points = route_data.points

    # weather_simulatorの関数を呼び出してレポートを生成
    report = simulate_journey_and_get_weather(
        ordered_route_data=ordered_points,
        average_speed_kmh=40.0
    )

    # 計算結果をJSON形式でフロントエンドに返す
    return {"status": "success", "report": report}


# ==============================================================================
# サーバーの実行
# ==============================================================================

# このファイルが直接実行された場合にUvicornサーバーを起動
# ターミナルで `python main.py` と実行してもサーバーが起動します。
# もちろん `uvicorn main:app --reload` での起動も可能です。
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)