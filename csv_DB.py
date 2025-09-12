# csv_DB.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Dict, Any

load_dotenv()

def get_accident_data_from_postgres(points: List[List[float]], buffer_meters: float = 500.0) -> List[Dict[str, Any]]:
    """
    PostgreSQL/PostGISを使い、ルート周辺の事故データを取得する。
    実際の日本語テーブル構造に合わせて修正済み。

    Args:
        points (List[List[float]]): [[lat, lon], ...] の形式の座標リスト。
        buffer_meters (float): 通常検索の範囲（半径）をメートル単位で指定。

    Returns:
        List[Dict[str, Any]]: 取得した事故データのリスト。
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("エラー: .envファイルにDATABASE_URLが設定されていません。")
        return []

    if not points or len(points) < 2:
        return []

    linestring_wkt = "LINESTRING(" + ", ".join(f"{p[1]} {p[0]}" for p in points) + ")"

    # --- ▼▼▼ ここからが修正箇所 ▼▼▼ ---

    # クエリ1: 通常検索用。実際の日本語列名に修正し、ASで英語の別名を設定。
    nearby_query = text("""
        SELECT
            id,
            "緯度" AS latitude,
            "経度" AS longitude,
            "件数" AS count
        FROM "CSV_data"
        WHERE ST_DWithin(
            ST_MakePoint("経度", "緯度")::geography,
            ST_GeomFromText(:linestring, 4326)::geography,
            :buffer_meters
        );
    """)

    # クエリ2: 強制検索用。こちらも同様に修正。
    fallback_query = text("""
        SELECT
            id,
            "緯度" AS latitude,
            "経度" AS longitude,
            "件数" AS count,
            ST_Distance(
                ST_MakePoint("経度", "緯度")::geography,
                ST_GeomFromText(:linestring, 4326)::geography
            ) AS distance_m
        FROM "CSV_data"
        ORDER BY distance_m ASC
        LIMIT 20;
    """)
    
    # --- ▲▲▲ ここまでが修正箇所 ▲▲▲ ---

    results_list = []
    try:
        engine = create_engine(database_url)
        with engine.connect() as connection:
            # フェーズ1: 通常検索
            print(f"PostGISを使用して、ルート周辺（半径{buffer_meters}m）のデータを 'CSV_data' テーブルから検索しています...")
            
            result_proxy = connection.execute(nearby_query, {
                "linestring": linestring_wkt,
                "buffer_meters": buffer_meters
            })
            results_list = [dict(row._mapping) for row in result_proxy]
            
            print(f"{len(results_list)}件のデータを取得しました。")

            # フェーズ2: 結果が0件なら強制検索
            if not results_list:
                print(f" -> 半径 {buffer_meters}m 以内ではデータが見つかりませんでした。")
                print(" -> 最終手段として、範囲を無制限に広げてデータベース全体を再検索します...")

                fallback_result_proxy = connection.execute(fallback_query, {
                    "linestring": linestring_wkt
                })
                results_list = [dict(row._mapping) for row in fallback_result_proxy]

                if results_list:
                    nearest_distance = results_list[0]['distance_m']
                    print(f" -> 全範囲を検索し、最も近いデータ({nearest_distance:.0f} m先)を{len(results_list)}件見つけました。")
                else:
                    print(" -> 'CSV_data' テーブルにデータが1件も存在しませんでした。")

    except OperationalError as e:
        print(f"データベースへの接続に失敗しました: {e}")
        return []
    except Exception as e:
        print(f"データ検索中に予期せぬエラーが発生しました: {e}")
        return []

    return results_list