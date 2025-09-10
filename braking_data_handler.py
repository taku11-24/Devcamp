# ファイル名: braking_data_handler.py

import os
import math
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Dict, Any

def get_nearest_braking_events(target_lat: float, target_lon: float) -> List[Dict[str, Any]]:
    """
    指定された座標に最も近い急ブレーキイベントを最大20件、データベースから取得する。
    
    Args:
        target_lat (float): 中心の緯度
        target_lon (float): 中心の経度

    Returns:
        List[Dict[str, Any]]: 取得したイベントのリスト。各辞書には距離(km)も含まれる。
    """
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("エラー: .envファイルにDATABASE_URLが設定されていません。")
        return []

    results_list = []

    # 地球の半径 (km)
    R = 6371

    # Haversine（ハーベサイン）公式をSQLで計算するクエリ。
    # これにより、DB内で直接距離を計算し、効率的にソートできる。
    # データベース内の全レコードとの距離を計算し、近い順に20件を取得する。
    # これが「自動で範囲を広げる」ロジックの実現方法です。
    query = text(f"""
        SELECT
            id,
            latitude,
            longitude,
            event_timestamp,
            (
                {R} * 2 * ASIN(SQRT(
                    POWER(SIN(RADIANS(latitude - :target_lat) / 2), 2) +
                    COS(RADIANS(:target_lat)) * COS(RADIANS(latitude)) *
                    POWER(SIN(RADIANS(longitude - :target_lon) / 2), 2)
                ))
            ) AS distance_km
        FROM
            "BrakingEvents"
        ORDER BY
            distance_km
        LIMIT 20;
    """)

    try:
        engine = create_engine(database_url)
        with engine.connect() as connection:
            print(f"データベースに接続し、({target_lat}, {target_lon}) 周辺の急ブレーキデータを検索しています...")
            
            # クエリ実行時に緯度経度をパラメータとして渡す
            result_proxy = connection.execute(query, {
                "target_lat": target_lat,
                "target_lon": target_lon
            })

            for row in result_proxy:
                results_list.append(dict(row._mapping))
            
            print(f"✅ {len(results_list)}件のデータを取得しました。")

    except OperationalError as e:
        print(f"❌ データベースへの接続に失敗しました: {e}")
        return []
    except Exception as e:
        print(f"❌ 予期せぬエラーが発生しました: {e}")
        return []

    return results_list

# --- このファイルが直接実行された場合のテストコード ---
if __name__ == '__main__':
    # テストケース1: 名古屋駅のすぐ近くを指定
    print("--- テストケース1: 名古屋駅周辺 ---")
    # 名古屋駅の座標
    test_lat_1, test_lon_1 = 35.170694, 136.881637
    nearest_events_1 = get_nearest_braking_events(test_lat_1, test_lon_1)
    if nearest_events_1:
        for event in nearest_events_1:
            print(f"ID: {event['id']}, 距離: {event['distance_km']:.2f} km, 座標: ({event['latitude']}, {event['longitude']})")
    
    print("\n" + "="*50 + "\n")

    # テストケース2: 少し離れた場所（鶴舞公園）を指定
    # 近くにデータは少ないが、名古屋駅や栄、金山のデータが取得されるはず
    print("--- テストケース2: 鶴舞公園周辺 ---")
    test_lat_2, test_lon_2 = 35.155761, 136.921312
    nearest_events_2 = get_nearest_braking_events(test_lat_2, test_lon_2)
    if nearest_events_2:
        for event in nearest_events_2:
            print(f"ID: {event['id']}, 距離: {event['distance_km']:.2f} km, 座標: ({event['latitude']}, {event['longitude']})")