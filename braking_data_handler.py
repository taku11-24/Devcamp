import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Dict, Any

# モジュール読み込み時に一度だけ環境変数をロード
load_dotenv()

def get_nearest_braking_events(target_lat: float, target_lon: float) -> List[Dict[str, Any]]:
    """
    指定された座標周辺の急ブレーキイベントをデータベースから最大20件取得する。
    データが見つからない場合、検索範囲を動的に拡大し、最終的には全範囲を検索して必ず見つけ出す。

    Args:
        target_lat (float): 中心の緯度
        target_lon (float): 中心の経度

    Returns:
        List[Dict[str, Any]]: 取得したイベントデータのリスト。各辞書には距離(km)も含まれる。
    """
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("エラー: .envファイルにDATABASE_URLが設定されていません。")
        return []

    # 最初に試す、段階的な検索半径のリスト (単位: km)
    search_radii_km = [1.0, 5.0, 10.0, 25.0, 50.0]
    
    results_list = []
    # 地球の半径 (km)
    EARTH_RADIUS_KM = 6371

    # Haversine公式を含むサブクエリを使い、計算結果のdistance_kmで範囲を絞り込む
    query = text(f"""
        SELECT
            *
        FROM (
            SELECT
                id,
                latitude,
                longitude,
                event_timestamp,
                (
                    {EARTH_RADIUS_KM} * 2 * ASIN(SQRT(
                        POWER(SIN(RADIANS(latitude - :target_lat) / 2), 2) +
                        COS(RADIANS(:target_lat)) * COS(RADIANS(latitude)) *
                        POWER(SIN(RADIANS(longitude - :target_lon) / 2), 2)
                    ))
                ) AS distance_km
            FROM
                "BrakingEvents"
        ) AS events_with_distance
        WHERE
            distance_km <= :radius_km
        ORDER BY
            distance_km
        LIMIT 20;
    """)

    try:
        engine = create_engine(database_url)
        with engine.connect() as connection:
            print(f"データベースに接続し、({target_lat}, {target_lon}) 周辺の急ブレーキデータを検索しています...")
            
            # --- フェーズ1: 段階的な近傍検索 ---
            for radius in search_radii_km:
                print(f" -> 半径 {radius} km以内で検索中...")
                
                result_proxy = connection.execute(query, {
                    "target_lat": target_lat,
                    "target_lon": target_lon,
                    "radius_km": radius
                })
                results_list = [dict(row._mapping) for row in result_proxy]
                
                # データが見つかったら、その時点で即座に結果を返して処理を終了
                if results_list:
                    print(f" -> {len(results_list)}件のデータを半径 {radius} km以内で見つけました。")
                    return results_list
            
            # --- ▼▼▼ ここからが修正箇所 ▼▼▼ ---

            # --- フェーズ2: 全範囲検索 (最終手段) ---
            # 上記のループで見つからなかった場合のみ、以下の処理が実行される
            print(f" -> 半径 {search_radii_km[-1]} km以内ではデータが見つかりませんでした。")
            print(" -> 最終手段として、範囲を無制限に広げてデータベース全体を検索します...")

            # 地球の半周より大きい、事実上「無限」とみなせる半径を設定
            INFINITE_RADIUS_KM = 21000 

            result_proxy = connection.execute(query, {
                "target_lat": target_lat,
                "target_lon": target_lon,
                "radius_km": INFINITE_RADIUS_KM
            })
            results_list = [dict(row._mapping) for row in result_proxy]

            if results_list:
                # 取得したデータの中で最も近いものとの距離を表示
                nearest_distance = results_list[0]['distance_km']
                print(f" -> 全範囲を検索し、最も近いデータ({nearest_distance:.2f} km先)を{len(results_list)}件見つけました。")
            else:
                # このメッセージが表示されるのは、DBのテーブルが完全に空の場合のみ
                print(" -> データベースにデータが1件も存在しませんでした。")
            
            # --- ▲▲▲ ここまでが修正箇所 ▲▲▲ ---

    except OperationalError as e:
        print(f"データベースへの接続に失敗しました: {e}")
        return []
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return []

    return results_list

# --- このファイルが直接実行された場合のテストコード ---
if __name__ == '__main__':
    # テストケース1: 名古屋駅のすぐ近くを指定
    print("--- テストケース1: 名古屋駅周辺 ---")
    test_lat_1, test_lon_1 = 35.170694, 136.881637
    nearest_events_1 = get_nearest_braking_events(test_lat_1, test_lon_1)
    if nearest_events_1:
        for event in nearest_events_1:
            print(f"ID: {event['id']}, 距離: {event['distance_km']:.2f} km, 座標: ({event['latitude']}, {event['longitude']})")

    print("\n" + "="*50 + "\n")

    # テストケース2: 周辺にデータが全くない可能性のある座標（例: 離島）
    print("--- テストケース2: 沖ノ鳥島周辺（データが非常に遠い場合をシミュレート）---")
    test_lat_2, test_lon_2 = 20.425556, 136.081389
    nearest_events_2 = get_nearest_braking_events(test_lat_2, test_lon_2)
    if nearest_events_2:
        for event in nearest_events_2:
            print(f"ID: {event['id']}, 距離: {event['distance_km']:.2f} km, 座標: ({event['latitude']}, {event['longitude']})")