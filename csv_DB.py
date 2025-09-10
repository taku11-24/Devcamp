# ファイル名: csv_DB.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Optional

# ★★★ ここを修正 ★★★
def csv_data_format(points: Optional[List[List[float]]] = None, buffer: float = 0.5):
    """
    データベースから事故データを取得します。
    - pointsが指定された場合：経路周辺のデータを【最大20件】取得します。
    - pointsが指定されない場合：全てのデータを取得します。
    """
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("エラー: .envファイルにDATABASE_URLが設定されていません。")
        return []

    results_list = []
    try:
        engine = create_engine(database_url)
        with engine.connect() as connection:
            
            base_query = 'SELECT id, "件数", "緯度", "経度" FROM "CSV_data"'
            params = {}
            
            if points and len(points) > 0:
                # ★★★ 検索範囲が広がります ★★★
                print(f"経路周辺のデータを【最大20件】で検索します... (検索バッファ: {buffer})")
                lats = [p[0] for p in points]
                lons = [p[1] for p in points]
                
                min_lat, max_lat = min(lats) - buffer, max(lats) + buffer
                min_lon, max_lon = min(lons) - buffer, max(lons) + buffer
                
                where_clause = ' WHERE "緯度" BETWEEN :min_lat AND :max_lat AND "経度" BETWEEN :min_lon AND :max_lon'
                limit_clause = ' LIMIT 20'
                
                final_query_str = f'{base_query}{where_clause} ORDER BY id{limit_clause};'
                params = {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                }
            else:
                print("全てのデータを取得します...")
                final_query_str = f'{base_query} ORDER BY id;'
            
            query = text(final_query_str)

            print("データベースからデータを取得しています...")
            result_proxy = connection.execute(query, params)
            
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