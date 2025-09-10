import os
import sqlite3
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Dict, Any, Optional

# --- 共通ヘルパー関数 ---
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """ 2点間の緯度経度から距離（km）を計算する """
    R = 6371  # 地球の半径 (km)
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


# --- SQLite 関連の機能 ---

def setup_demo_sqlite_database(db_path: str = 'locations.db'):
    """ デモ用のSQLiteデータベースとテーブルを作成し、サンプルデータを挿入する """
    if os.path.exists(db_path):
        print(f"データベース '{db_path}' は既に存在します。セットアップをスキップします。")
        return

    print(f"デモ用データベース '{db_path}' を作成します...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE CSV_data (
            件数 INTEGER,
            緯度 REAL,
            経度 REAL
        )
        """)
        sample_data = [
            (101, 35.6895, 139.6917), # 新宿
            (102, 35.5437, 139.7383), # 羽田空港
            (201, 35.1815, 136.9066), # 名古屋城
            (301, 34.6937, 135.5023), # 大阪市中心部
            (302, 34.6525, 135.4331), # USJ
            (901, 43.0621, 141.3544), # 札幌 (ルートから遠い)
            (902, 26.2124, 127.6809), # 那覇 (ルートから遠い)
        ]
        cursor.executemany("INSERT INTO CSV_data (件数, 緯度, 経度) VALUES (?, ?, ?)", sample_data)
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLiteデータベースのセットアップ中にエラーが発生しました: {e}")
    finally:
        if conn:
            conn.close()
    print("データベースのセットアップが完了しました。")


def get_nearby_accident_data_from_sqlite(route_points: List[List[float]], db_path: str, radius_km: float) -> List[Dict[str, Any]]:
    """
    指定されたルートの周辺にある事故データをSQLiteデータベースから効率的にフィルタリングして取得する。
    """
    if not route_points:
        return []

    # 1. 経路全体を囲む矩形（バウンディングボックス）を計算
    lats = [p[0] for p in route_points]
    lons = [p[1] for p in route_points]
    
    # 検索範囲に半径分のマージンを追加
    margin_lat = radius_km / 111.0  # 緯度1度あたり約111km
    margin_lon = radius_km / (111.0 * np.cos(np.radians(np.mean(lats)))) # 経度は緯度によって変化

    min_lat, max_lat = min(lats) - margin_lat, max(lats) + margin_lat
    min_lon, max_lon = min(lons) - margin_lon, max(lons) + margin_lon

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # 2. バウンディングボックスで候補を絞り込むSQLクエリ
        query = """
        SELECT 件数, 緯度, 経度
        FROM CSV_data
        WHERE (緯度 BETWEEN ? AND ?) AND (経度 BETWEEN ? AND ?)
        """
        cursor.execute(query, (min_lat, max_lat, min_lon, max_lon))
        candidate_points = cursor.fetchall()
        conn.close()

        if not candidate_points:
            return []

        # 3. 絞り込んだ候補の中から、実際に経路上のいずれかの点から半径以内にあるものだけを抽出
        nearby_data = []
        for data_count, data_lat, data_lon in candidate_points:
            is_nearby = any(
                haversine(route_lat, route_lon, data_lat, data_lon) <= radius_km
                for route_lat, route_lon in route_points
            )
            if is_nearby:
                nearby_data.append({'件数': data_count, '緯度': data_lat, '経度': data_lon})
        
        return nearby_data

    except sqlite3.Error as e:
        print(f"データベースエラーが発生しました: {e}")
        return []


# --- PostgreSQL 関連の機能 (元のコードからリファクタリング) ---

def get_accident_data_from_postgres(points: Optional[List[List[float]]] = None, buffer: float = 0.5) -> List[Dict[str, Any]]:
    """
    PostgreSQLデータベースから事故データを取得します。
    - pointsが指定された場合：経路周辺のデータを最大20件取得します。
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
                print(f"経路周辺のデータを最大20件で検索します... (検索バッファ: {buffer}度)")
                lats = [p[0] for p in points]
                lons = [p[1] for p in points]
                
                min_lat, max_lat = min(lats) - buffer, max(lats) + buffer
                min_lon, max_lon = min(lons) - buffer, max(lons) + buffer
                
                where_clause = ' WHERE "緯度" BETWEEN :min_lat AND :max_lat AND "経度" BETWEEN :min_lon AND :max_lon'
                limit_clause = ' LIMIT 20'
                final_query_str = f'{base_query}{where_clause} ORDER BY id{limit_clause};'
                params = {
                    "min_lat": min_lat, "max_lat": max_lat,
                    "min_lon": min_lon, "max_lon": max_lon,
                }
            else:
                print("全てのデータを取得します...")
                final_query_str = f'{base_query} ORDER BY id;'
            
            query = text(final_query_str)
            result_proxy = connection.execute(query, params)
            
            for row in result_proxy:
                results_list.append(dict(row._mapping))
            
            print(f"{len(results_list)}件のデータを取得しました。")

    except OperationalError as e:
        print(f"データベースへの接続に失敗しました: {e}")
        return []
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        return []

    return results_list