import os
import sqlite3
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Dict, Any, Optional

# --- Common helper functions ---
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """ Calculate distance (km) between two latitude/longitude points using Haversine formula """
    R = 6371  # Earth radius (km)
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(np.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


# --- SQLite related functions ---

def setup_demo_sqlite_database(db_path: str = 'locations.db'):
    """ Create a demo SQLite database and table, and insert sample data """
    if os.path.exists(db_path):
        print(f"Database '{db_path}' already exists. Skipping setup.")
        return

    print(f"Creating demo database '{db_path}'...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE CSV_data (
            count INTEGER,
            latitude REAL,
            longitude REAL
        )
        """)
        sample_data = [
            (101, 35.6895, 139.6917), # Shinjuku
            (102, 35.5437, 139.7383), # Haneda Airport
            (201, 35.1815, 136.9066), # Nagoya Castle
            (301, 34.6937, 135.5023), # Osaka City Center
            (302, 34.6525, 135.4331), # USJ
            (901, 43.0621, 141.3544), # Sapporo (far from route)
            (902, 26.2124, 127.6809), # Naha (far from route)
        ]
        cursor.executemany("INSERT INTO CSV_data (count, latitude, longitude) VALUES (?, ?, ?)", sample_data)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error occurred while setting up SQLite database: {e}")
    finally:
        if conn:
            conn.close()
    print("Database setup completed.")


def get_nearby_accident_data_from_sqlite(route_points: List[List[float]], db_path: str, radius_km: float) -> List[Dict[str, Any]]:
    """
    Retrieve accident data near the given route from SQLite database with efficient filtering.
    """
    if not route_points:
        return []

    # 1. Calculate bounding box around entire route
    lats = [p[0] for p in route_points]
    lons = [p[1] for p in route_points]
    
    # Add margin for search range
    margin_lat = radius_km / 111.0  # Approx. 111 km per 1 degree latitude
    margin_lon = radius_km / (111.0 * np.cos(np.radians(np.mean(lats)))) # Longitude varies with latitude

    min_lat, max_lat = min(lats) - margin_lat, max(lats) + margin_lat
    min_lon, max_lon = min(lons) - margin_lon, max(lons) + margin_lon

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # 2. Filter candidates using bounding box in SQL
        query = """
        SELECT count, latitude, longitude
        FROM CSV_data
        WHERE (latitude BETWEEN ? AND ?) AND (longitude BETWEEN ? AND ?)
        """
        cursor.execute(query, (min_lat, max_lat, min_lon, max_lon))
        candidate_points = cursor.fetchall()
        conn.close()

        if not candidate_points:
            return []

        # 3. From candidates, select only those within radius from any route point
        nearby_data = []
        for data_count, data_lat, data_lon in candidate_points:
            is_nearby = any(
                haversine(route_lat, route_lon, data_lat, data_lon) <= radius_km
                for route_lat, route_lon in route_points
            )
            if is_nearby:
                nearby_data.append({'count': data_count, 'latitude': data_lat, 'longitude': data_lon})
        
        return nearby_data

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
        return []


# --- PostgreSQL related functions (refactored from original code) ---

def get_accident_data_from_postgres(points: Optional[List[List[float]]] = None, buffer: float = 0.5) -> List[Dict[str, Any]]:
    """
    Retrieve accident data from PostgreSQL database.
    - If points are provided: fetch up to 20 records near the route.
    - If not provided: fetch all records.
    """
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("Error: DATABASE_URL is not set in .env file.")
        return []

    results_list = []
    try:
        engine = create_engine(database_url)
        with engine.connect() as connection:
            base_query = 'SELECT id, "count", "latitude", "longitude" FROM "CSV_data"'
            params = {}
            
            if points and len(points) > 0:
                print(f"Fetching up to 20 records near the route... (Search buffer: {buffer} degrees)")
                lats = [p[0] for p in points]
                lons = [p[1] for p in points]
                
                min_lat, max_lat = min(lats) - buffer, max(lats) + buffer
                min_lon, max_lon = min(lons) - buffer, max(lons) + buffer
                
                where_clause = ' WHERE "latitude" BETWEEN :min_lat AND :max_lat AND "longitude" BETWEEN :min_lon AND :max_lon'
                limit_clause = ' LIMIT 20'
                final_query_str = f'{base_query}{where_clause} ORDER BY id{limit_clause};'
                params = {
                    "min_lat": min_lat, "max_lat": max_lat,
                    "min_lon": min_lon, "max_lon": max_lon,
                }
            else:
                print("Fetching all records...")
                final_query_str = f'{base_query} ORDER BY id;'
            
            query = text(final_query_str)
            result_proxy = connection.execute(query, params)
            
            for row in result_proxy:
                results_list.append(dict(row._mapping))
            
            print(f"Retrieved {len(results_list)} records.")

    except OperationalError as e:
        print(f"Failed to connect to database: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        return []

    return results_list
