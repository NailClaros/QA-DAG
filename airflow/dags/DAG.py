from __future__ import annotations
import os
import datetime as dt
import requests
from typing import List, Dict, Tuple

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

load_dotenv()

# Default arguments
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
}

# Constants
UNIT_LIST = [2324, 1307714, 4272428, 25516, 12915154, 1188, 1758, 3952, 4272273, 3316, 3324, 4272325]
LOC_DICT = {
    "Garinger, NC": 1297,
    "Washington, DC": 691,
    "Elizabeth, NJ": 971,
    "Miami, FL": 1877
}
API_KEY = os.getenv("QA_API_KEY", "___")
API_BASE = "https://api.openaq.org/v3"

today = dt.date.today().strftime("%Y-%m-%d")

# DAG definition
@dag(
    dag_id="qa_daily_ingest",
    start_date=dt.datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,  
    default_args=DEFAULT_ARGS,
    tags=["qa", "openaq"],
)
def qa_daily_ingest():

    # Step 1: Fetch sensors for each location
    @task
    def fetch_sensors() -> Dict[str, List[Dict]]:
        daily_sensors = {}
        for location, location_id in LOC_DICT.items():
            r = requests.get(f"{API_BASE}/locations/{location_id}/sensors",
                             headers={"X-API-Key": API_KEY}, timeout=30)
            r.raise_for_status()
            daily_sensors[location] = r.json()["results"]
        return daily_sensors

    # Step 2: Fetch latest sensor values and normalize
    @task
    def fetch_and_normalize(sensors_by_location: Dict[str, List[Dict]]) -> List[Tuple]:
        row_data = []
        for location, sensors in sensors_by_location.items():
            r = requests.get(f"{API_BASE}/locations/{LOC_DICT[location]}/latest",
                             headers={"X-API-Key": API_KEY}, timeout=30)
            r.raise_for_status()
            sensor_values = {s["sensorsId"]: s["value"] for s in r.json()["results"]
                             if s["sensorsId"] in UNIT_LIST}

            for sensor in sensors:
                if sensor["id"] not in UNIT_LIST:
                    continue
                sid = sensor["id"]
                row_data.append(
                    [
                        location,
                        f"{sensor['parameter']['displayName']} {sensor['parameter']['units']}",
                        sensor_values.get(sid),
                        today
                    ]
                )
        return row_data

    # Step 3: Insert data into database
    @task
    def insert_rows(rows: List[Tuple]) -> int:
        if not rows:
            return 0
        hook = PostgresHook(postgres_conn_id="neon_postgres")
        sql = """
        INSERT INTO aq_data.daily_measurements
            (location, sensor_name_units, measurement, date_inserted)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location, sensor_name_units, date_inserted)
        DO NOTHING;
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    # DAG flow
    sensors = fetch_sensors()
    rows = fetch_and_normalize(sensors)
    inserted_count = insert_rows(rows)

    sensors >> rows >> inserted_count

qa_daily_ingest()
