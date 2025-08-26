from dotenv import load_dotenv
from airflow.sdk import dag, task
import datetime as dt
load_dotenv()
import os
import requests
import psycopg2
import logging

UNIT_LIST = [2324, 1307714, 4272428, 25516, 12915154, 1188, 1758, 3952, 4272273, 3316, 3324, 4272325]
LOC_DICT = {
    "Garinger, NC": 1297,
    "Washington, DC": 691,
    "Elizabeth, NJ": 971,
    "Miami, FL": 1877
}

DB_URL = os.getenv("DB_URL", "____")
API_KEY = os.getenv("QA_API_KEY", "____")
API_BASE = "https://api.openaq.org/v3"


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

@dag(
    dag_id="AQ_DAG",
    schedule="@daily",
    start_date=dt.datetime(2025, 8, 22),
    catchup=False,
)
def run_dag():

    @task
    def extract():
        logger.info("Fetching daily data from OpenAQ...")
        today = dt.date.today().strftime("%Y-%m-%d")
        row_data = []

        for location, location_id in LOC_DICT.items():
            logger.info(f"-- Fetching data for {location} (ID: {location_id}) --")
            try:
                sensors_resp = requests.get(f"{API_BASE}/locations/{location_id}/sensors",
                                            headers={"X-API-Key": API_KEY})
                sensors_resp.raise_for_status()
                sensors = sensors_resp.json()["results"]

                values_resp = requests.get(f"{API_BASE}/locations/{location_id}/latest",
                                           headers={"X-API-Key": API_KEY})
                values_resp.raise_for_status()
                sensor_values = values_resp.json()["results"]

            except Exception as e:
                logger.error(f"Failed to fetch data for {location}: {e}")
                raise  # Stop here

            daily_data = {sensor["sensorsId"]: sensor["value"] for sensor in sensor_values if sensor["sensorsId"] in UNIT_LIST}

            for sensor in sensors:
                if sensor["id"] in UNIT_LIST:
                    param = f"{location} {sensor['parameter']['displayName']} {sensor['parameter']['units']}"
                    row_data.append([
                        location,
                        param,
                        daily_data.get(sensor["id"], None),
                        today
                    ])

        if not row_data:
            logger.error("No data extracted! Stopping DAG.")
            raise ValueError("Extraction failed: no data retrieved from API.")
            # Stop here

        logger.info("Data extraction complete.")
        return row_data

    @task
    def transform(row_data):
        logger.info("Starting data transformation...")
        try:
            transformed = [row for row in row_data if row[2] is not None]
            if not transformed:
                logger.error("No valid rows after transformation! Stopping DAG.")
                raise ValueError("Transformation failed: all rows have null measurements.")
        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise 
        logger.info(f"Transformation complete. {len(transformed)} rows kept.")
        return transformed

    @task
    def load(transformed_data):
        logger.info("Starting data load into database...")
        try:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor()
            for row in transformed_data:
                cur.execute("""INSERT INTO aq_data.daily_measurements 
                            (location, sensor_name_units, measurement, date_inserted) 
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (location, sensor_name_units, date_inserted) DO NOTHING""",
                            (row[0], row[1], row[2], row[3]))
            conn.commit()
        except Exception as e:
            logger.error(f"Database load failed: {e}")
            raise
        finally:
            cur.close()
            conn.close()
        logger.info(f"Data load complete. {len(transformed_data)} rows inserted.")
        logger.info("DAG run completed successfully.")

    load(transform(extract()))

run_dag()