from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'openaq_us_daily',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # daily at 1 AM
    catchup=False
)

def fetch_openaq_data(**kwargs):
    execution_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    date_from = execution_date - timedelta(days=1)
    date_to = execution_date - timedelta(days=1)
    
    url = f"https://api.openaq.org/v2/measurements"
    
    params = {
        "country": "US",
        "date_from": date_from.strftime('%Y-%m-%dT00:00:00Z'),
        "date_to": date_to.strftime('%Y-%m-%dT23:59:59Z'),
        "limit": 100,
        "page": 1
    }
    
    all_data = []
    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data['results'])
        
        # Check for pagination
        if 'meta' in data and data['meta']['found'] > params['limit'] * params['page']:
            params['page'] += 1
        else:
            break
    
    df = pd.json_normalize(all_data)
    df.to_csv(f'/tmp/openaq_us_{date_from.strftime("%Y-%m-%d")}.csv', index=False)
    print(f"Saved {len(df)} records to CSV")

fetch_task = PythonOperator(
    task_id='fetch_openaq_data',
    python_callable=fetch_openaq_data,
    provide_context=True,
    dag=dag
)

fetch_task
