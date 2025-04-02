import requests
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

TEMP_FILE = "/usr/local/airflow/data/bronze/data.json"

def extract_data():
    """
    Extracts data from the SFGov API and saves it as a JSON file in the specified location.    
    """
    url = "https://data.sfgov.org/resource/wr8u-xric.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        with open(TEMP_FILE, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        print(f"Data successfully saved to {TEMP_FILE}")

    except requests.exceptions.RequestException as e:
        print(f"Error accessing API: {e}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_extract_sfgov_data",
    default_args=default_args,
    description="DAG to extract data from the SFGov API daily",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_sfgov_data",
        python_callable=extract_data,        
    )

    extract_task
