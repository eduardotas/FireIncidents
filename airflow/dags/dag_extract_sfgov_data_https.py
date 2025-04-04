import requests
import json
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

log = logging.getLogger("dag_extract_sfgov_data_https_logs")

TEMP_BRONZE = "/usr/local/airflow/data/bronze/data.json"
API_URL = "https://data.sfgov.org/resource/wr8u-xric.json"
LIMIT = 50000  # Maximum allowed

def extract_data():
    """
    Extracts data from the Socrata API in batches of 50,000 records and appends them to a JSON file.
    """
    offset = 0
    
    while True:
        url = f"{API_URL}?$limit={LIMIT}&$offset={offset}"
        
        try:
            log.info(f"Start offset:{offset}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()            
            if not data:
                log.info(f"End offset:{offset} - No data to process")
                break  # If there is no more data, exit the loop
            
            # Append data to the file in incremental steps
            with open(TEMP_BRONZE, "a", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
                json_file.write("\n")  # Ensure each batch is on a new line
            log.info(f"End offset:{offset}")
            offset += LIMIT
            
        except requests.exceptions.RequestException as e:
            log.error(f"Error accessing API: {e}")
            raise AirflowFailException(f"Execution failed: {str(e)}")
            break
        
    log.info(f"Data successfully appended to {TEMP_BRONZE}")

# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_extract_sfgov_data_https",
    default_args=default_args,
    description="DAG to extract data from the Socrata API daily",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_sfgov_data_https",
        python_callable=extract_data,
    )

    extract_task
