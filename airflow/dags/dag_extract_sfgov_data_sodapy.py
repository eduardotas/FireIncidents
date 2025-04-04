import requests
import json
import os
from sodapy import Socrata
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

log = logging.getLogger("dag_extract_sfgov_data_sodapy_logs")
TEMP_BRONZE = "/usr/local/airflow/data/bronze/data_sodapy.json"
DATASET_ID = "wr8u-xric" 
LIMIT = 50000  # Maximum allowed

def extract_data():
    """
    Extracts data from the Socrata API in batches of 50,000 records and appends them to a JSON file.
    """
    # Configuração do cliente Socrata
    client = Socrata("data.sfgov.org", None)
    offset = 0
    
    while True:
        try:
            log.info(f"Start offset:{offset}")
            # Buscar os dados com limite e offset
            data = client.get(DATASET_ID, limit=LIMIT, offset=offset)
            
            if not data:  # Se não houver mais dados, sair do loop
                log.info(f"End offset:{offset} - No data to process")
                break        

            with open(TEMP_BRONZE, "a", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
                json_file.write("\n")  # Ensure each batch is on a new line

            offset += LIMIT
            log.info(f"End offset:{offset}")
        
        except Exception as e:            
            log.error(f"Error API: {e}")
            raise AirflowFailException(f"Execution failed: {str(e)}")
            break

# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_extract_sfgov_data_sodapy",
    default_args=default_args,
    description="DAG to extract data from the Socrata API daily",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_sfgov_data_sodapy",
        python_callable=extract_data,
    )

    extract_task
