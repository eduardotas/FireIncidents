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

date = datetime.now().strftime("%Y-%m-%d")
time = datetime.now().strftime("%H-%M")

DIR_BRONZE = f"/usr/local/airflow/data/bronze/{date}/{time}"
FILE_PATH = f"{DIR_BRONZE}/data.json"
DATASET_ID = "wr8u-xric" 
LIMIT = 50000 # Maximum allowed

def extract_data():
    """
    Extracts data from the Socrata API in batches of 50,000 records and appends them to a JSON file.
    """
    os.makedirs(DIR_BRONZE, exist_ok=True)
    # Configuração do cliente Socrata
    client = Socrata("data.sfgov.org", None)
    offset = 0
    first_batch = True    

    while True:
        try:
            log.info(f"Start offset:{offset}")
            # Buscar os dados com limite e offset
            data = client.get(DATASET_ID, limit=LIMIT, offset=offset)
            
            if not data:  # Se não houver mais dados, sair do loop
                log.info(f"End offset:{offset} - No data to process")
                break
            
            with open(FILE_PATH, "a", encoding="utf-8") as json_file:
                for item in data:                
                    json.dump(item, json_file, ensure_ascii=False)
                    json_file.write("\n")  # Adiciona uma nova linha após cada registro  
                    
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
