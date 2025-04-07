from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

from includes.step_extract_sfgov_data_sodapy import extract_data_from_api

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
    
    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=extract_data_from_api,
    )

    extract_data_from_api
