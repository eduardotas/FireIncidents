from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

from includes.step_extract_sfgov_data_sodapy import extract_data

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
