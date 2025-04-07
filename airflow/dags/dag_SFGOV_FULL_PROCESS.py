from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from includes.step_extract_sfgov_data_sodapy import extract_data_from_api
from includes.step_bronze_to_temp_silver import bronze_to_temp_silver
from includes.step_temp_silver_to_main_silver import temp_silver_to_main_silver
from includes.step_silver_to_gold import silver_to_gold

# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_SFGOV_FULL_PROCESS",
    default_args=default_args,
    description="DAG to execute the ETL of sfgov",    
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov", "main"],
) as dag:

    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        python_callable=extract_data_from_api,
    )

    bronze_to_temp_silver = PythonOperator(
        task_id="bronze_to_temp_silver",
        python_callable=bronze_to_temp_silver,
    )

    temp_silver_to_main_silver = PythonOperator(
        task_id="temp_silver_to_main_silver",
        python_callable=temp_silver_to_main_silver,
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    extract_data_from_api >> bronze_to_temp_silver >> temp_silver_to_main_silver >> silver_to_gold