from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from includes.step_temp_silver_to_main_silver import temp_silver_to_main_silver

# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_temp_silver_to_main_silver",
    default_args=default_args,
    description="DAG to update the data.",    
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:

    temp_silver_to_main_silver = PythonOperator(
        task_id="temp_silver_to_main_silver",
        python_callable=temp_silver_to_main_silver,
    )

    temp_silver_to_main_silver