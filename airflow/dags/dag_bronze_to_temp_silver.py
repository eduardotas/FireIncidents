from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from includes.step_bronze_to_temp_silver import bronze_to_temp_silver

# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_bronze_to_temp_silver",
    default_args=default_args,
    description="DAG to extract data from the Bronze to Temp Silver layer",    
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:

    bronze_to_temp_silver = PythonOperator(
        task_id="bronze_to_temp_silver",
        python_callable=bronze_to_temp_silver,
    )

    bronze_to_temp_silver