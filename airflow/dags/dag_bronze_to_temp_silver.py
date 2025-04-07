from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from includes.step_bronze_to_temp_silver import spark_transform

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

    spark_transform = PythonOperator(
        task_id="Spark_transform_data",
        python_callable=spark_transform,
    )

    spark_transform