from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, when
import json
from datetime import datetime, timedelta
import logging

log = logging.getLogger("pyspark_teste")

FILE_PATH = f"/usr/local/airflow/data/bronze/2025-04-04/06-18/data.json"

def spark_transform():    
    spark = SparkSession.builder \
        .appName("IncidentsTransform") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()    
    
    df = spark.read.json(FILE_PATH)    
    
    df = df.withColumn("point", 
        concat(
            lit("POINT ("), 
            col("point.coordinates").getItem(0).cast("string"), 
            lit(" "), 
            col("point.coordinates").getItem(1).cast("string"), 
            lit(")")
            )
        )
    
    orded_columns = [
        "incident_number", "exposure_number", "id", "address", "incident_date", 
        "call_number", "alarm_dttm", "arrival_dttm", "close_dttm", "city", "zipcode", 
        "battalion", "station_area", "suppression_units", "suppression_personnel", 
        "ems_units", "ems_personnel", "other_units", "other_personnel", "first_unit_on_scene", 
        "fire_fatalities", "fire_injuries", "civilian_fatalities", "civilian_injuries", 
        "number_of_alarms", "primary_situation", "mutual_aid", "action_taken_primary", 
        "property_use", "supervisor_district", "neighborhood_district", "point", 
        "data_as_of", "data_loaded_at"
    ]

    df = df.select(*orded_columns)

    jdbc_url = "jdbc:postgresql://datalake_postgres:5432/fire_incidents"
    # jdbc_url = "jdbc:postgresql://<nome_do_container_ou_ip>:5432/<nome_do_banco>"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
    table_name = "silver.incidents"

    df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)


# DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "bronze_to_silver",
    default_args=default_args,
    description="DAG to extract data from the Socrata API daily",    
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["extract", "sfgov"],
) as dag:

    task = PythonOperator(
        task_id="transform_data",
        python_callable=spark_transform,
    )

    task