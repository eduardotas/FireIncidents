from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
import json
from datetime import datetime, timedelta
import logging

log = logging.getLogger("pyspark_teste")

FILE_PATH = f"/usr/local/airflow/data/bronze/2025-04-04/03-23/data.json"

def spark_transform():    
    spark = SparkSession.builder \
        .appName("IncidentsTransform") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()    
    
    df = spark.read.option("mode", "DROPMALFORMED").json(FILE_PATH)    
    df.printSchema()    

    #Tratativa do point
    df = df.withColumn("longitude", col("point.coordinates")[0]) \
       .withColumn("latitude", col("point.coordinates")[1])
    
    df = df.withColumn("point",
    concat_ws(" ",
        lit("POINT("),
        col("point.coordinates")[0],
        col("point.coordinates")[1],
        lit(")")
    ))
    

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