import json
import logging
from includes.constants import SPARK_POSTGRES_JAR, BASE_PATH_BRONZE, LATEST_FILE, POSTGRES_PASSWORD, POSTGRES_NAME, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, SCHEMA_SILVER, TEMP_TABLE
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, when, to_date, to_timestamp
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException

log = logging.getLogger("step_bronze_to_temp_silver")

def get_last_file():
    with open(f"{BASE_PATH_BRONZE}{LATEST_FILE}", "r") as f:
        latest_file = f.read().strip()
    
    return latest_file

def spark_transform():
    try:
        spark = SparkSession.builder \
            .appName("IncidentsTransform") \
            .config("spark.driver.memory", "2g") \
            .config("spark.jars", SPARK_POSTGRES_JAR) \
            .getOrCreate()
    except Exception as e:
        raise AirflowFailException(f"Error creating SparkSession: {e}")
    
    try:
        file_path = get_last_file()
        log.info(f"Reading file {file_path}...")
        df = spark.read.json(file_path)
    except Exception as e:
        raise AirflowFailException(f"Error reading JSON file: {e}")
    
    try:
        log.info("Adjusting 'point' column...")
        df = df.withColumn("point", 
            concat(
                lit("POINT ("), 
                col("point.coordinates").getItem(0).cast("string"), 
                lit(" "), 
                col("point.coordinates").getItem(1).cast("string"), 
                lit(")")
            )
        )
    except Exception as e:
        raise AirflowFailException(f"Error adjusting 'point' column: {e}")
    
    try:
        log.info("Reordering columns...")
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
    except Exception as e:
        raise AirflowFailException(f"Error reordering columns: {e}")
    
    try:
        log.info("Casting column types...")
        df = df.withColumn("incident_number", col("incident_number").cast("long")) \
           .withColumn("exposure_number", col("exposure_number").cast("int")) \
           .withColumn("id", col("id").cast("long")) \
           .withColumn("call_number", col("call_number").cast("long")) \
           .withColumn("incident_date", to_date("incident_date")) \
           .withColumn("alarm_dttm", to_timestamp("alarm_dttm")) \
           .withColumn("arrival_dttm", to_timestamp("arrival_dttm")) \
           .withColumn("close_dttm", to_timestamp("close_dttm")) \
           .withColumn("suppression_units", col("suppression_units").cast("int")) \
           .withColumn("suppression_personnel", col("suppression_personnel").cast("int")) \
           .withColumn("ems_units", col("ems_units").cast("int")) \
           .withColumn("ems_personnel", col("ems_personnel").cast("int")) \
           .withColumn("other_units", col("other_units").cast("int")) \
           .withColumn("other_personnel", col("other_personnel").cast("int")) \
           .withColumn("fire_fatalities", col("fire_fatalities").cast("int")) \
           .withColumn("fire_injuries", col("fire_injuries").cast("int")) \
           .withColumn("civilian_fatalities", col("civilian_fatalities").cast("int")) \
           .withColumn("civilian_injuries", col("civilian_injuries").cast("int")) \
           .withColumn("number_of_alarms", col("number_of_alarms").cast("int")) \
           .withColumn("data_as_of", to_timestamp("data_as_of")) \
           .withColumn("data_loaded_at", to_timestamp("data_loaded_at"))
    except Exception as e:
        raise AirflowFailException(f"Error casting column types: {e}")
    
    try:
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        table_name = f"{SCHEMA_SILVER}.{TEMP_TABLE}"

        log.info(f"Writing to database, table {table_name}...")

        df.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
        
        log.info("Done!")
    except Exception as e:
        raise AirflowFailException(f"Error writing to the database: {e}")