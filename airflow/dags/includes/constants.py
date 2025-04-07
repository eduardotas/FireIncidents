import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_NAME = os.environ.get("POSTGRES_NAME")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
SCHEMA_SILVER = "silver"
TEMP_TABLE = "temp_incidents"
MAIN_TABLE = "incidents"

FILE_NAME = "data"
FILE_EXTENSION = ".json"
DATASET_ID = "wr8u-xric" 
BASE_PATH_BRONZE = "/usr/local/airflow/data/bronze/"
SPARK_POSTGRES_JAR = "/opt/spark/jars/postgresql-42.5.0.jar"

LATEST_FILE = "latest_file.txt"

EXPECTED_BRONZE_SCHEMA = StructType([
    StructField("action_taken_other", StringType(), True),
    StructField("action_taken_primary", StringType(), True),
    StructField("action_taken_secondary", StringType(), True),
    StructField("address", StringType(), True),
    StructField("alarm_dttm", StringType(), True),
    StructField("area_of_fire_origin", StringType(), True),
    StructField("arrival_dttm", StringType(), True),
    StructField("automatic_extinguishing_system_present", StringType(), True),
    StructField("automatic_extinguishing_sytem_failure_reason", StringType(), True),
    StructField("automatic_extinguishing_sytem_perfomance", StringType(), True),
    StructField("automatic_extinguishing_sytem_type", StringType(), True),
    StructField("battalion", StringType(), True),
    StructField("box", StringType(), True),
    StructField("call_number", StringType(), True),
    StructField("city", StringType(), True),
    StructField("civilian_fatalities", StringType(), True),
    StructField("civilian_injuries", StringType(), True),
    StructField("close_dttm", StringType(), True),
    StructField("data_as_of", StringType(), True),
    StructField("data_loaded_at", StringType(), True),
    StructField("detector_alerted_occupants", StringType(), True),
    StructField("detector_effectiveness", StringType(), True),
    StructField("detector_failure_reason", StringType(), True),
    StructField("detector_operation", StringType(), True),
    StructField("detector_type", StringType(), True),
    StructField("detectors_present", StringType(), True),
    StructField("ems_personnel", StringType(), True),
    StructField("ems_units", StringType(), True),
    StructField("estimated_contents_loss", StringType(), True),
    StructField("estimated_property_loss", StringType(), True),
    StructField("exposure_number", StringType(), True),
    StructField("fire_fatalities", StringType(), True),
    StructField("fire_injuries", StringType(), True),
    StructField("fire_spread", StringType(), True),
    StructField("first_unit_on_scene", StringType(), True),
    StructField("floor_of_fire_origin", StringType(), True),
    StructField("heat_source", StringType(), True),
    StructField("human_factors_associated_with_ignition", StringType(), True),
    StructField("id", StringType(), True),
    StructField("ignition_cause", StringType(), True),
    StructField("ignition_factor_primary", StringType(), True),
    StructField("ignition_factor_secondary", StringType(), True),
    StructField("incident_date", StringType(), True),
    StructField("incident_number", StringType(), True),
    StructField("item_first_ignited", StringType(), True),
    StructField("mutual_aid", StringType(), True),
    StructField("neighborhood_district", StringType(), True),
    StructField("no_flame_spread", StringType(), True),
    StructField("number_of_alarms", StringType(), True),
    StructField("number_of_floors_with_extreme_damage", StringType(), True),
    StructField("number_of_floors_with_heavy_damage", StringType(), True),
    StructField("number_of_floors_with_minimum_damage", StringType(), True),
    StructField("number_of_floors_with_significant_damage", StringType(), True),
    StructField("number_of_sprinkler_heads_operating", StringType(), True),
    StructField("other_personnel", StringType(), True),
    StructField("other_units", StringType(), True),
    StructField("point", StructType([
        StructField("coordinates", ArrayType(DoubleType(), True), True),
        StructField("type", StringType(), True)
    ]), True),
    StructField("primary_situation", StringType(), True),
    StructField("property_use", StringType(), True),
    StructField("station_area", StringType(), True),
    StructField("structure_status", StringType(), True),
    StructField("structure_type", StringType(), True),
    StructField("supervisor_district", StringType(), True),
    StructField("suppression_personnel", StringType(), True),
    StructField("suppression_units", StringType(), True),
    StructField("zipcode", StringType(), True),
])