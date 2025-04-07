import os

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