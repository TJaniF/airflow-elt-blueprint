# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
import json

# -------------------- #
# Enter your own info! #
# -------------------- #

MY_NAME = "Friend"
MY_CITY = "Portland"

# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
WEATHER_BUCKET_NAME = "weather"
CLIMATE_BUCKET_NAME = "climate"
ARCHIVE_BUCKET_NAME = "archive"

# Source file path climate data
TEMP_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"

# Datasets
DS_CLIMATE_DATA_MINIO = Dataset(f"minio://{CLIMATE_BUCKET_NAME}")
DS_WEATHER_DATA_MINIO = Dataset(f"minio://{WEATHER_BUCKET_NAME}")
DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
WEATHER_IN_TABLE_NAME = "in_weather"
CLIMATE_TABLE_NAME = "temp_global_table"
REPORTING_TABLE_NAME = "reporting_table"
CONN_ID_DUCKDB = "duckdb_default"

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
}

# default coordinates
default_coordinates = {"city": "No city provided", "lat": 0, "long": 0}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client


# command to run streamlit app within codespaces/docker
# modifications are necessary to support double-port-forwarding
STREAMLIT_COMMAND = "streamlit run weather_v_climate_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"
