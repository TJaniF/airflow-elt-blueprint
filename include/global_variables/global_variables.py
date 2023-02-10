from airflow import Dataset
import logging
import os
from minio import Minio

# ENTER YOU OWN INFO!
MY_NAME = "Jani"
MY_CITY = "New York"
MY_COUNTRY = "United States"

# MinIO connection config
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_IP="host.docker.internal:9000"
WEATHER_BUCKET_NAME = "weather"
CLIMATE_BUCKET_NAME = "climate"
ARCHIVE_BUCKET_NAME = "archive"

# Source files climate data
CLIMATE_DATA_SOURCES = [
    f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_countries.csv",
    f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"
]

# Datasets
DS_CLIMATE_DATA_MINIO = Dataset(f"minio://{CLIMATE_BUCKET_NAME}")
DS_WEATHER_DATA_MINIO = Dataset(f"minio://{WEATHER_BUCKET_NAME}")
DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")

# DuckDB config
WEATHER_IN_TABLE_NAME = "in_weather"

# get Airflow task logger
task_log = logging.getLogger('airflow.task')

# utility functions
def get_minio_client():
    client = Minio(
        MINIO_IP,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        secure=False
    )

    return client