from airflow import Dataset
import logging
import os

# ENTER YOU OWN INFO!
MY_NAME = "Jani"
MY_CITY = "New York"
MY_COUNTRY = "US"

# Source files
CLIMATE_DATA_SOURCE_CITIES = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_countries.csv"
CLIMATE_DATA_SOURCE_GLOBAL = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"

# Datasets
DS_CLIMATE_DATA_MINIO = Dataset("minio://climate_data")
DS_WEATHER_DATA_MINIO = Dataset("minio://current_weather")
DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")

# get Airflow task logger
task_log = logging.getLogger('airflow.task')