from airflow.decorators import dag, task 
from pendulum import datetime, duration

import duckdb
import os

from include.global_variables import global_variables as gv

default_args = {
    'owner': gv.MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_CLIMATE_DATA_MINIO, gv.DS_WEATHER_DATA_MINIO],
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["load", "minio", "duckdb"]
)
def load_data():

    @task(
        outlets=[gv.DS_DUCKDB_IN_CLIMATE]
    )
    def load_climate_data():
        pass 

    @task(
        outlets=[gv.DS_DUCKDB_IN_WEATHER]
    )
    def load_weather_data():
        pass

    load_climate_data()
    load_weather_data()


load_data()