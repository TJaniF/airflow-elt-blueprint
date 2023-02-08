from airflow import Dataset
from airflow.decorators import dag, task 
from pendulum import datetime, duration
from airflow.operators.empty import EmptyOperator

from include.global_variables import global_variables as gv

default_args = {
    'owner': gv.MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["ingestion", "minio"]
)
def in_climate_date():

   
    @task
    def ingest_climate_data_my_city(source):

        return source
    
    @task
    def ingest_climate_data_global(source):

        return source
    
    in_complete = EmptyOperator(
        task_id="ingestion_complete",
        outlets=[gv.DS_CLIMATE_DATA_MINIO]
    )

    # set dependencies
    ingest_climate_data_my_city(gv.CLIMATE_DATA_SOURCE_CITIES) >> in_complete
    ingest_climate_data_global(gv.CLIMATE_DATA_SOURCE_GLOBAL) >> in_complete


in_climate_date()