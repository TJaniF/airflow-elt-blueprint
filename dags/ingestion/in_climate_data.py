from airflow import Dataset
from airflow.decorators import dag, task_group, task 
from pendulum import datetime, duration
from airflow.operators.empty import EmptyOperator
import io

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket

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

    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket",
        bucket_name=gv.CLIMATE_BUCKET_NAME
    )
   
    @task(
        outlets=[gv.DS_CLIMATE_DATA_MINIO]
    )
    def ingest_climate_data(source):
        client = gv.get_minio_client()
        data_scale = source.split("/")[-1].split(".")[0]

        key = f"{data_scale}.csv"

        with open(source, 'r') as f:
            string_file = f.read()
            bytes_to_write = io.BytesIO(bytes(string_file, 'utf-8'))

        client.put_object(
            gv.CLIMATE_BUCKET_NAME,
            key,
            bytes_to_write,
            -1, # -1 = unknown filesize
            part_size=10*1024*1024,
        )

        return source

    # set dependencies
    create_bucket_tg >> ingest_climate_data.expand(source=gv.CLIMATE_DATA_SOURCES)


in_climate_date()