from airflow import Dataset
from airflow.decorators import dag, task_group, task 
from pendulum import datetime, duration
from airflow.operators.empty import EmptyOperator
import io

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

    @task_group
    def bucket_creation():
        @task
        def list_buckets_minio():
            client = gv.get_minio_client()
            buckets = client.list_buckets()
            bucket_names = [bucket.name for bucket in buckets]
            gv.task_log.info(
                f"MinIO contains the following buckets: {bucket_names}"
            )

            return bucket_names
        
        @task.branch
        def decide_whether_to_create_bucket(buckets):
            if gv.CLIMATE_BUCKET_NAME in buckets:
                return "bucket_creation.bucket_already_exists"
            else:
                return "bucket_creation.create_current_weather_bucket"
            
        @task
        def create_current_weather_bucket():
            client = gv.get_minio_client()
            client.make_bucket(
                gv.CLIMATE_BUCKET_NAME
            )

        bucket_already_exists = EmptyOperator(
            task_id="bucket_already_exists"
        )

        bucket_exists = EmptyOperator(
            task_id="bucket_exists",
            trigger_rule="none_failed_min_one_success"
        )

        # set dependencies within task group
        branch_task = decide_whether_to_create_bucket(list_buckets_minio())
        branch_options = [create_current_weather_bucket(), bucket_already_exists]
        branch_task >> branch_options >> bucket_exists

   
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
    bucket_creation() >> ingest_climate_data.expand(source=gv.CLIMATE_DATA_SOURCES)


in_climate_date()