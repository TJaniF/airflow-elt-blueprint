# package imports
from airflow.decorators import dag, task 
from pendulum import datetime
import io

# local module imports
from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests climate data from provided csv files to MinIO",
    tags=["ingestion", "minio"]
)
def in_climate_date():

    # create an instance of the CreateBucket task group consisting of 5 tasks 
    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket",
        bucket_name=gv.CLIMATE_BUCKET_NAME
    )
   
    # dynamically mapped task that will create one task instance for each
    # climate data source file provided
    @task(
        outlets=[gv.DS_CLIMATE_DATA_MINIO],
    )
    def ingest_climate_data(source):
        """Opens a csv file provided as input and writes the contents to 
        a bucket in MinIO."""

        # use a utility function to connect to MinIO
        client = gv.get_minio_client()

        # derive MinIO csv key from input file name
        data_scale = source.split("/")[-1].split(".")[0]
        key = f"{data_scale}.csv"

        # read csv and convert to bytes
        with open(source, 'r') as f:
            string_file = f.read()
            bytes_to_write = io.BytesIO(bytes(string_file, 'utf-8'))

        # write bytes to MinIO
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