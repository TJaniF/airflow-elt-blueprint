"""DAG that loads climate ingests from local csv files into MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import io

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests climate data from provided csv files to MinIO.",
    tags=["ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_climate_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_climate_bucket", bucket_name=gv.CLIMATE_BUCKET_NAME
    )

    # use the custom LocalCSVToMinIOOperator to read the contents in /include/climate
    # into MinIO. This task uses dynamic task allowing you to add additional files to
    # the folder and reading them in without changing any DAG code
    ingest_climate_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_climate_data",
        bucket_name=gv.CLIMATE_BUCKET_NAME,
        outlets=[gv.DS_CLIMATE_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.TEMP_GLOBAL_PATH,
                "object_name": gv.TEMP_GLOBAL_PATH.split("/")[-1],
            },
        ]
    )

    # set dependencies
    create_bucket_tg >> ingest_climate_data


in_climate_data()
