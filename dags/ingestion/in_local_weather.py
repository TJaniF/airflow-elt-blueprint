"""DAG that queries and ingests local weather data from an API to MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_current_weather_from_city_coordinates,
)

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Queries and ingests local weather data from an API to MinIO.",
    tags=["ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_local_weather():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_weather_bucket", bucket_name=gv.WEATHER_BUCKET_NAME
    )

    # use a python package to get lat/long for a city from its name
    @task
    def get_lat_long_for_city(city):
        city_coordinates = get_lat_long_for_cityname(city)

        # write coordinates to an Airflow variable to use in the streamlit app
        Variable.set(key="city_coordinates", value=json.dumps(city_coordinates))
        return city_coordinates

    # use the open weather API to get the current weather at the provided coordinates
    @task()
    def get_current_weather(coordinates, **context):
        # the logical_date is the date for which the DAG run is scheduled it
        # is retrieved here from the Airflow context
        logical_date = context["logical_date"]
        city_weather_and_coordinates = get_current_weather_from_city_coordinates(
            coordinates, logical_date
        )
        return city_weather_and_coordinates

    # write the weather information to MinIO
    write_current_weather_to_minio = LocalFilesystemToMinIOOperator(
        task_id="write_current_weather_to_minio",
        bucket_name=gv.WEATHER_BUCKET_NAME,
        object_name=f"{gv.MY_CITY}.json",
        # pull the dictionary containing the information from XCom using a Jinja template
        json_serializeable_information="{{ ti.xcom_pull(task_ids='get_current_weather') }}",
        outlets=[gv.DS_WEATHER_DATA_MINIO],
    )

    # set dependencies
    coordinates = get_lat_long_for_city(gv.MY_CITY)
    current_weather = get_current_weather(coordinates)
    create_bucket_tg >> current_weather >> write_current_weather_to_minio


in_local_weather()
