"""DAG that loads climate and weather data from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime, parse
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import (
    MinIOListOperator,
    MinIOCopyObjectOperator,
    MinIODeleteObjectsOperator,
)

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in MinIO
    schedule=[gv.DS_CLIMATE_DATA_MINIO, gv.DS_WEATHER_DATA_MINIO],
    catchup=False,
    default_args=gv.default_args,
    description="Loads climate and weather data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def load_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket", bucket_name=gv.ARCHIVE_BUCKET_NAME
    )

    list_files_climate_bucket = MinIOListOperator(
        task_id="list_files_climate_bucket", bucket_name=gv.CLIMATE_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_CLIMATE], pool="duckdb")
    def load_climate_data(obj):
        """Loads content of one fileobject in the MinIO climate bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.CLIMATE_BUCKET_NAME, obj, file_path=obj)

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)

    list_files_weather_bucket = MinIOListOperator(
        task_id="list_files_weather_bucket", bucket_name=gv.WEATHER_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_WEATHER], pool="duckdb")
    def load_weather_data(city, obj):
        """Loads content of one fileobject in the MinIO weather bucket
        to DuckDB."""

        minio_client = gv.get_minio_client()
        # get the object from MinIO and save as a local tmp file
        minio_client.fget_object(gv.WEATHER_BUCKET_NAME, obj, file_path=obj)

        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)

        # open the local tmp file to extract information
        with open(obj) as f:
            weather_data = json.load(f)
            print(weather_data)
            city = weather_data["city"]
            api_response = weather_data["API_response"]
            timestamp = weather_data["current_weather"]["time"].replace("T", " ")
            timestamp = parse(timestamp)
            temperature = weather_data["current_weather"]["temperature"]
            windspeed = weather_data["current_weather"]["windspeed"]
            winddirection = weather_data["current_weather"]["winddirection"]
            weathercode = weather_data["current_weather"]["weathercode"]

        # write extracted information to DuckDB
        cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {gv.WEATHER_IN_TABLE_NAME} (
                    CITY VARCHAR(255),
                    API_RESPONSE INT,
                    TIMESTAMP TIMESTAMP,
                    TEMPERATURE FLOAT,
                    WINDSPEED FLOAT,
                    WINDDIRECTION FLOAT,
                    WEATHERCODE FLOAT,
                );
                INSERT INTO {gv.WEATHER_IN_TABLE_NAME} VALUES (
                    '{city}',
                    {api_response},
                    '{timestamp}',
                    {temperature},
                    {windspeed},
                    {winddirection},
                    {weathercode}
                );"""
        )
        cursor.commit()
        cursor.close()

        # remove tmp json file
        os.remove(obj)

    @task
    def get_copy_args(obj_list_weather, obj_list_climate):
        """Return tuples with bucket names and bucket contents."""

        return [
            {
                "source_bucket_name": gv.WEATHER_BUCKET_NAME,
                "source_object_names": obj_list_weather,
                "dest_object_names": obj_list_weather,
            },
            {
                "source_bucket_name": gv.CLIMATE_BUCKET_NAME,
                "source_object_names": obj_list_climate,
                "dest_object_names": obj_list_climate,
            },
        ]

    copy_objects_to_archive = MinIOCopyObjectOperator.partial(
        task_id="copy_objects_to_archive",
        dest_bucket_name=gv.ARCHIVE_BUCKET_NAME,
    ).expand_kwargs(
        get_copy_args(
            list_files_weather_bucket.output, list_files_climate_bucket.output
        )
    )

    @task
    def get_deletion_args(obj_list_weather, obj_list_climate):
        """Return tuples with bucket names and bucket contents."""

        return [
            {"bucket_name": gv.WEATHER_BUCKET_NAME, "object_names": obj_list_weather},
            {"bucket_name": gv.CLIMATE_BUCKET_NAME, "object_names": obj_list_climate},
        ]

    delete_objects = MinIODeleteObjectsOperator.partial(
        task_id="delete_objects",
    ).expand_kwargs(
        get_deletion_args(
            list_files_weather_bucket.output, list_files_climate_bucket.output
        )
    )

    # set dependencies

    climate_data = load_climate_data.expand(obj=list_files_climate_bucket.output)
    weather_data = load_weather_data.partial(city=gv.MY_CITY).expand(
        obj=list_files_weather_bucket.output
    )
    
    archive_bucket = create_bucket_tg

    [climate_data, weather_data] >> archive_bucket
    (archive_bucket >> [copy_objects_to_archive] >> delete_objects)


load_data()
