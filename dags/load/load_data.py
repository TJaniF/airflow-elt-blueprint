from airflow.decorators import dag, task 
from pendulum import datetime, duration, parse

import duckdb
import os
import json
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject

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
    schedule=[gv.DS_CLIMATE_DATA_MINIO, gv.DS_WEATHER_DATA_MINIO],
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["load", "minio", "duckdb"]
)
def load_data():

    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket",
        bucket_name=gv.ARCHIVE_BUCKET_NAME
    )

    @task
    def list_files_climate_bucket():
        client = gv.get_minio_client()
        objects = client.list_objects(
            gv.CLIMATE_BUCKET_NAME,
        )
        objects_list = [obj.object_name for obj in objects]
        gv.task_log.info(f"{gv.CLIMATE_BUCKET_NAME} contains {objects_list}")

        return objects_list


    @task(
        outlets=[gv.DS_DUCKDB_IN_CLIMATE],
        max_active_tis_per_dag=1
    )
    def load_climate_data(obj):
        minio_client = gv.get_minio_client()
        minio_client.fget_object(
            gv.CLIMATE_BUCKET_NAME,
            obj,
            file_path=obj
        )

        table_name = obj.split(".")[0] + "_table"
        
        cursor = duckdb.connect("dwh")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        os.remove(obj)

    @task
    def list_files_weather_bucket(city):
        client = gv.get_minio_client()
        objects = client.list_objects(
            gv.WEATHER_BUCKET_NAME,
            prefix=city + "/"
        )
        objects_list = [obj.object_name for obj in objects]
        gv.task_log.info(f"{gv.WEATHER_BUCKET_NAME} contains {objects_list}")

        return objects_list


    @task(
        outlets=[gv.DS_DUCKDB_IN_WEATHER],
        max_active_tis_per_dag=1
    )
    def load_weather_data(city, obj):
        minio_client = gv.get_minio_client()
        minio_client.fget_object(
            gv.WEATHER_BUCKET_NAME,
            obj,
            file_path=obj
        )
        
        cursor = duckdb.connect("dwh")

        with open(obj, 'r') as f:
            weather_data = json.load(f)
            city = weather_data["city"]
            timestamp = weather_data["current_weather"]["time"].replace("T", " ")
            timestamp = parse(timestamp)
            temperature = weather_data["current_weather"]["temperature"]
            windspeed = weather_data["current_weather"]["windspeed"]
            winddirection = weather_data["current_weather"]["winddirection"]
            weathercode = weather_data["current_weather"]["weathercode"]

        cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {gv.WEATHER_IN_TABLE_NAME} (
                    CITY VARCHAR(255),
                    TIMESTAMP TIMESTAMP,
                    TEMPERATURE FLOAT,
                    WINDSPEED FLOAT,
                    WINDDIRECTION FLOAT,
                    WEATHERCODE FLOAT,
                );
                INSERT INTO {gv.WEATHER_IN_TABLE_NAME} VALUES (
                    '{city}',
                    '{timestamp}',
                    {temperature},
                    {windspeed},
                    {winddirection},
                    {weathercode}
                );"""
        )
        cursor.commit()
        cursor.close()

        os.remove(obj)

    @task
    def copy_objects_climate_to_archive(objects):
        client = gv.get_minio_client()
        copy_sources = [
            CopySource(gv.CLIMATE_BUCKET_NAME, obj) for obj in objects
        ]
        for obj, copy_source in zip(objects, copy_sources):
            client.copy_object(
                gv.ARCHIVE_BUCKET_NAME,
                obj,
                copy_source
            )

    @task
    def copy_objects_weather_to_archive(objects):
        client = gv.get_minio_client()
        copy_sources = [
            CopySource(gv.WEATHER_BUCKET_NAME, obj) for obj in objects
        ]
        for obj, copy_source in zip(objects, copy_sources):
            client.copy_object(
                gv.ARCHIVE_BUCKET_NAME,
                obj,
                copy_source
            )

    @task
    def get_deletion_args(obj_list_weather, obj_list_climate):
        return [
            (gv.WEATHER_BUCKET_NAME, obj_list_weather),
            (gv.CLIMATE_BUCKET_NAME, obj_list_climate)
        ]

    @task
    def delete_objects(deletion_args):
        bucket_name = deletion_args[0]
        obj_list = deletion_args[1]
        delete_obj_list = [DeleteObject(obj) for obj in obj_list]
        client = gv.get_minio_client()

        print(bucket_name)
        print(obj_list)
        print(delete_obj_list)

        errors = client.remove_objects(
            bucket_name,
            delete_obj_list,
            bypass_governance_mode=True
        )

        for error in errors:
            print("error occurred when deleting object", error)

    list_objects_climate = list_files_climate_bucket()
    list_objects_weather = list_files_weather_bucket(gv.MY_CITY)

    climate_data = load_climate_data.expand(obj=list_objects_climate)
    weather_data = load_weather_data.partial(city=gv.MY_CITY).expand(
        obj=list_objects_weather
    )

    climate_data >> weather_data

    archive_bucket = create_bucket_tg

    deletion_args = get_deletion_args(list_objects_weather, list_objects_climate)

    [climate_data, weather_data] >> archive_bucket
    archive_bucket >> [
        copy_objects_climate_to_archive(list_objects_climate),
        copy_objects_weather_to_archive(list_objects_weather)
    ] >> delete_objects.expand(deletion_args=deletion_args)


load_data()