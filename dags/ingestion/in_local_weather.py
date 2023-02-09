from airflow.decorators import dag, task_group, task 
from pendulum import datetime, duration
from airflow.operators.empty import EmptyOperator

from geopy.geocoders import Nominatim
import requests
import io 
import json

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
    tags=["ingestion", "MinIO"]
)
def in_local_weather():

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
            if gv.WEATHER_BUCKET_NAME in buckets:
                return "bucket_creation.bucket_already_exists"
            else:
                return "bucket_creation.create_current_weather_bucket"
            

        @task
        def create_current_weather_bucket():
            client = gv.get_minio_client()
            client.make_bucket(
                gv.WEATHER_BUCKET_NAME
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

    @task
    def get_lat_long_for_city(city):
        geolocator = Nominatim(user_agent="MyApp")

        location = geolocator.geocode(city)
        lat = location.latitude
        long = location.longitude

        gv.task_log.info(
            f"Coordinates for {city}: {lat}/{long}"
        )

        return {"city": city, "lat": lat, "long": long}

    # note: add something to put into the streamlit in case API fails
    @task
    def get_current_weather(coordinates):
        lat = coordinates["lat"]
        long = coordinates["long"]
        city = coordinates["city"]

        r = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true"
        )

        if r.status_code == 200:
            current_weather = r.json()["current_weather"]

            gv.task_log.info(
                "The current temperature in {0} is {1}°C".format(
                    city,
                    current_weather["temperature"]
                )
            )

        else:
            current_weather = "NA"

            gv.task_log.warn(
                f"""
                    Could not retrieve current temperature for {city} from
                    https://api.open/meteo.com.
                    Request returned {r.status_code}
                """
            )

        return {"city": city, "current_weather": current_weather}

    @task(
        outlets=[gv.DS_WEATHER_DATA_MINIO]
    )
    def write_current_weather_to_minio(weather_data):
        city = weather_data["city"]
        timestamp = weather_data["current_weather"]["time"]
        client = gv.get_minio_client()

        key = f"{city}/{timestamp}_{city}_weather.json"

        bytes_to_write = io.BytesIO(bytes(json.dumps(weather_data), 'utf-8'))

        client.put_object(
            gv.WEATHER_BUCKET_NAME,
            key,
            bytes_to_write,
            -1, # -1 = unknown filesize
            part_size=10*1024*1024,
        )

        gv.task_log.info(f"Wrote weather in {city} at {timestamp} to MinIO.")
        
        return weather_data

    # set dependencies

    coordinates = get_lat_long_for_city(gv.MY_CITY)
    current_weather = get_current_weather(coordinates)
    bucket_creation() >> write_current_weather_to_minio(current_weather)


in_local_weather()