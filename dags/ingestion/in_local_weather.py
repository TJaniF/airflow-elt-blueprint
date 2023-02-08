from airflow import Dataset
from airflow.decorators import dag, task 
from pendulum import datetime, duration

from geopy.geocoders import Nominatim
import requests

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
    def write_current_weather_to_minio(current_weather):
        return current_weather

    # set dependencies
    coordinates = get_lat_long_for_city(gv.MY_CITY)
    current_weather = get_current_weather(coordinates)
    write_current_weather_to_minio(current_weather)


in_local_weather()