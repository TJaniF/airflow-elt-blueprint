from airflow.decorators import dag, task 
from pendulum import datetime, duration
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

from geopy.geocoders import Nominatim
import duckdb
import logging
import os
import requests

MY_NAME = "Jani"
MY_CITY = "Basel"
MY_COUNTRY = "Switzerland"
CLIMATE_DATA_SOURCE_CITIES = f"{os.environ['AIRFLOW_HOME']}/include/\
    climate_data/temp_countries.csv"

CLIMATE_DATA_SOURCE_GLOBAL = f"{os.environ['AIRFLOW_HOME']}/include/\
    climate_data/temp_global.csv"


default_args = {
    'owner': MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

# get Airflow task logger
log = logging.getLogger('airflow.task')


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["minio", "duckdb", "streamlit"]
)
def DEV_DAG():

    @task
    def get_lat_long_for_city(city):
        geolocator = Nominatim(user_agent="MyApp")

        location = geolocator.geocode(city)
        lat = location.latitude
        long = location.longitude

        log.info(
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

            log.info(
                "The current temperature in {0} is {1}°C".format(
                    city,
                    current_weather["temperature"]
                )
            )

        else:
            current_weather = "NA"

            log.warn(
                f"""
                    Could not retrieve current temperature for {city} from
                    https://api.open/meteo.com.
                    Request returned {r.status_code}
                """
            )

        return {"city": city, "current_weather": current_weather}

    @task 
    def write_current_weather_to_minio(current_weather):
        pass

    @task
    def ingest_climate_data(source):

        return {"current_weather": "placeholder"}


    # custom MinIO to duckdb operator
    extract_minio_to_duckdb = EmptyOperator(task_id="extract_minio_to_duckdb")

    @task 
    def create_reporting_table():
        # queries duckdb ingest table and creates a reporting table
        cursor = duckdb.connect()
        print(cursor.execute('SELECT 42').fetchall())
        pass

    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        bash_command="streamlit run streamlit_test.py &",
        cwd="include"
    )

    # set dependencies
    coordinates = get_lat_long_for_city(MY_CITY)
    current_weather = get_current_weather(coordinates)
    write_weather = write_current_weather_to_minio(current_weather)
    transform = create_reporting_table()

    write_weather >> transform
    

    chain(
        ingest_climate_data(CLIMATE_DATA_SOURCE_CITIES),
        extract_minio_to_duckdb,
        transform,
        run_streamlit_script
    )   

DEV_DAG()