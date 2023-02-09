from airflow.decorators import dag, task
from pendulum import datetime, duration

import duckdb

from include.global_variables import global_variables as gv


default_args = {
    'owner': gv.MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_IN_WEATHER, gv.DS_DUCKDB_IN_CLIMATE],
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["transform", "duckdb"]
)
def create_reporting_table():

    @task(
        outlets=[gv.DS_DUCKDB_REPORTING]
    )
    def create_country_reporting():

        table_name = f"{gv.MY_COUNTRY}_temp"
        
        cursor = duckdb.connect("dwh")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT *
            FROM temp_countries_table
            WHERE country = '{gv.MY_COUNTRY}'
            """
        )
        cursor.commit()
        cursor.close()

    create_country_reporting()
  

create_reporting_table()