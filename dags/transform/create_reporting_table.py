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
    def create_reporting_table():
        # queries duckdb ingest table and creates a reporting table
        cursor = duckdb.connect()
        print(cursor.execute('SELECT 42').fetchall())
        pass

    create_reporting_table()
  

create_reporting_table()