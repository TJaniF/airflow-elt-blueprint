from airflow.decorators import dag 
from pendulum import datetime, duration

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

default_args = {
    'owner': "me",
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

@aql.transform()
def create_country_table(
    table: Table
):  
    return """
        SELECT 5;
    """

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["transform", "duckdb"]
)
def repro_example():

    tmp_temp_countries_table = create_country_table(table=Table(conn_id="duckdb_default"))

repro_example()