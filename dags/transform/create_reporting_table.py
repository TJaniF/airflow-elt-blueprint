from airflow.decorators import dag, task
from pendulum import datetime, duration
import duckdb

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

from include.global_variables import global_variables as gv


default_args = {
    'owner': gv.MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

@aql.transform()
def query_climate_data(
    temp_countries_table: Table,
    country: str
):  
    return """
        SELECT *
        FROM {{temp_countries_table}}
        WHERE Country = {{country}};
    """

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_IN_WEATHER, gv.DS_DUCKDB_IN_CLIMATE],
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["transform", "duckdb"]
)
def create_reporting_table():

    @task 
    def create_country_table(table_name):
        table_name_clean = table_name.replace(" ", "_")
        cursor = duckdb.connect("dwh")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name_clean} (
                dt DATE,
                AverageTemperature DOUBLE,
                AverageTemperatureUncertainty DOUBLE,
                Country VARCHAR
            );"""
        )
        cursor.commit()
        cursor.close()

        return table_name_clean
    
    target_table = create_country_table(gv.MY_COUNTRY)

    tmp_temp_countries_table = query_climate_data(
        temp_countries_table=Table(
            conn_id="duckdb_default",
            name="temp_countries_table"
        ),
        country=f"{gv.MY_COUNTRY}"
    )

    aql.append(
        target_table=Table(conn_id="duckdb_default", name=f"{target_table}"),
        source_table=tmp_temp_countries_table,
        outlets=[gv.DS_DUCKDB_REPORTING]
    )

    aql.cleanup()

    target_table >> tmp_temp_countries_table
  

create_reporting_table()