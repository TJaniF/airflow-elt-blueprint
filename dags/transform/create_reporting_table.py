"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import duckdb

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# ----------------- #
# Astro SDK Queries #
# ----------------- #


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

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[gv.DS_DUCKDB_IN_WEATHER, gv.DS_DUCKDB_IN_CLIMATE],
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on data in DuckDB using the Astro SDK.",
    tags=["transform", "duckdb"]
)
def create_reporting_table():

    @task
    def create_country_table(table_name):
        """Creates a new table for the country provided."""

        table_name_clean = table_name.replace(" ", "_")
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
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

    # run the query in query_climate_data on the country climate table
    tmp_temp_countries_table = query_climate_data(
        temp_countries_table=Table(
            conn_id="duckdb_default",
            name=gv.COUNTRY_CLIMATE_TABLE_NAME
        ),
        country=f"{gv.MY_COUNTRY}"
    )

    # append the result from the above query to the existing country table
    aql.append(
        target_table=Table(conn_id="duckdb_default", name=f"{target_table}"),
        source_table=tmp_temp_countries_table,
        outlets=[gv.DS_DUCKDB_REPORTING]
    )

    # clean up temporary tables created
    aql.cleanup()

    # set dependencies
    target_table >> tmp_temp_countries_table


create_reporting_table()
