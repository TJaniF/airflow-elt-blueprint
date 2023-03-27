"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime

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


# run a SQL transformation on the 'in_climate' table in order to create averages
# over different time periods
@aql.transform(pool="duckdb", outlets=[gv.DS_DUCKDB_REPORTING])
def create_global_climate_reporting_table(
    in_climate: Table,
):
    return """
        SELECT CAST(dt AS DATE) AS date, 
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))/10*10) AS decade_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))) AS year_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY MONTH(CAST(dt AS DATE))) AS month_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY CAST(dt AS DATE)) AS day_average_temp,
        FROM {{ in_climate }}
    """


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[gv.DS_DUCKDB_IN_CLIMATE, gv.DS_DUCKDB_IN_WEATHER],
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on climate data in DuckDB.",
    tags=["duckdb", "transform", "Astro SDK"],
)
def create_reporting_table():

    # input the raw climate data and save the outcome of the transformation to a
    # permanent reporting table
    create_global_climate_reporting_table(
        in_climate=Table(name=gv.CLIMATE_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
        output_table=Table(name=gv.REPORTING_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
    )


create_reporting_table()
