"""DAG that runs a streamlit app."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_REPORTING],
    catchup=False,
    default_args=gv.default_args,
    # this DAG will time out after one hour
    dagrun_timeout=duration(hours=1),
    description="Runs a streamlit app.",
    tags=["reporting", "streamlit"]
)
def run_streamlit_app():

    # run the streamlit app contained in the include folder
    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        # retrieve the command from the global variables file
        bash_command=gv.STREAMLIT_COMMAND,
        # provide the directory to run the bash command in
        cwd="include/streamlit_app",
        # add environment variables
        env={
            # city coordinates are retrieved from an Airflow Variable
            "city_coordinates": "{{ var.value.city_coordinates }}",
            # additional variables are retrieved from a local module
            "my_city": gv.MY_CITY,
            "my_name": gv.MY_NAME
        }
    )

    run_streamlit_script


run_streamlit_app()
