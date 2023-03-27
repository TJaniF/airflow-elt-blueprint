"""DAG that creates the duckdb pool and kicks off the pipeline by producing to the start dataset."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # after being unpaused this DAG will run once, afterwards it can be run
    # manually with the play button in the Airflow UI
    schedule="@once",
    catchup=False,
    default_args=gv.default_args,
    description="Run this DAG to kick off the pipeline!",
    tags=["start"],
)
def start():

    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'",
        outlets=[gv.DS_START],
    )


# when using the @dag decorator, the decorated function needs to be
# called after the function definition
start()
