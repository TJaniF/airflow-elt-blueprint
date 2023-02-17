"""DAG that kicks off the pipeline by producing to the start dataset."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
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
    tags=["start"]
)
def start():

    # empty task which produces to the "start" Dataset to kick off the pipeline
    start_task = EmptyOperator(
        task_id="start",
        outlets=[gv.DS_START]
    )

    start_task


# when using the @dag decorator, the decorated function needs to be
# called after the function definition
start()
