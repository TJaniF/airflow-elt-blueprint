# package imports
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

# local module imports
from include.global_variables import global_variables as gv

@dag(
    start_date=datetime(2023, 1, 1),
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

start()