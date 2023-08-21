"""DAG that creates the duckdb pool and kicks off the pipeline by producing to the start dataset."""

from datetime import datetime
from datetime import timedelta

from airflow.decorators import dag
from airflow.decorators import task

default_args = {
    'owner': 'Santiago Gandolfo',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    tags=['train_model', 'cnn', 'pipeline', 'cnn-pipeline'],
    schedule_interval='0 6 * * *',
    catchup=False,
    max_active_runs=1,
)
def train_model():
    @task
    def test_1():
        print("Hello test_1")

    @task
    def test_2():
        print("Hello test_1")

    test_1() >> test_2()


train_model()
