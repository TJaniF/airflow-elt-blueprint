import duckdb
from airflow.decorators import dag, task 
from pendulum import datetime

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
)
def test_dag():

    @task
    def duckdb_testing():
        cursor = duckdb.connect()
        print(cursor.execute('SELECT 42').fetchall())

    duckdb_testing()


test_dag()