from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration

from include.global_variables import global_variables as gv


default_args = {
    'owner': gv.MY_NAME,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': duration(minutes=5)
}

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_REPORTING],
    catchup=False,
    default_args=default_args,
    description="ETL pattern",
    tags=["reporting", "streamlit"]
)
def run_streamlit_app():

    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        bash_command="streamlit run weather_v_climate_app.py &",
        cwd="include/streamlit_app"
    )

    run_streamlit_script
  

run_streamlit_app()