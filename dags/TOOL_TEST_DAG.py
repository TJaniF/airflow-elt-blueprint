"""DAG to test MinIO, DuckDB, Streamlit."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import duckdb
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.bash import BashOperator

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    description="Use to test MinIO, DuckDB, Streamlit.",
    tags=["minio", "duckdb", "streamlit"],
    catchup=False
)
def TOOL_TEST_DAG():

    @task
    def minio_testing():
        """Log a list of all buckets in the MinIO instance."""
        client = gv.get_minio_client()
        buckets = client.list_buckets()
        existing_bucket_names = [bucket.name for bucket in buckets]
        gv.task_log.info(
            f"MinIO contains the following buckets: {existing_bucket_names}"
        )

        return existing_bucket_names

    @task
    def duckdb_testing():
        """Log a list of all tables in the DuckDB instance."""
        db_name = gv.DUCKDB_INSTANCE_NAME
        cursor = duckdb.connect(db_name)
        table_names = cursor.execute('SHOW TABLES;').fetchall()
        gv.task_log.info(
            f"DuckDB {db_name} contains the following tables: {table_names}"
        )

        return table_names

    # run a self-contained streamlit app
    run_streamlit_test = BashOperator(
        task_id="run_streamlit_test",
        bash_command="streamlit run streamlit_test.py",
        cwd="include/tool_testing"
    )

    minio_testing() >> duckdb_testing() >> run_streamlit_test


TOOL_TEST_DAG()
