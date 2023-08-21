"""DAG that creates the duckdb pool and kicks off the pipeline by producing to the start dataset."""

from datetime import datetime
from datetime import timedelta

from airflow.decorators import dag
from airflow.decorators import task

from include.repositories import BackendRepository
from include.repositories import TelegramRepository
from include.usecases import Usecases
from include.settings import settings

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
    tags=['create_new_material', 'cnn', 'pipeline', 'cnn-pipeline'],
    schedule_interval='0 6 * * *',
    catchup=False,
    max_active_runs=1,
)
def create_new_material():
    backend_repository = BackendRepository(base_url=settings.BACKEND_URL)
    telegram_repository = TelegramRepository(
        base_url=settings.TELEGRAM_URL,
        token=settings.TELEGRAM_TOKEN,
        chat_id=settings.TELEGRAM_CHAT_ID,
    )
    usecases = Usecases(backend_repository, telegram_repository)
    @task
    def test_1():
        final_tags = usecases.get_potential_tags()
        new_materials = usecases.create_new_materials(
            final_tags, threshold=settings.MATERIAL_CREATION_THRESHOLD
        )
        usecases.send_telegram_notification(new_materials)

    test_1()

create_new_material()

