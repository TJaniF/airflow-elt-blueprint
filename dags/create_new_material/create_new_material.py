"""DAG that creates the duckdb pool and kicks off the pipeline by producing to the start dataset."""

from datetime import datetime
from datetime import timedelta

from airflow.decorators import dag

from include.create_new_material.tasks import create_new_materials
from include.create_new_material.tasks import created_new_material
from include.create_new_material.tasks import get_potential_tags
from include.create_new_material.tasks import send_telegram_notification
from include.repositories import BackendRepository
from include.repositories import TelegramRepository
from include.settings import settings

default_args = {
    'owner': 'Santiago Gandolfo',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
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

    tags = get_potential_tags(backend_repository)
    new_materials = create_new_materials(
        backend_repository, tags, threshold=settings.MATERIAL_CREATION_THRESHOLD
    )
    created_new_material(new_materials) >> send_telegram_notification(telegram_repository,
                                                                      new_materials)


create_new_material()
