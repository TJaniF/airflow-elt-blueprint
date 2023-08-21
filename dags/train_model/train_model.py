"""DAG that creates the duckdb pool and kicks off the pipeline by producing to the start dataset."""

from datetime import datetime
from datetime import timedelta

from airflow.decorators import dag

from include.repositories import BackendRepository
from include.repositories import MinioRepository
from include.repositories import TelegramRepository
from include.settings import settings
from include.train_model.tasks import download_new_images

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
    backend_repository = BackendRepository(base_url=settings.BACKEND_URL)
    telegram_repository = TelegramRepository(
        base_url=settings.TELEGRAM_URL,
        token=settings.TELEGRAM_TOKEN,
        chat_id=settings.TELEGRAM_CHAT_ID,
    )
    minio_repository = MinioRepository(
        conn_type=settings.MINIO_CONN_TYPE,
        host=settings.MINIO_HOST,
        login=settings.MINIO_LOGIN,
        password=settings.MINIO_PASSWORD,
    )

    download_new_images(backend_repository, minio_repository)


train_model()
