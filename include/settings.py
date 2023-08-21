from pydantic import BaseSettings


class Settings(BaseSettings):
    BACKEND_URL: str
    TELEGRAM_URL: str
    MATERIAL_CREATION_THRESHOLD: int
    MODEL_RETRAIN_THRESHOLD: int
    TELEGRAM_CHAT_ID: int
    TELEGRAM_TOKEN: str
    IMAGES_PATH: str
    TRAINED_MODEL_PATH: str
    BATCH_SIZE: int
    IMG_HEIGHT: int
    IMG_WIDTH: int
    VALIDATION_SPLIT: float
    SEED: int
    FINE_TUNE_AT: int
    INITIAL_EPOCHS: int
    FINE_TUNE_EPOCHS: int
    LEARNING_RATE: float
    FIREBASE_CREDENTIALS: str
    FIREBASE_STORAGE_BUCKET: str

    MINIO_CONN_TYPE: str
    MINIO_HOST: str
    MINIO_LOGIN: str
    MINIO_PASSWORD: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
