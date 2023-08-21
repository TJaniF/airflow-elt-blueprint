import io
import logging

from airflow.decorators import task

from include.entities import Image
from include.entities import Material
from include.repositories import BackendRepository
from include.repositories import MinioRepository
from include.settings import settings

LOGGER = logging.getLogger(__name__)


def images_over_threshold(backend_repository: BackendRepository) -> bool:
    images_count = backend_repository.get_new_images_count()
    return images_count > settings.MODEL_RETRAIN_THRESHOLD


@task()
def download_new_images(backend_repository: BackendRepository,
                        minio_repository: MinioRepository,
                        ):
    print('Downloading new Images')
    images = backend_repository.get_new_images()
    materials = backend_repository.get_enabled_materials()
    for image in images:
        material = _get_material(image, materials)
        if material:
            image_data = backend_repository.download_image(image)
            LOGGER.info("Downloaded image. Uploading to MinIO")
            minio_repository.save_images(material=material, image=image, image_data=image_data)
            LOGGER.info(f"Uploaded image {image.filename} to MiniIO")

            # backend_repository.mark_image_as_downloaded(image)


#
# def upload_model(model, model_accuracy: float):
#     print('Uploading Model')
#     current_model = backend_repository.get_latest_model()
#     print(f'current_model: {current_model}')
#     if model_accuracy <= current_model.accuracy:
#         return False
#     print('Model accuracy is larger')
#     current_timestamp = int(time())
#     ml_model = MLModel(timestamp=current_timestamp, accuracy=model_accuracy)
#     print(ml_model)
#     print(ml_model.dict())
#     print('Saving model in Firebase')
#     firebase_repository.upload_model(model)
#     print('Saved model in Firebase')
#     print('Saving model in DB')
#     backend_repository.create_model(ml_model)
#     print('Saved model in DB')
#     return True
#
#
# def send_telegram_notification(model_accuracy: float):
#     print('Sending Telegram Notification')
#     message = f'Se actualizó el modelo. Nueva exactitud: {model_accuracy}'
#     telegram_repository.send_message(message)
#     print('Sent Telegram Notification')
#
#
def _get_material(image: Image, materials: dict[str, Material]):
    if image.material_name in materials:
        return materials[image.material_name]
    for tag in image.tags:
        if tag in materials:
            return materials[tag]
    return None
#
#
# backend_repository = BackendRepository(base_url=settings.BACKEND_URL)
# telegram_repository = TelegramRepository(
#     base_url=settings.TELEGRAM_URL,
#     token=settings.TELEGRAM_TOKEN,
#     chat_id=settings.TELEGRAM_CHAT_ID,
# )
# firebase_repository = FirebaseRepository(
#     firebase_credentials=settings.FIREBASE_CREDENTIALS,
#     firabase_storage_bucket=settings.FIREBASE_STORAGE_BUCKET,
# )
#
# usecases = Usecases(backend_repository, telegram_repository, firebase_repository)
#
# is_over_threshold = usecases.images_over_threshold()
# print(f'is_over_threshold: {is_over_threshold}')
# if is_over_threshold:
#     print('Downloading new images')
#     usecases.download_new_images(images_path=settings.IMAGES_PATH)
#     print('Downloaded new images')
#     print('Training model')
#     model, model_accuracy = train_and_evaluate_model()
#     print(f'Trained model. Accuracy: {model_accuracy}')
#     uploaded_model = usecases.upload_model(model, model_accuracy)
#     if uploaded_model:
#         usecases.send_telegram_notification(model_accuracy)
