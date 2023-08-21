import json
import logging
from collections.abc import Iterator

import firebase_admin
import httpx
from firebase_admin import credentials
from firebase_admin import ml
from tensorflow import keras

from include.entities import Image
from include.entities import ImagesCountResponse
from include.entities import Material
from include.entities import MLModel

LOGGER = logging.getLogger(__name__)


class BackendRepository:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_all_images(self) -> list[Image]:
        with httpx.Client() as client:
            url = f'{self.base_url}/images/'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return [Image(**item) for item in data]
            else:
                response.raise_for_status()

    def get_new_images(self) -> list[Image]:
        with httpx.Client() as client:
            url = f'{self.base_url}/images/?downloaded=false'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return [Image(**item) for item in data]
            else:
                response.raise_for_status()

    def get_all_materials(self) -> list[Material]:
        with httpx.Client() as client:
            url = f'{self.base_url}/materials/'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return [Material(**item) for item in data]
            else:
                response.raise_for_status()

    def get_enabled_materials(self) -> dict[str, Material]:
        with httpx.Client() as client:
            url = f'{self.base_url}/materials/?enabled=true'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return {item['name']: Material(**item) for item in data}
            else:
                response.raise_for_status()

    def get_latest_material(self) -> Material:
        with httpx.Client() as client:
            url = f'{self.base_url}/materials/latest/'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return Material(**data)
            else:
                response.raise_for_status()

    def create_material(self, material: Material) -> Material:
        with httpx.Client() as client:
            url = f'{self.base_url}/materials/'
            response = client.post(url, json=material.dict())
            if response.status_code == 201:
                data = response.json()
                return Material(**data)
            else:
                response.raise_for_status()

    def get_new_images_count(self) -> int:
        with httpx.Client() as client:
            url = f'{self.base_url}/images/count/?downloaded=false'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                images_count = ImagesCountResponse(**data)
                return images_count.count
            else:
                response.raise_for_status()

    def download_image(self, image: Image) -> Iterator[bytes]:
        with httpx.Client() as client:
            response = client.get(f'{self.base_url}/images/file/{image.filename}/')
            if response.status_code == 200:
                return response.iter_bytes()
            else:
                response.raise_for_status()

    def mark_image_as_downloaded(self, image: Image) -> Image:
        downloaded_image = Image(downloaded=True)
        with httpx.Client() as client:
            response = client.patch(
                f'{self.base_url}/images/{image.id}/', json=downloaded_image.dict()
            )
            if response.status_code == 200:
                data = response.json()
                return Image(**data)
            else:
                response.raise_for_status()

    def get_latest_model(self):
        with httpx.Client() as client:
            url = f'{self.base_url}/models/latest/'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return MLModel(**data)
            else:
                response.raise_for_status()

    def create_model(self, model: MLModel):
        with httpx.Client() as client:
            url = f'{self.base_url}/models/'
            response = client.post(url, json=model.dict())
            if response.status_code == 201:
                data = response.json()
                return MLModel(**data)
            else:
                response.raise_for_status()


class TelegramRepository:
    def __init__(self, base_url: str, token: str, chat_id: int):
        self.base_url = base_url
        self.token = token
        self.chat_id = chat_id

    def send_message(self, message: str) -> dict:
        with httpx.Client() as client:
            url = f'{self.base_url}{self.token}/sendMessage?chat_id={self.chat_id}&text={message}'
            response = client.get(url)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                response.raise_for_status()


class FirebaseRepository:
    MODEL_ID = '21073965'

    def __init__(self, firebase_credentials: str, firabase_storage_bucket: str):
        cred = credentials.Certificate({**json.loads(firebase_credentials)})
        bucket = {**json.loads(firabase_storage_bucket)}
        firebase_admin.initialize_app(cred, bucket)

    def upload_model(self, model: keras.Model):
        existing_model = ml.get_model(model_id=self.MODEL_ID)
        existing_model.model_format = ml.TFLiteFormat(
            model_source=ml.TFLiteGCSModelSource.from_keras_model(model)
        )
        ml.update_model(existing_model)
