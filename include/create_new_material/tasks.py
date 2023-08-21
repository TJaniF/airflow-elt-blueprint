import logging
from collections import defaultdict

from airflow.decorators import task

from include.entities import Material
from include.repositories import BackendRepository
from include.repositories import TelegramRepository

LOGGER = logging.getLogger(__name__)


@task()
def get_potential_tags(backend_repository: BackendRepository) -> dict[str, int]:
    print('Getting potential tags')
    images = backend_repository.get_all_images()
    materials = backend_repository.get_all_materials()
    materials_names = {material.name for material in materials}
    tags = defaultdict(lambda: 0)
    for image in images:
        if image.tags:
            for tag in image.tags:
                if tag not in materials_names:
                    tags[tag] += 1
    return tags


@task()
def create_new_materials(backend_repository: BackendRepository, tags: dict[str, int],
                         threshold: int) -> list[dict]:
    LOGGER.info('Creating new Materials')
    LOGGER.info(tags)
    new_materials = []
    latest_material = backend_repository.get_latest_material()
    latest_order = latest_material.order
    for tag, count in tags.items():
        if count >= threshold:
            LOGGER.info(f"Creating mateirl: {tag}")
            latest_order += 1
            material = Material(name=tag, order=latest_order, enabled=False)
            created_material = backend_repository.create_material(material)
            LOGGER.info(f"Created mateirl: {tag}")
            new_materials.append(created_material.dict())
    LOGGER.info(f'New materials: {new_materials}')
    return new_materials


@task.short_circuit
def created_new_material(new_materials):
    return new_materials is not None and len(new_materials) > 0


@task()
def send_telegram_notification(telegram_repository: TelegramRepository,
                               new_materials):
    LOGGER.info('Sending Telegram notification')
    materials = [Material(**material) for material in new_materials]
    formatted_new_materials = ', '.join(
        ['- ' + material.name for material in materials]
    )
    message = f'Se crearon los siguientes materiales: {formatted_new_materials}'
    LOGGER.info(message)
    telegram_repository.send_message(message)
    LOGGER.info('Sent Telegram notification')
