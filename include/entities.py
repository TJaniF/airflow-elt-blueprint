import logging
from typing import Union

from pydantic import BaseModel

LOGGER = logging.getLogger(__name__)


class Image(BaseModel):
    id: Union[str, None] = None
    filename: Union[str, None] = None
    material_name: Union[str, None] = None
    downloaded: Union[bool, None] = None
    tags: Union[list[str], None] = None


class Material(BaseModel):
    name: str
    order: int
    enabled: bool


class ImagesCountResponse(BaseModel):
    count: int


class MLModel(BaseModel):
    timestamp: int
    accuracy: float
