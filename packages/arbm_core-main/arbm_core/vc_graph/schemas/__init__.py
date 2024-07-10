import abc

from bson import ObjectId
from pydantic import BaseModel


class PyMongoBase(BaseModel, abc.ABC):
    class Config:
        # The ObjectIdField creates an bson ObjectId value, so its necessary to setup the json encoding
        json_encoders = {ObjectId: str}