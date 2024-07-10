from abc import ABC
import uuid
from bson.objectid import ObjectId as BsonObjectId
from pydantic import BaseModel, Field

class PydanticObjectId(BsonObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        # handle request -> MongoDb case
        if isinstance(v, str) and BsonObjectId.is_valid(v):
            return BsonObjectId(v)

        # handle MongoDb -> response case
        if isinstance(v, BsonObjectId):
            return str(v)

        raise TypeError('ObjectId required')

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string", example="64e9dfd8e7fbd8ba02247477")


class PymongoBaseModel(BaseModel, ABC):
    id: PydanticObjectId = Field(default_factory=uuid.uuid4, alias="_id")

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        json_encoders = {PydanticObjectId: str}