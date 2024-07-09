import uuid

from pydantic import BaseModel, Field, validator
from bson.objectid import ObjectId as BsonObjectId

from crm.schemas import AllOptional


from pydantic.json import ENCODERS_BY_TYPE


# ENCODERS_BY_TYPE |= {BsonObjectId: str}


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


class EntryCreateSchema(BaseModel):
    value: str
    enabled: bool = True


class EntrySchema(BaseModel):
    id: PydanticObjectId = Field(default_factory=uuid.uuid4, alias="_id")

    value: str
    enabled: bool

    class Config:
        json_encoders = {BsonObjectId: str}
        allow_population_by_field_name = True


class EntryPatchSchema(EntrySchema, metaclass=AllOptional):
    pass


class BaseCollectionSchema(BaseModel):
    name: str


class CollectionSchema(BaseCollectionSchema):
    items: list[EntrySchema]


class CollectionPatchSchema(BaseModel):
    patch_items: list[EntryPatchSchema] | None = None
    delete_items: list[PydanticObjectId] | None = None
