from uuid import UUID
from typing import Any
from pydantic import BaseModel, Extra, Field, HttpUrl, create_model, root_validator, validator
from pprint import pprint

from arbm_core.private.investors import Fund

from crm.schemas import AllOptional


class FundCreateSchema(BaseModel):
    name: str
    website: HttpUrl | None

    thesis: str | None

    priority: int | None
    enabled: bool = True

    type: str | None
    logo: str | None


class FundIdentitySchema(BaseModel):
    id: int
    uuid: UUID

    class Config:
        orm_mode = True


FundDetailsSchema = create_model(
    'FundDetailsSchema',
    __base__=FundCreateSchema,
    # include parent fields into the schema so that they can be seen by AllOptional metaclass in FundPatchSchema
    **{k: (v, ...) for k, v in FundCreateSchema.__annotations__.items()},
    # load extra fields from mongo
    **{k: (str, None) for k in Fund._FUND_ATTRS},
) # type: ignore


# # init the schema manually so that the id/uuid fields are displayed first
# FundSchema = create_model(
#     'FundSchema',
#     __base__=FundIdentitySchema,
#     id=(int, ...),
#     uuid=(UUID, ...),
#     # copy other fields from FundDetailsSchema, preserving their type and default
#     **{field.name: (field.type_, field.default) for field in FundDetailsSchema.__fields__.values()},
# ) # type: ignore


class FundSchema(FundIdentitySchema, FundDetailsSchema):
    projects_count: int = Field(default=0, alias='signals_count')

    @validator('projects_count', pre=True)
    def validate_signals(cls, v):
        if v:
            return len(v)

    class Config:
        orm_mode = True


class FundPatchSchema(FundDetailsSchema, metaclass=AllOptional):
    pass


class InvestorCreateSchema(BaseModel):
    name: str

    type: str | None
    role: str | None

    funds: list[int]

    twitter_url: str | None
    linkedin_url: str | None


class InvestorSchema(InvestorCreateSchema):
    id: int

    funds: list[FundIdentitySchema]

    class Config:
        orm_mode = True


class InvestorPatchSchema(InvestorCreateSchema, metaclass=AllOptional):
    pass
