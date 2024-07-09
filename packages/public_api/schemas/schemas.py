import datetime
import enum
from typing import Any, Literal, Optional

import pydantic
from pydantic import BaseModel, EmailStr, Field, constr, conint, root_validator, validator

from arbm_core.private.projects import InvestmentStage
from pydantic.utils import GetterDict


class ModifiableGetter(GetterDict):
    """
    Custom GetterDict subclass allowing to modify values
    """
    def __setitem__(self, key: str, value: Any) -> Any:
        return setattr(self._obj, key, value)


class AllOptional(pydantic.main.ModelMetaclass):
    def __new__(self, name, bases, namespaces, **kwargs):
        annotations = namespaces.get('__annotations__', {})
        for base in bases:
            annotations.update(base.__annotations__)
        for field in annotations:
            if not field.startswith('__'):
                annotations[field] = Optional[annotations[field]]
        namespaces['__annotations__'] = annotations
        return super().__new__(self, name, bases, namespaces, **kwargs)


class TerminalRequest(BaseModel):
    purpose: Literal['terminal_request']

    phone: str | None
    inquiry: str

    # full_name: constr(min_length=3, max_length=128) | None
    # company: constr(min_length=3, max_length=128) | None


class DatasetRequest(BaseModel):
    purpose: Literal['dataset_request']

    phone: constr(min_length=3, max_length=32)
    role: constr(min_length=3)
    how_found: constr(min_length=3)


class AccessRequest(BaseModel):
    email: EmailStr
    request: TerminalRequest | DatasetRequest = Field(..., discriminator='purpose')


class ProductType(str, enum.Enum):
    software = 'Pure Software'
    hardware = 'Hardware & Software'


class BusinessModel(str, enum.Enum):
    b2b = 'B2B (Business-to-Business)'
    b2c = 'B2C (Business-to-Consumer)'
    c2c = 'C2C (Consumer-to-Consumer)'


class SearchFiltersSchema(BaseModel):
    search_mode: str

    # TAG FILTERS
    # keyword filters
    keywords: list[str] | None
    verticals: list[str] | None
    customer_segments: list[str] | None
    competing_spaces: list[str] | None
    origin_country: str | None

    # enum filters
    product_types: list[ProductType] | None
    business_models: list[BusinessModel] | None

    # ANALYTICS FILTERS
    funding_stage: list[InvestmentStage] | None

    founded_from: conint(gt=1900, le=datetime.date.today().year) | None
    founded_to: conint(gt=1900, le=datetime.date.today().year) | None

    @root_validator
    def require_any_filter(cls, values):
        if not any(values.keys()):
            raise ValueError('at least one filter must be supplied')
        return values

    @validator('founded_to')
    def validate_founded_to(cls, value, values, **kwargs):
        founded_from = values.get('founded_from', 1900)

        if value and not value >= founded_from:
            raise ValueError('founded_to must be >= founded_from')
        return value
