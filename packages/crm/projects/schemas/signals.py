from abc import ABC
import datetime
from typing import Literal

from loguru import logger
from loguru import logger
from pydantic import BaseModel, HttpUrl, constr, validator

from projects.schemas.linkedin import ProjectLinkedinDetailsSchema
from util import validate_linkedin_url


class InvestingEntitySchema(BaseModel, ABC):
    entity_type: Literal['fund', 'investor']


class FundIdSchema(InvestingEntitySchema):
    entity_type: Literal['fund'] = 'fund'
    id: int


class InvestorIdSchema(InvestingEntitySchema):
    entity_type: Literal['investor'] = 'investor'
    id: int


class BaseSignalSchema(BaseModel, ABC):
    signal_type: None
    investing_entity: InvestingEntitySchema

    picked_up_date: datetime.datetime
    # estimated_date: datetime.datetime

    @validator('picked_up_date', pre=True)
    def collate_date(cls, v):
        if isinstance(v, datetime.date):
            v = datetime.datetime.combine(v, datetime.datetime.min.time())
        if isinstance(v, str):
            try:
                v = datetime.datetime.fromisoformat(v)
            except ValueError as e:
                logger.error(f'unable to parse date into datetime from ISO format: {v}')
        return v


class DirectSignal(BaseSignalSchema):
    signal_type: Literal['direct'] = 'direct'

    investing_entity: FundIdSchema
    signal_source: Literal['twitter', 'linkedin']

    count: int = 1


class LinkedinPostSignal(BaseSignalSchema):
    signal_type: Literal['linkedin'] = 'linkedin'

    investing_entity: InvestorIdSchema
    post_id: int
    leader_id: int


class ProjectSourceSchema(ABC, BaseModel):
    signal: None


class LinkedinSourceSchema(ProjectSourceSchema):
    signal: LinkedinPostSignal | None

    company_url: HttpUrl  # linkedin_url (previously)
    linkedin_details: ProjectLinkedinDetailsSchema

    @validator('company_url')
    def validate_url(cls, v):
        v = validate_linkedin_url(v)
        return v
