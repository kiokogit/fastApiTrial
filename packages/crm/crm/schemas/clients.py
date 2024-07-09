from datetime import datetime
from uuid import UUID

from loguru import logger
from pydantic import BaseModel, EmailStr, constr, validator
from pytz import common_timezones_set

from arbm_core.public.users import MembershipPlan

from ..schemas import AllOptional

from util import Weekday


class ClientUserCreateSchema(BaseModel):
    organization_id: str

    username: constr(min_length=1, max_length=64, strip_whitespace=True, regex=r'^[a-zA-Z0-9_]+$')
    email: EmailStr

    firstname: constr(min_length=1, max_length=64, strip_whitespace=True, regex=r'^[a-zA-Z\- 0-9]+$')
    lastname: constr(min_length=1, max_length=64, strip_whitespace=True, regex=r'^[a-zA-Z\- 0-9]+$')


class ClientUserSchema(ClientUserCreateSchema):
    active: bool
    hashed_password: str | None
    email: EmailStr | None

    class Config:
        orm_mode = True


class PatchOrgSchema(BaseModel, metaclass=AllOptional):
    timezone: constr(strip_whitespace=True) | None

    # report_day: Weekday | None

    @validator('timezone', pre=True)
    def validate_timezone(cls, v):
        if not v:
            return None

        if v not in common_timezones_set:
            raise ValueError(f'Invalid timezone: {v}')

        return v

    # @validator('report_day', pre=True)
    # def validate_report_day(cls, v):
    #     if not v:
    #         return None

    #     if v not in Weekday:
    #         raise ValueError(f'Invalid report day: {v}')

    #     return v


class OrgCreateSchema(BaseModel):
    name: constr(min_length=3, max_length=64, strip_whitespace=True)

    timezone: constr(strip_whitespace=True) | None
    summary_day: Weekday | None

    @validator('summary_day', pre=True)
    def validate_summary_day(cls, v):
        if not v:
            return None
            v = v.lower().strip()

        match v:
            case 'monday':
                return 1
            case 'tuesday':
                return 2
            case 'wednesday':
                return 3
            case 'thursday':
                return 4
            case 'friday':
                return 5
            case 'saturday':
                return 6
            case 'sunday':
                return 7


    @validator('timezone', pre=True)
    def validate_timezone(cls, v):
        if not v:
            return None

        if v not in common_timezones_set:
            raise ValueError(f'Invalid timezone: {v}')

        return v


class OrgSchema(OrgCreateSchema):
    signup_date: datetime | None
    membership: MembershipPlan

    users: list[ClientUserSchema]
    all_users: list[ClientUserSchema]

    funds_portfolio: list[UUID]

    reports: list[str]

    @validator('funds_portfolio', pre=True, each_item=True)
    def funds_to_uuids(cls, v):
        if v:
            return v.uuid

    @validator('reports', pre=True, each_item=True)
    def reports_to_uuids(cls, v):
        if v:
            return str(v.uuid)

    @validator('reports' )
    def reports_any(cls, v):
        if not v:
            return []

    class Config:
        orm_mode = True


class ProjectUserSchema(BaseModel):
    username: str
    project_uuid: str

    time_recommended: datetime

    revoked: bool

    favourite: bool
    rating: int | None
    feedback: str | None
    feedback_posted: datetime | None


    class Config:
        orm_mode = True
