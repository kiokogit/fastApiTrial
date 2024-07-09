from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Literal
from uuid import UUID
from pprint import pformat

from loguru import logger
from pydantic import BaseModel, EmailStr, Field, conint, constr, HttpUrl, root_validator, validator

from arbm_core.private.projects import InvestmentStage, ProjectStatus

from crm.schemas import AllOptional, ModifiableGetter
from crm.schemas.entities import FundIdentitySchema, FundSchema
from util import prune_website, validate_linkedin_profile_url


class LeaderCreateSchema(BaseModel):
    name: constr(min_length=3)

    linkedin: HttpUrl | None
    email: EmailStr | None

    role: str | None
    img: HttpUrl | None

    recommended: bool = False

    @root_validator
    def require_contact_for_new_leader(cls, values):
        id, linkedin, email = values.get('id'), values.get('linkedin'), values.get('email')

        if not id and not any([linkedin, email]):
            raise ValueError('at least one contact has to be supplied for a new leader (either email or linkedin)')

        return values

    @validator('linkedin')
    def validate_url(cls, v):
        if v is not None:
            return validate_linkedin_profile_url(v)
        return v


class LeaderUpdateSchema(LeaderCreateSchema, metaclass=AllOptional):
    id: int

    unlink: bool | None  # delete leader


class TagSchema(BaseModel):
    type: str
    value: str

    source: str
    valid_from: date

    @root_validator(pre=True)
    def rv(cls, vals):
        # because for some fn reason Field(alias=...) is not working
        new_vals = {
            'type': vals.get('tag_type'),
            'value': vals.get('tag_name'),
            'source': vals.get('data_source'),
            'valid_from': vals.get('effective_dates'),
            **vals
        }

        return new_vals

    @validator('valid_from', pre=True)
    def validate_effective_dates(cls, v):
        if v is not None:
            return v.lower or v.upper

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


# project updates schemas
class ProjectAnalyticsSchema(BaseModel, metaclass=AllOptional):
    previous_exit: bool | None

    stage: InvestmentStage | None

    funding: conint(ge=0) | None

    last_round: date | None
    last_round_amount: conint(ge=0) | None

    team_size: int | None
    founded: int | None
    location: str | None

    industries: list[TagSchema] | None
    verticals: list[TagSchema] | None

    class Config:
        orm_mode = True
        use_enum_values = True
        getter_dict = ModifiableGetter
        allow_population_by_field_name = True


class ProjectAnalyticsPatchSchema(ProjectAnalyticsSchema):
    @root_validator(pre=True)
    def parse_investment_stage(cls, values):
        if stage_raw := values.get('stage'):
            if stage_raw not in InvestmentStage.__members__:
                stage_raw = stage_raw.strip().lower()
                # logger.debug(f'stage name "{stage_raw}" did not match any ENUM value, trying to parse...')

                if re.match(r'^\W*pre.?seed\W*$', stage_raw):
                    values['stage'] = InvestmentStage.pre_seed
                elif re.match(r'^\W*seed\W*$', stage_raw):
                    values['stage'] = InvestmentStage.seed
                elif re.match(r'^\W*series.?a\W*$', stage_raw):
                    values['stage'] = InvestmentStage.series_a
                elif re.match(r'^\W*series.?b\W*$', stage_raw):
                    values['stage'] = InvestmentStage.series_b
                elif re.match(r'^\W*series.?c\W*$', stage_raw):
                    values['stage'] = InvestmentStage.series_c
                elif re.match(r'^\W*series.?d\W*$', stage_raw):
                    values['stage'] = InvestmentStage.series_d
                elif re.match(r'^\W*ipo\W*$', stage_raw):
                    values['stage'] = InvestmentStage.ipo
                else:
                    logger.error(f"unable to parse {stage_raw} into any allowed investment stage")
                    values['stage'] = None
                    logger.debug(f"stage name parsed into ENUM value: {values['stage']}")

                # logger.debug(f"stage name parsed into ENUM value: {values['stage']}")

        return values

    @root_validator
    def validate_funding(cls, values):
        if (funding := values.get('funding')) and (last_round_amount := values.get('last_round_amount')):
            if last_round_amount > funding:
                raise ValueError(f'Last round qty'
                                f' (${last_round_amount},000)'
                                f' cannot be'
                                f' greater than total Funding'
                                f' (${funding},000)')
        return values

    @validator('founded')
    def validate_founded(cls, v):
        if v is None:
            return v

        present_year = date.today().year
        if not ((start_year := 900) <= v <= present_year):
            raise ValueError(f"founded year must be between between {start_year} and {present_year}")

        return v

    class Config:
        orm_mode = False
        use_enum_values = True
        getter_dict = ModifiableGetter


class ProjectCreateSchema(BaseModel):
    title: constr(min_length=2, strip_whitespace=True)
    website: HttpUrl

    project_type: Literal['competitors', 'startup']

    logo: HttpUrl | None

    analytics: ProjectAnalyticsSchema | None

    @validator('website', pre=True)
    def validate_website(cls, v):
        return prune_website(v)


class LinkedinProfileSchema(BaseModel):
    id: int
    linkedin_id: str | None
    linkedin_url: str | None
    name: str | None
    last_parsed: datetime | None

    class Config:
        orm_mode = True


class ProjectSchema(BaseModel):
    source: Any = Field(None, exclude=True)

    id: int
    uuid: UUID

    title: str
    website: HttpUrl | None

    is_startup: bool | None
    is_b2b: bool | None

    logo: HttpUrl | None
    description: str | None

    discovered_date: datetime | None
    status: ProjectStatus
    status_changed: datetime | None

    analytics: ProjectAnalyticsSchema | None
    linkedin_profile: LinkedinProfileSchema | None

    signals: Any | None
    funds: list[FundSchema] | None

    # interested_funds: Any | None

    class Config:
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


class ProjectPatchSchema(BaseModel, metaclass=AllOptional):
    title: str
    status: ProjectStatus

    website: HttpUrl
    logo: HttpUrl

    description: str
