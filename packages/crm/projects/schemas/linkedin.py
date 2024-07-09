import re
from datetime import date
from typing import Any
from pprint import pprint

from loguru import logger

from pydantic import BaseModel, Field, HttpUrl, Json, validator, root_validator

from util import clean_url, validate_linkedin_url


def parse_list(v):
    """
    If value is a comma-delimited string, split and return resulting list
    Otherwise, return value
    :param v:
    :return:
    """
    if isinstance(v, str):
        return [speciality.strip() for speciality in v.split(',')]
    return v


class ProjectLinkedinDetailsSchema(BaseModel):
    # this is unique identifier for the Linkedin Project entity in db
    linkedin_url: HttpUrl
    linkedin_id: int

    # main info
    title: str
    about: str | None
    tagline: str | None

    website: str | None
    logo: HttpUrl | None

    crunchbase_url: str | None

    # categories
    category: str | None
    industry: str | None
    specialities: str | None

    # location & founded
    headquarters: str | None
    location: str | None
    founded_year: int | None

    # employees
    company_size: str | None
    company_size_linkedin: str | None

    # financials
    stage: str | None

    last_round_date: str | None
    latest_funding: int | None

    raw_data: Json[Any] | None

    class Config:
        anystr_strip_whitespace = True

    @root_validator(pre=True)
    def get_team_size(cls, values):
        if 'company_size_linkedin' not in values:
            if 'company_size' in values:
                size_range = re.search(r'^(\d{1-6})-(\d{1-6})', values['company_size'])
                if size_range:
                    approx_team_size = (int(size_range.group(1)) + int(size_range.group(2))) / 2
                    values['company_size_linkedin'] = approx_team_size
        return values

    @validator('crunchbase_url', pre=True)
    def validate_crunchbase_url(cls, v):
        if v:
            return clean_url(v)
        return v


    @validator('linkedin_url', pre=True)
    def validate_url(cls, v):
        v = validate_linkedin_url(v)
        return v


    @validator('headquarters', pre=True)
    def validate_headqurater(cls, v):
        if isinstance(v, dict):
            return ", ".join([
                item for item
                in [v.get('city', '') or v.get('geographic_area', ''), v.get('country', '') or v.get('Country Code', '')]
                if item
            ])
        return v


    @validator('specialities', pre=True)
    def validate_specialities(cls, v):
        if isinstance(v, list):
            return ', '.join(v)
        if isinstance(v, str):
            v = re.sub(r',?\s+and\s+', ', ', v)
        return v


    @validator('company_size', pre=True)
    def validate_company_size(cls, v):
        if isinstance(v, dict):
            return f"{v['start']}-{v['end']} employees"
        return v


    @validator('last_round_date', pre=True)
    def validate_last_round(cls, v):
        if isinstance(v, dict):
            last_round_date = date(year=v['year'], month=v['month'], day=v['day'])
            return last_round_date.strftime("%Y-%m-%d")
        return v

    @validator('latest_funding', pre=True)
    def parse_latest_funding(cls, v):
        if isinstance(v, str):
            v = v.lower().strip()
            # logger.critical(f"{v}")
            match = re.search(r'((?:$|us|eur|usd)\s*)?(\d+\.?\d*)\s*(k|mn?|bn?)?', v, re.IGNORECASE)

            # logger.critical(match)
            if not match:
                raise ValueError(f'Invalid funding amount: {v}')

            str_amount = match.group(2)
            str_amount = str_amount.split('.')[0]
            str_amount = str_amount.replace(',', '')

            if not str_amount.isnumeric():
                raise ValueError(f'Invalid funding amount even after parsing: "{str_amount}"')

            amount = int(str_amount)

            units = match.group(3)
            scale = 1

            match units:
                case 'k' | 'thousand' | 'thousands':
                    scale = 10**3
                case 'm' | 'mn':
                    scale = 10**6
                case 'b' | 'bn':
                    scale = 10**9
            # logger.critical(f'amount: {amount}, scale: {scale}')
            # logger.critical(f'parsed funding amount: {amount * scale}')

            return amount * scale

        return v


class Urls(BaseModel):
    company_page: str | None
    linkedin_url: str | None


class Industry(BaseModel):
    id: int
    name: str


class Images(BaseModel):
    logo: str | None | None
    cover: str | None | None


class Founded(BaseModel):
    month: int | None
    day: int | None
    year: int


class Staff(BaseModel):
    start: int
    end: int


class CallToAction(BaseModel):
    url: str | None
    text: str | None


class Address(BaseModel):
    country: str | None
    geographic_area: str | None
    city: str | None
    postal_code: str | None
    line1: str | None
    line2: str | None


class OtherItem(BaseModel):
    description: str | None
    address: Address | None
    is_headquarter: bool | None


class Locations(BaseModel):
    headquarter: Address | None
    other: list[OtherItem] | None


class MoneyRaised(BaseModel):
    currency: str
    amount: str


class AnnouncedOn(BaseModel):
    month: int
    day: int
    year: int


class LeadInvestor(BaseModel):
    name: str
    logo: str
    investor_url: str


class LastFundingRound(BaseModel):
    funding_type: str
    money_raised: MoneyRaised
    announced_on: AnnouncedOn
    lead_investors: list[LeadInvestor]
    num_of_other_investors: int
    round_url: str
    investors_url: str


class FundingData(BaseModel):
    num_of_funding_rounds: int
    last_funding_round: LastFundingRound
    organization_url: str
    funding_rounds_url: str


class Urls1(BaseModel):
    company_page: str
    linkedin_url: str


class Industry1(BaseModel):
    id: int
    name: str


class Images1(BaseModel):
    logo: str
    cover: str


class RelatedCompany(BaseModel):
    name: str
    universal_name: str
    company_id: int
    description: str
    phone: str
    followers: int
    urls: Urls1
    industries: list[Industry1]
    images: Images1


class CompanyDetailsSchema(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str = Field(alias='CompanyName')
    universal_name: str
    company_id: int = Field(alias='Id')

    description: str | None = Field(alias='Description')
    urls: Urls | None

    funding_data: FundingData | None

    specialities: list[str] | None = Field(alias='Specialities')
    industries: list[Industry] | None = Field(alias='Industries')
    type: str | None
    hashtags: list[str] | None

    founded: Founded | None = Field(alias='Founded')
    locations: Locations | None
    staff: Staff | None = Field(alias='CompanySize')

    followers: int | None
    call_to_action: CallToAction | None
    phone: str | None
    related_companies: list[RelatedCompany] | None

    images: Images | None

    @validator('specialities', pre=True)
    def parse_specialities(cls, v):
        return parse_list(v)

    @validator('industries', pre=True)
    def parse_industries(cls, v):
        return parse_list(v)

    @validator('founded', pre=True)
    def parse_founded(cls, v):
        if isinstance(v, str):
            if v.isnumeric():
                return Founded(year=int(v))
            return None
        return v

    @validator('staff', pre=True)
    def parse_staff(cls, v):
        if isinstance(v, str):
            range = re.search(r'(\d+)-(\d+)')
            print(range)
            return None
        return v
