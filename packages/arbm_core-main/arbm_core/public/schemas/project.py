from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, HttpUrl, validator
from pydantic.color import Color

from .signals import FundSchema


def format_thousands(qty: str):
    thousands = (int(qty) * 1000)
    formatted = f'${thousands:,}'
    return formatted


def date_human(fmt_date: date | str):
    if isinstance(fmt_date, str):
        fmt_date = datetime.strptime(fmt_date, "%Y-%m-%d")

    if fmt_date.year == date.today().year:
        return fmt_date.strftime("%d %B")
    else:
        return fmt_date.strftime("%d %B %Y")


PROJECT_TAGS = {
    'location': {
    },
    'stage': {
    },
    'founded': {
    },
    'team_size': {
        'title': 'team size',
    },
    'funding': {
        'formatting': format_thousands,
    },
    'last_round': {
        'title': 'last round',
        'formatting': date_human,
    },
    'last_round_amount': {
        'title': 'last round amount',
        'formatting': format_thousands,
    },
}

SOCIAL_TAGS = {
    'twitter_url':{
        'icon': 'twitter',
    },
    'linkedin_url': {
        'icon': 'linkedin',
    },
    'pitchbook_url': {
        'icon': 'pitchbook',
    },
    'crunchbase_url': {
        'icon': 'crunchbase',
    },
}


class DetailsSchema(BaseModel):
    pass


class FinancialDetailsSchema(BaseModel):
    valuation: int | None


class SocialSchema(BaseModel):
    title: str
    url: str

    icon: str | None

    class Config:
        orm_mode = True


class TagSchema(BaseModel):
    title: str
    content: str

    icon: str | None

    row: int | None
    ordering: int | None

    colour: Color | None

    class Config:
        orm_mode = True


class ProjectSchema(BaseModel):
    # technical params
    uuid: UUID
    discovered_date: date | None
    time_published: date | None

    #main details
    title: str
    about: str | None
    markdown_description: str | None
    verticals: list[str] | None = []
    keywords: list[str] | None = []

    # funds information
    funds: list[FundSchema] | None = []
    investor_interest: str | None

    # project details
    logo: HttpUrl | None
    website: HttpUrl | None

    tags: list[TagSchema] | None = []

    # misc details
    socials: list[SocialSchema] = []
    financials: FinancialDetailsSchema | None

    class Config:
        orm_mode = True

    @validator("tags")
    def format_tags(cls, tag_values):
        tags = []
        for tag in tag_values:
            if tag_config := PROJECT_TAGS.get(tag.title):
                tag.title = tag_config.get('title') or tag.title

                if fmt := tag_config.get('formatting'):
                    tag.content = fmt(tag.content)

            tags.append(tag)

        return tags
