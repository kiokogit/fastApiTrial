from pydantic import BaseModel, HttpUrl, constr, validator

from crm.schemas import AllOptional
from crm.schemas.projects import ProjectAnalyticsPatchSchema
from util import prune_website


class ProjectData(BaseModel, metaclass=AllOptional):
    title: constr(min_length=2, strip_whitespace=True)

    description: str
    website: HttpUrl
    logo: HttpUrl

    crunchbase_url: HttpUrl

    analytics: ProjectAnalyticsPatchSchema

    @validator('website', pre=True)
    def validate_website(cls, v):
        return prune_website(v).lower()