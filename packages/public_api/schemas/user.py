from datetime import datetime

from pydantic import BaseModel, root_validator

from .schemas import AllOptional, ModifiableGetter
from schemas.funds import FundSchema


class OrganizationSchema(BaseModel):
    name: str
    timezone: str

    summary_day: str | None

    funds_portfolio: list[FundSchema] | None = None
    allowed_pages: list[str] | None = []

    class Config:
        orm_mode = True


class User(BaseModel):
    username: str
    email: str | None = None
    active: bool | None = None

    organization_id: str
    organization: OrganizationSchema

    pipeline_funds: list[FundSchema] | None

    class Config:
        orm_mode = True


class CreateAutoList(BaseModel):
    name: str
    prompt: str
    active: bool = False


class PatchAutoList(CreateAutoList, metaclass=AllOptional):
    pass


class AutoList(CreateAutoList):
    id: int

    projects_count: int
    projects: list

    author: User
    created_on: datetime
    edited_on: datetime | None

    @root_validator(pre=True)
    def count_projects(cls, vals):
        projects = vals.get('projects', [])
        vals['projects_count'] = len(projects)

        return vals

    class Config:
        orm_mode = True
        getter_dict = ModifiableGetter


class UserInDB(User):
    hashed_password: str

    class Config:
        orm_mode = True


class TokenData(BaseModel):
    username: str | None = None
