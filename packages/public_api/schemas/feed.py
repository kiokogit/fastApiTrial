from contextlib import suppress
from pprint import pformat
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseConfig, BaseModel, Field, ValidationError, validator
from loguru import logger

from .project import ProjectSchema


class ProjectUserInfo(BaseModel):
    project_id: UUID
    username: str

    time_recommended: datetime
    archived: bool

    favourite: bool
    contacted: bool | None

    rating: int | None
    feedback: str | None
    feedback_posted: datetime | None

    class Config:
        orm_mode = True


class ProjectEntry(BaseModel):
    project: ProjectSchema
    project_user_info: ProjectUserInfo | None
    comments: list | None


class SafeProjectList(BaseModel):
    __root__: list[ProjectEntry]

    @validator("__root__", pre=True)
    def validate_projects(
        cls,
        value: list[dict],
        values: "Stuff that was already parsed",
        config: BaseConfig,
        field,
    ):
        projects = []
        for v in value:
            try:
                ProjectEntry(**v)
                projects.append(v)
            except ValidationError:
                logger.critical(f'Failed to validate ProjectEntry for project {v["project"].title} {v["project"].uuid}')
        return projects


class Feed(BaseModel):
    funds: list[str]
    verticals: list[str]

    projects: list[ProjectEntry] | None

    @validator("projects", pre=True)
    def validate_projects(
        cls,
        value: list[dict],
        values: "Stuff that was already parsed",
        config: BaseConfig,
        field,
    ):
        projects = []
        for v in value:
            try:
                ProjectEntry(**v)
                projects.append(v)
            except ValidationError as e:
                logger.critical(f'Failed to validate ProjectEntry for project {v["project"]}, errors:')
                logger.critical(e.errors())

        return projects
