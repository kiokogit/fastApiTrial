from typing import Any
from uuid import UUID

from pydantic import BaseModel

from projects.schemas.filters import FilterResult


class LogEvent(BaseModel):
    event_name: str

    # an optional id for grouping related events
    group_id: UUID | None = None

    # optional human-readable description of the event
    display_name: str | None = None
    details: str | None = None

    data: Any = None


class ProjectEvent(LogEvent):
    project_id: int | None = None
    project_data: dict | None = None


class FilteringEvent(ProjectEvent):
    data: list[FilterResult] | list[dict] | None = None


class ProjectInitError(Exception):
    pass


class DuplicateProjectsError(Exception):
    pass


class ProjectUpdateError(Exception):
    pass


class ProjectException(Exception):
    def __init__(self, *args: object, cause: ProjectEvent) -> None:
        super().__init__(*args)
        self.cause = cause

    def to_dict(self):
        return {
            'message': self.args[0],
            'cause': self.cause
        }


class FilteringException(ProjectException):
    def __init__(self, *args: object, cause: FilteringEvent) -> None:
        super().__init__(*args, cause=cause)


class FilterPreconditionException(Exception):
    def __init__(self, *args: object, inputs: dict) -> None:
        super().__init__(*args)
        self.inputs = inputs

    def to_dict(self):
        return {
            'reason': self.args[0],
            'inputs': self.inputs
        }
