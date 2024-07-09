import datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel

from schemas.schemas import ModifiableGetter, root_validator
from schemas.feed import ProjectSchema, ProjectUserInfo


class UserStatsSchema(BaseModel):
    username: str

    all_projects: list[ProjectUserInfo]

    great_projects: list[ProjectUserInfo]
    good_projects: list[ProjectUserInfo]
    unfit_projects: list[ProjectUserInfo]

    unrated_projects: list[ProjectUserInfo]
    projects_with_feedback: list[ProjectUserInfo]

    rated_projects_percentage: int
    feedback_projects_percentage: int


class OrganizationReportSchema(BaseModel):
    most_active_user: str

    users_stats: list[UserStatsSchema]

    great_projects: list[ProjectSchema]
    unrated_projects: list[ProjectSchema]
    one_response_projects: list[ProjectSchema]
    unfit_no_feedback: list[ProjectSchema]

    team_rated_projects_percentage: int
    team_feedback_projects_percentage: int


class ReportStatsSchema(BaseModel):
    most_active_user: str

    team_rated_projects_percentage: int
    team_feedback_projects_percentage: int


class ReportDataSchema(BaseModel):
    uuid: UUID
    report_type: str
    start_date: datetime.date

    report_stats: ReportStatsSchema | None

    @root_validator(pre=True)
    def validate_report_stats(
        cls,
        values: Any,
    ):
        """
        Initialise report_stats from the orm object
        """
        report_stats = {}

        report_data = values.get('contents', {})

        for field in ReportStatsSchema.schema()['properties']:
            if field in report_data:
                report_stats[field] = report_data[field]

        values['report_stats'] = report_stats

        return values

    class Config:
        orm_mode = True
        getter_dict = ModifiableGetter