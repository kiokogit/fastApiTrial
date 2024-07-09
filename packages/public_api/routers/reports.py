import sys
from uuid import UUID
from loguru import logger

from fastapi import APIRouter, HTTPException, Request, Response

from arbm_core.public.users import ClientOrganization
from arbm_core.public.users import OrganizationReport
from arbm_core.public.projects import UserProjectAssociation, Project
from schemas.feed import ProjectSchema, ProjectUserInfo

from dependencies import PrivateSession
from schemas.reports import OrganizationReportSchema
from schemas.reports import ReportDataSchema

from pydantic import BaseModel, root_validator
from pydantic.utils import GetterDict
from typing import Any

from dependencies import LoggedInUser
from utils import log_user_event


logger.add(sys.stdout)
logger.add(sys.stderr)


router = APIRouter()


class ModifiableGetter(GetterDict):
    """
    Custom GetterDict subclass allowing to modify values
    """
    def __setitem__(self, key: str, value: Any) -> Any:
        return setattr(self._obj, key, value)


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
    most_active_user: str | None

    users_stats: list[UserStatsSchema]

    great_projects: list[ProjectSchema]
    unrated_projects: list[ProjectSchema]
    one_response_projects: list[ProjectSchema]
    unfit_no_feedback: list[ProjectSchema]

    team_rated_projects_percentage: int
    team_feedback_projects_percentage: int


class ReportStatsSchema(BaseModel):
    most_active_user: str | None

    team_rated_projects_percentage: int
    team_feedback_projects_percentage: int


class ReportDataSchema(BaseModel):
    uuid: UUID
    report_type: str
    start_date: str #datetime.date

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

    @root_validator(pre=True)
    def report_title(
        cls,
        values: Any,
    ):
        date_from = values['start_date'].strftime("%a, %-d %b")
        date_to = values['end_date'].strftime("%a, %-d %b")
        values['start_date'] = f"{date_from} to {date_to}"
        return values


    class Config:
        orm_mode = True
        getter_dict = ModifiableGetter


def get_user_rated_projects(s, username: str, rating: int | None, filters: list,  feedback_required: bool = False):
    # avoid modifying filters in-place
    filters = filters.copy()

    filters.append(UserProjectAssociation.username == username)

    if feedback_required:
        filters.append(UserProjectAssociation.feedback != None)

    match rating:
        case None:
            pass
        case 0:
            filters.append(UserProjectAssociation.rating == None)
        case _:
            filters.append(UserProjectAssociation.rating == rating)

    return s.query(UserProjectAssociation) \
            .filter(*filters) \
            .all()


@router.get('/reports/weekly')
def organization_all_reports(request: Request, current_user: LoggedInUser, s: PrivateSession) -> list[ReportDataSchema]:
    log_user_event(user=current_user,
                event=request.url.path,
                details={
                    'ip': request.client,
                })

    org = s.query(ClientOrganization).get(current_user.organization_id)

    reports = s.query(OrganizationReport).filter(OrganizationReport.revoked==False,
                                                     OrganizationReport.report_type=='weekly',
                                                     OrganizationReport.organization==org)\
                                            .order_by(OrganizationReport.start_date.desc(),
                                                      OrganizationReport.time_generated.desc()).all()

    return reports


@router.get('/reports/{report_uuid}')
def organization_report(request: Request, response: Response, report_uuid: UUID, current_user: LoggedInUser, s: PrivateSession) -> OrganizationReportSchema:
    log_user_event(user=current_user,
                event=request.url.path,
                details={
                        'ip': request.client,
                        'report_uuid': str(report_uuid),
                })

    org = s.query(ClientOrganization).get(current_user.organization_id)

    report = s.query(OrganizationReport).get({'organization_id': org.name, 'uuid': report_uuid})

    if report is None:
        raise HTTPException(status_code=400, detail=f'report with uuid {report_uuid} was not found for'
                                f' user {current_user}. does user belong to the report organization? ')

    report = report.contents

    for project_list in ['great_projects', 'unrated_projects', 'one_response_projects', 'unfit_no_feedback']:
        new_schemas = []
        for project in report[project_list]:
            new_schemas.append(s.query(Project).get(project['uuid']))

        report[project_list] = new_schemas

    return report




