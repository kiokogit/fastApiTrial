from uuid import UUID
from pprint import pformat

from loguru import logger
from fastapi import APIRouter, HTTPException
from pydantic import ValidationError
from sqlalchemy import select

from arbm_core.private.investors import Fund

from arbm_core.public.projects import Project
from arbm_core.core.parsing import parse_website

from dependencies import LoggedInUser, PrivateSession, QueryParams
from schemas.schemas import SearchFiltersSchema
from schemas.funds import FundFullSchema
from schemas.project import ProjectSchema
from search_utils import search_and_publish, copy_project_filters, get_filter_values, \
                            suggest_projects
from dependencies import PrivateSession

router = APIRouter()


@router.get('/options/{field_name}')
def suggest_options(_: LoggedInUser,
                    private_s: PrivateSession,
                    field_name: str,
                    query_str: str | None = None,
                    ) -> list[str]:
    return get_filter_values(private_s, field_name, query_str=query_str)


@router.get('/funds')
def search_funds(session: PrivateSession,
                 _: LoggedInUser,
                 params: QueryParams
                 ) -> list[FundFullSchema]:
    orm_q = select(Fund)

    if params.q:
        fund_q = f'%{params.q.strip()}%'
        orm_q = orm_q.where(Fund.name.ilike(fund_q))

    return session.scalars(
                        orm_q.order_by(Fund.name).
                        offset(params.offset).
                        limit(params.limit)
        ).all()


@router.get('/verticals')
def get_verticals(session: PrivateSession, _: LoggedInUser) -> list[str]:
    return get_filter_values(session, 'verticals', query_str=None)


@router.post('/search')
def search_endpoint(search_filters: SearchFiltersSchema, _: LoggedInUser, private_s: PrivateSession) -> list[ProjectSchema]:
    project_uuids = search_and_publish(private_s=private_s, search_filters=search_filters)

    if not project_uuids:
        return []
    projects = public_s.query(Project).filter(Project.uuid.in_(project_uuids)).all()

    valid_projects = []
    for p in projects:
        try:
            ProjectSchema.from_orm(p)
            valid_projects.append(p)
        except ValidationError as e:
            logger.error(f'error validating project {p}')
            logger.error(e)
            continue
    return sorted(valid_projects, key=lambda p: p.title)


@router.post('/search/project')
def find_project(_: LoggedInUser, session: PrivateSession,
                 project_query: str,
                 include_data: bool = False,
                 full_search: bool = False
                ) -> list[dict]:
    return suggest_projects(session, project_query, include_data, include_pending=full_search)


@router.post('/parse')
def parse_website_content(website_url: str, _: LoggedInUser):
    return parse_website('/home/api_pub/website_parser/WebScrapper.js', website_url)


@router.post('/search/project/{project_uuid}/filters')
def get_project_filters(project_uuid: UUID,
                        _: LoggedInUser,
                        private_s: PrivateSession) -> dict[str, list | None]:
    return copy_project_filters(private_s, project_uuid)
