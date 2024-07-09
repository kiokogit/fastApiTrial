from pprint import pformat
from uuid import UUID

from fastapi import APIRouter
from loguru import logger
from pydantic import ValidationError

from arbm_core.public.projects import Project

from dependencies import PrivateSession
from search_utils import search_and_publish, copy_project_filters, get_filter_values, \
                         suggest_projects
from schemas.schemas import SearchFiltersSchema
from schemas.project import ProjectSchema
from routers.graph import Graph, build_graph


router = APIRouter()

@router.post('/market_map')
def search_graph(session: PrivateSession, search_filters: SearchFiltersSchema) -> Graph:
    project_uuids = search_and_publish(private_s=session, search_filters=search_filters)

    graph = build_graph(project_uuids, current_user=None, public_s=session)

    return graph


@router.post('/search/project')
def find_project(session: PrivateSession, project_query: str, include_data: bool = False, full_search: bool = False) -> list[dict]:
    return suggest_projects(session, project_query, include_data, include_pending=full_search)


@router.post('/filters/project/{project_uuid}')
def project_filters(session: PrivateSession, project_uuid: UUID) -> dict[str, list | None]:
    logger.info(f'getting project filters for UUID: {project_uuid}')

    return copy_project_filters(session, project_uuid)


@router.post('/filters/options/{field_name}')
def get_filter_options(session: PrivateSession, field_name: str, query: str | None = None):
    logger.info(f'getting field options for field: {field_name}')

    return get_filter_values(session, field_name, query)


@router.post('/search')
def public_search(session: PrivateSession, search_filters: SearchFiltersSchema):
    project_uuids = search_and_publish(private_s=session, search_filters=search_filters)

    if not project_uuids:
        return []
    projects = session.query(Project).filter(Project.uuid.in_(project_uuids)).all()

    valid_projects = []
    for p in projects:
        try:
            ProjectSchema.from_orm(p)
            valid_projects.append(p)
        except ValidationError:
            logger.error(f'error validating project {p}:'\
                        f'\n\n{pformat(p.__dict__)}')
            continue

    return valid_projects


@router.post('/verticals')
def verticals(session: PrivateSession):
    return get_filter_values(session, 'verticals', query_str=None)


@router.post('/search/options/{field_name}')
def search_options(session: PrivateSession, field_name: str, query_str: str | None = None):
    return get_filter_values(session, field_name, query_str=query_str)
