from pprint import pformat
from typing import Annotated
import uuid

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, or_

from loguru import logger

from arbm_core.core.signals import AddSignal, YearMonth
from arbm_core.private.investors import Fund
from arbm_core.private.projects import TrackedProject, ProjectAnalytics, InvestmentStage, \
                                        ProjectTag, ProjectStatus, Leader
from arbm_core.private.linkedin import LinkedinProfile
from arbm_core.core.utils import get_one_or_create
from crm.schemas.feed import FeedSchema
from parsing.linkedin_enrichment import enrich_companies, update_linkedin_project
from projects import LogEvent
from projects.linkedin_utils import extract_project_data, parse_company_data

from projects.project_init import inject_project, update_entities, update_project_details

from crm.filters import PROJECT_FILTERS, ChoicesFilterSchema, filter_objects, ProjectFiltersSchema
from crm.helpers import group_by, patch_object
from crm.schemas.projects import LeaderCreateSchema, LeaderUpdateSchema, ProjectAnalyticsPatchSchema, ProjectAnalyticsSchema, \
    ProjectCreateSchema, ProjectPatchSchema, ProjectSchema
from crm.dependencies import DbSession, QueryParams
from projects.schemas.project import ProjectData
from projects.schemas.signals import DirectSignal, LinkedinPostSignal, LinkedinSourceSchema


router = APIRouter()


def get_project_consts(s):
    return {
        'status': [{'name': str(s), 'value': s.value} for s in ProjectStatus],
        'stages': [s.value for s in InvestmentStage],
        'verticals': [{'name': v.name}
                        for v in s.scalars(select(ProjectTag)
                                           .filter_by(type='verticals')
                                           .order_by(ProjectTag.name))
                                  .all()
                     ]
    }


def get_projects_feed(db, q, filters_schema: ProjectFiltersSchema | None = None):
    filters = filters_schema.dict() if filters_schema else {}

    count, projects = filter_objects(db, TrackedProject, q, filters)

    project_funds = set()
    for p in projects:
        project_funds = project_funds.union(set(p.interested_funds))

    funds_by_type = group_by(project_funds, 'type', 'No type')

    return {
        'projects_count': count,
        'projects': [ProjectSchema.from_orm(p) for p in projects],
        'funds': {g: [f.to_dict() for f in funds] for g, funds in funds_by_type.items()},
        'const': get_project_consts(db),
    }


@router.get('/constants')
def project_constants(db: DbSession) -> dict:
    return get_project_consts(db)


@router.get('/filters')
def filter_options(db: DbSession):
    filter_sections = PROJECT_FILTERS
    filter_options = []

    for filters_config in filter_sections:
        section_filters = []

        for k, v in filters_config.get('filters', {}).items():
            options_v = v.copy()
            for field in v:
                if field.startswith('filter'):
                    options_v.pop(field)
                    continue
                if field.endswith('func'):
                    # remove func from field name and run function to get field values
                    fn = options_v.pop(field)
                    options_v[field.replace('_func', '')] = fn(db)
            options_v['key'] = k
            section_filters.append(options_v)

        section = {'filters': section_filters}
        if id := filters_config.get('section'):
            section['section'] = id
        if title := filters_config.get('title'):
            section['title'] = title

        filter_options.append(section)

    return filter_options


@router.get('')
def projects(db: DbSession,
             q: QueryParams,
             status: Annotated[list[ProjectStatus] | None, Query()] = None
             ) -> FeedSchema:

    filters = None
    if status:
        filters = ProjectFiltersSchema(
            filters=[
                ChoicesFilterSchema(
                    identifier='status',
                    mode='OR',
                    choices=status
                )
            ]
        )

    count, projects = get_projects_feed(db, q, filters)
    return {'results_count': count, 'projects': projects, 'const': get_project_consts(db)}


@router.post('/search')
def search_projects(db: DbSession,
             q: QueryParams,
             filters: ProjectFiltersSchema
             ) -> FeedSchema:
    count, projects = filter_objects(db, TrackedProject, q, filters.dict())

    return {'results_count': count, 'projects': projects, 'const': get_project_consts(db)}



@router.post('')
def create_project(db: DbSession, new_project: ProjectCreateSchema) -> dict:
    # for submissions done manually, explicitly check if the founder exists
    raise NotImplementedError
    if isinstance((source := new_project.source), LinkedinSourceSchema):
        profile_id = source.signal.leader_url


        founder_profile = db.query(LinkedinProfile).get(profile_id)
        if founder_profile is None:
            raise HTTPException(detail="Profile not found", status_code=501)

    event, tracked_project = inject_project(db, project_update=new_project)

    return {
        'result': 'success',
        'event': event,
        'project_id': tracked_project.id,
        'project': ProjectSchema.from_orm(tracked_project),
        'constants': get_project_consts(db)
    }


@router.post('/manual_submission')
def manual_submission(db: DbSession,
                      signal: DirectSignal,
                      # required if project_id is not provided
                      project_id: Annotated[int | None, Body()] = None,
                      project_create: ProjectData | None = None,
                      linkedin_url: Annotated[str | None, Body()] = None):
    if project_id:
        project = db.get(TrackedProject, project_id)

        if not project:
            raise HTTPException(detail="Project not found", status_code=404)
    else:
        if not project_create:
            raise HTTPException(detail="Project data is required for new projects", status_code=400)

        project, exists = get_one_or_create(db, TrackedProject,
                                    title=project_create.title,
                                    website=project_create.website,
                                    create_method_kwargs=dict(analytics=ProjectAnalytics(), **project_create.dict(
                                            exclude_unset=True
                                        ))
                                )
        if not exists:
            if not linkedin_url:
                raise HTTPException(detail="Linkedin URL is required for new projects", status_code=400)

            company_json = enrich_companies([linkedin_url])[0]
            linkedin_details = parse_company_data(company_json)
            project_data: ProjectData = extract_project_data(linkedin_details)

            source = LinkedinSourceSchema(signal=None, company_url=linkedin_url, linkedin_details=linkedin_details)
            entity_profile = update_entities(db, project_data, source)

            db.commit()

            project = update_project_details(project, project_data)

    # add signals by fund to the project
    fund = db.scalars(select(Fund).where(Fund.id == signal.investing_entity.id)).unique().one()

    class Source:
        def __init__(self, signal):
            self.signal = signal

    project.add_signal(signal=AddSignal(
                            project_uuid=project.uuid,
                            fund_uuid=fund.uuid,
                            timeframe=YearMonth(
                                year=signal.picked_up_date.year,
                                month=signal.picked_up_date.month,
                            ),
                            #todo: post id
                            source=Source(signal)
                        )
                    )

    if not project.status in (ProjectStatus.accepted, ProjectStatus.rejected):
        project.status = ProjectStatus.review
    db.add(project)
    db.commit()



@router.get('/{project_id}')
def get_project(db: DbSession, project_id: int) -> ProjectSchema:
    if not (project := db.get(TrackedProject, project_id)):
        raise HTTPException(detail="Project not found", status_code=404)
    return project


@router.patch('/{project_id}')
def patch_project(db: DbSession,
                  project_id: str,
                  project_patch: ProjectPatchSchema
                  ) -> ProjectSchema:

    if description := project_patch.description:
        project_patch.description = (description, 'analyst')

    p = patch_object(db.get(TrackedProject, project_id), project_patch)
    db.add(p)
    db.commit()
    return p


#@router.patch('/{project_id}/links')
def patch_project_links(db: DbSession,
                       project_id: str
                       ) -> ProjectSchema:
    #     if (link_type := request.form.get('linkType')) and (link_url := request.form.get('linkUrl')):
    #         if link_type == 'linkedin':
    #
    #             if not project.linkedin_profile:
    #                 project.linkedin_profile = LinkedinCompany(name=project.title,
#                                                                linkedin_url=link_url))
    #             else:
    #                 project.linkedin_profile.name = project.title
    #                 project.linkedin_profile.linkedin_url = link_url

#                 s.add(project.linkedin_profile)
    #         else:
    #             # custom links
    #             updated = False
    #             for l in project.links:
    #                 if l.name == link_type:
    #                     l.value = link_url
    #                     updated = True

    #             if not updated:
    #                 project.links.append(ProjectLink(name=link_type, value=link_url))

    #     s.add(project)
    #     s.commit()

    #     return jsonify({'result': 'success',
    #                     'new_status': ProjectStatus[status].name if status else project.status.name,
    #                     'project': project.to_dict()})
    raise NotImplementedError


def get_project_analytics(db: DbSession, project_id: int):
    project: TrackedProject = db.get(TrackedProject, int(project_id))

    return project.analytics or ProjectAnalytics(project_id=project.id)


InitAnalytics = Annotated[ProjectAnalytics, Depends(get_project_analytics)]


@router.patch('/{project_id}/analytics')
def patch_project_analytics(db: DbSession,
                            analytics: InitAnalytics,
                            update: ProjectAnalyticsPatchSchema) -> ProjectAnalyticsSchema:

    analytics = patch_object(analytics, update.dict(exclude_unset=True))

    db.add(analytics)
    db.commit()

    logger.critical(pformat(analytics))

    return analytics


@router.post('/{project_id}/leaders')
def add_leader(db: DbSession,
               analytics: InitAnalytics,
               leader: LeaderCreateSchema) -> LeaderCreateSchema:
    logger.debug(f'got leader update for the project {analytics.project}: {pformat(leader.dict())}')

    new_leader, is_new = get_one_or_create(db,
                                        Leader,
                                        create_method_kwargs=leader.dict(),
                                        filter_expression=or_(*[
                                            getattr(Leader, field) == getattr(leader, field)
                                            for field in ['email', 'linkedin']
                                            if getattr(leader, field) is not None
                                        ])
                                   )
    if not is_new:
        patch_object(new_leader, leader.dict(exclude_unset=True))

    analytics.leaders.append(new_leader)

    db.add(analytics)
    db.commit()

    return new_leader


@router.patch('/{project_id}/leaders')
def patch_leader(db: DbSession,
                 analytics: InitAnalytics,
                 leader_patch: LeaderUpdateSchema) -> LeaderUpdateSchema:

    if not (leader := db.get(Leader, leader_patch.id)):
        raise HTTPException(detail="Leader not found", status_code=404)

    patch_object(leader, leader_patch.dict(exclude_unset=True))

    if leader_patch.unlink:
        analytics.leaders = [leader for leader in analytics.leaders if leader.id != leader_patch.id]

    db.add(analytics)
    db.commit()

    return leader
