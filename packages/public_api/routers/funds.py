import datetime
from pprint import pformat
from typing import Annotated
from uuid import UUID
from itertools import groupby
from collections import namedtuple

from loguru import logger

from sqlalchemy import select, func, text, desc, distinct, and_, or_

from fastapi import APIRouter, Depends, HTTPException
from fastapi_cache.decorator import cache

from arbm_core.core import MongoDb
from arbm_core.private.projects import Fund, TrackedProject, ProjectTagsAssociation
from arbm_core.public.projects import Project


from dependencies import LoggedInUser, PrivateSession, QueryParams

from schemas.funds import FundFullSchema
from schemas.project import ProjectSchema


router = APIRouter()


def restrict_funds_to_organization(current_user: LoggedInUser, fund_uuid: UUID):
    # logger.info([f.uuid for f in current_user.organization.funds_portfolio])
    if fund_uuid not in [f.uuid for f in current_user.organization.funds_portfolio]:
        raise HTTPException(status_code=403, detail="Fund is not available for user's organisation")

    return fund_uuid

RestrictedFund = Annotated[UUID, Depends(restrict_funds_to_organization)]


@router.get('/published')
def get_published_funds(_: LoggedInUser, s: PrivateSession, query: QueryParams) -> list[FundFullSchema]:
    stmt = select(Fund).where(Fund.published)

    if query and query.q:
        stmt = stmt.where(Fund.name.ilike(f'%{query.q}%'))
    if query and query.offset:
        stmt = stmt.offset(query.offset)
    if query and query.limit:
        stmt = stmt.limit(query.limit)

    return s.scalars(stmt).all()


@router.get('/{fund_uuid}')
def get_fund(_: LoggedInUser, fund_uuid: RestrictedFund, s: PrivateSession) -> FundFullSchema:
    fund = s.scalars(select(Fund).where(Fund.uuid == fund_uuid)).one_or_none()

    if fund is None:
        raise HTTPException(status_code=404, detail="Fund doesn't exist with this UUID")

    fund_data = FundFullSchema.from_orm(fund)

    return fund_data

#@cache(expire=60 * 60)
def get_fund_signals(s, fund_uuid):
    fund: Fund = s.scalars(select(Fund).where(Fund.uuid == fund_uuid)).one_or_none()
    return fund.compute_signals()


#@cache(expire=60 * 60)
def get_fund_dealflow(s, fund_uuid, query: QueryParams | None = None, filters: list | None = None):
    if not filters:
        filters = []

    signals = get_fund_signals(s, fund_uuid)

    dealflow = []
    for year in signals:
        for month in year.get('months', []):
            for signal in month.get('signals', []):
                dealflow.append((signal['project_uuid'], datetime.date(year=year['year'], month=month['month'], day=1)))

    dealflow = [p[0] for p in sorted(dealflow, key=lambda x: (x[1], x[0]), reverse=True)][:1000]

    if (query and query.q):
        filters.append(TrackedProject.title.ilike(f'%{query.q}%'))

    projects = s.scalars(select(Project).join(TrackedProject, TrackedProject.uuid == Project.uuid).where(
                                                                                        TrackedProject.status != 'rejected', \
                                                                                        Project.uuid.in_(dealflow),
                                                                                        *filters)
                                                                                    ).all()
    logger.info(f'len of dealflow: {len(projects)} ({len(dealflow)})')
    projects = sorted(projects, key=lambda p: dealflow.index(p.uuid))

    if query and query.offset:
        projects = projects[query.offset:]
    if query and query.limit:
        projects = projects[:query.limit]

    return projects


@router.get('/{fund_uuid}/dealflow', deprecated=True)
#@cache(expire=60 * 60)
def fund_dealflow(_: LoggedInUser, fund_uuid: RestrictedFund, query: QueryParams, s: PrivateSession) -> list[ProjectSchema]:
    projects = get_fund_dealflow(s, fund_uuid, query)
    return projects


@router.get('/{fund_uuid}/dealflow/tags')
#@cache(expire=60 * 60)
def fund_dealflow_tags(_: LoggedInUser,
                         fund_uuid: RestrictedFund,
                         s: PrivateSession):
    dealflow = get_fund_dealflow(s, fund_uuid)

    supported_tags = ['verticals', 'product_types', 'customer_segments', 'company_types', 'competing_space']

    project_ids = s.scalars(select(TrackedProject.id).where(TrackedProject.uuid.in_([p.uuid for p in dealflow]))).all()
    tag_options = s.execute(select(ProjectTagsAssociation.tag_type,
                                    ProjectTagsAssociation.tag_name,
                                    func.count('*').label('count')).where(
                        ProjectTagsAssociation.tag_type.in_(supported_tags),
                        ProjectTagsAssociation.project_id.in_(project_ids),
                    )
                    .group_by(ProjectTagsAssociation.tag_type, ProjectTagsAssociation.tag_name)
                    .order_by(
                        ProjectTagsAssociation.tag_type,
                        desc(text('count'))
                    )
        ).all()

    top_filters = sorted(tag_options, key=lambda t: t.count, reverse=True)[:5]

    # data should be sorted by group key for the groupby to work
    tag_options = groupby(tag_options, lambda t: t.tag_type)

    return {'filters': {k: list(v) for k, v in tag_options},
            'top_filters': top_filters
            }


@router.post('/{fund_uuid}/dealflow/filter',
             openapi_extra={
                "requestBody": {
                    "content": {"application/json": {"example": [
                                                {
                                                    'tag_type': 'verticals',
                                                    'tag_names': ['E-commerce', 'SaaS']
                                                },
                                                {
                                                    'tag_type': 'customer_segments',
                                                    'tag_names': ['B2B', 'B2C']
                                                }
                                            ]}},
                    "required": True,
                },
    })
def filter_dealflow(_: LoggedInUser,
                    fund_uuid: RestrictedFund,
                    s: PrivateSession,
                    query: QueryParams,
                    required_tags: list[dict]) -> list[ProjectSchema]:
    '''
    Use this endpoint to filter dealflow by tags
    '''
    if not required_tags:
        return get_fund_dealflow(s, fund_uuid, query)

    QueryTuple = namedtuple('TextQuery', ['q', 'offset', 'limit'])
    dealflow = get_fund_dealflow(s, fund_uuid, QueryTuple(q=query.q, limit=None, offset=None))

    tag_filters = [
        and_(ProjectTagsAssociation.tag_type == tag['tag_type'], ProjectTagsAssociation.tag_name.in_(tag['tag_names']))
        for tag in required_tags
    ]

    project_ids = s.scalars(select(TrackedProject.id).where(TrackedProject.uuid.in_([p.uuid for p in dealflow]))).all()

    stmt = select(ProjectTagsAssociation.project_id)\
                            .where(
                                or_(*tag_filters),
                                ProjectTagsAssociation.project_id.in_(project_ids)
                            )\
                            .group_by(ProjectTagsAssociation.project_id)\
                            .having(
                                func.count(distinct(ProjectTagsAssociation.tag_type)) == len(required_tags)
                            )
    projects_with_tags = s.scalars(stmt).all()

    # logger.info(pformat(s.execute(projects_tags).all()))

    # stmt = select(TrackedProject.uuid).join(ProjectTagsAssociation, ProjectTagsAssociation.project_id == TrackedProject.id).where(
    #     TrackedProject.uuid.in_([p.uuid for p in dealflow]),
    #     *tag_filters
    # )

    project_uuids = s.scalars(select(TrackedProject.uuid).where(TrackedProject.id.in_(projects_with_tags))).all()

    dealflow = s.scalars(select(Project).where(Project.uuid.in_(project_uuids))).all()[query.offset:query.offset + query.limit]

    return dealflow



@router.get('/{fund_uuid}/timeline')
#@cache(expire=60 * 60)
def get_fund_timeline(_: LoggedInUser, fund_uuid: RestrictedFund, s: PrivateSession):
    return get_fund_signals(s, fund_uuid)
