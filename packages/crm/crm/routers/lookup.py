from typing import Literal
from fastapi import APIRouter

from arbm_core.private.projects import TrackedProject
from arbm_core.private.investors import Fund, Investor

from crm.dependencies import DbSession, RouterTags
from crm.helpers import lookup_obj

from crm.schemas.projects import ProjectSchema
from crm.schemas.entities import FundSchema, InvestorSchema

from util import strip_url


router = APIRouter()


@router.get('/fund', tags=[RouterTags.funds], deprecated=True)
def lookup_fund(db: DbSession, query: str):
    return [FundSchema.from_orm(f) for f in lookup_obj(db, Fund,
                                                        query_field='name', query_string=query)]


@router.get('/investor', tags=[RouterTags.investors], deprecated=True)
def lookup_investor(db: DbSession, query: str):
    return [InvestorSchema.from_orm(i) for i in lookup_obj(db, Investor,
                                                            query_field='name', query_string=query)]


@router.get('/project', tags=[RouterTags.projects], deprecated=True)
def lookup_project(db: DbSession, query_field: Literal['title', 'website'], query: str):
    """
    Search for a project by a text field
    """
    if query_field == 'website':
        query = strip_url(query)

    return [ProjectSchema.from_orm(p) for p in lookup_obj(db, TrackedProject,
                                                query_field=query_field, query_string=query)]
