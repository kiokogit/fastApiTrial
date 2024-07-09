from fastapi import APIRouter, HTTPException

from loguru import logger

from arbm_core.private.investors import Investor
from crm.schemas.collections import CollectionSchema
from crm.schemas.entities import InvestorSchema

from parsing.linkedin_parsing import queue_linkedin_signals
from crm.schemas.parsing import LinkedinLikesSignalSchema

from crm.dependencies import DbSession
from crm.helpers import lookup_obj

from crm.routers.collections import Collection


router = APIRouter()


@router.get("/investor_search")
def investor_search(db: DbSession, query_string: str):
    if not query_string:
        raise HTTPException(status_code=400, detail="query string is empty")

    res = lookup_obj(db, Investor, query_string, query_field='name')

    # res = [i for i in res if any([f.thesis for f in i.funds])]

    return [InvestorSchema.from_orm(i) for i in res]


@router.get("/valid_titles")
def get_titles_collection(name: str, coll: Collection) -> CollectionSchema:
    return dict(name=name, items=list(coll.find(limit=100)))
    # get_collection(name=parsing_type)


@router.post("/push_linkedin_signals")
def upload_linkedin_signals(db: DbSession, like_signals: LinkedinLikesSignalSchema, parsing_type: str = 'startups'):
    match parsing_type:
        case 'startups':
            queue_id = 'linkedin_likes_enrich_v2'
        case 'competitors':
            queue_id = 'competitors_mapping'
        case _:
            raise HTTPException(status_code=401, detail='Unsupported parsing category')

    logger.critical(f'got like signals with type {parsing_type}: {like_signals}')

    investor_id = like_signals.investor.id
    investor = db.get(Investor, investor_id)

    if not all([fund.thesis for fund in investor.funds]):
        funds_no_thesis = ', '.join([fund.name for fund in investor.funds if not fund.thesis])
        return {
            'status': 'error',
            'msg': f'cannot parse signals, one or more funds of investor do not have a thesis: {funds_no_thesis}',
        }


    leaders_queued = queue_linkedin_signals(like_signals, queue_id=queue_id)

    logger.critical(f'queued {leaders_queued} leaders')
    return {
        'status': 'success',
        'msg': f'{leaders_queued} profiles have been queued successfully',
    }


