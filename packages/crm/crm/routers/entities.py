from typing import Annotated
from uuid import UUID
from pprint import pformat

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from sqlalchemy import select

from loguru import logger

from arbm_core.private.investors import Fund, Investor
from arbm_core.public.projects import FundProfile
from arbm_core.public.users import ClientOrganization

from crm import IMAGES_URL
from crm.file_handlers import save_fund_thumbnail
from crm.helpers import query_model
from crm.schemas.clients import OrgSchema

from ..dependencies import DbSession, QueryParams, RouterTags
from ..schemas.entities import FundCreateSchema, FundSchema, FundPatchSchema, InvestorSchema


router = APIRouter()


def get_fund_by_id(fund_id: Annotated[int | UUID, Path()], db: DbSession):
    print(fund_id)
    match fund_id:
        case int():
            fund = db.get(Fund, fund_id)
        case UUID():
            fund = db.execute(select(Fund).where(Fund.uuid == fund_id)).scalar()

    if not fund:
        raise HTTPException(detail="Fund not found", status_code=404)

    return fund


FundById = Annotated[Fund, Depends(get_fund_by_id)]


@router.get('/funds', tags=[RouterTags.funds])
def get_funds(db: DbSession, q: QueryParams) -> list[FundSchema]:
    return query_model(db, Fund, q, query_field='name')


@router.get('/funds/{fund_id}', tags=[RouterTags.funds])
def get_fund(fund: FundById) -> FundSchema:
    return fund


@router.get('/funds/{fund_id}/clients', tags=[RouterTags.funds])
def get_fund_clients(db: DbSession, fund: FundById) -> list[OrgSchema]:
    return db.scalars(select(ClientOrganization).where(ClientOrganization.funds_portfolio.any(Fund.uuid == fund.uuid))).all()


@router.patch('/funds/{fund_id}/client', tags=[RouterTags.funds])
def add_fund_client(db: DbSession, fund: FundById, organization_id: str) -> list[OrgSchema]:
    client = db.get(ClientOrganization, organization_id)
    if not client:
        raise HTTPException(detail="Client not found", status_code=404)

    fund.published = True
    db.add(fund)

    # if fund is published, update it
    if published_fund := db.get(FundProfile, fund.uuid):
        published_fund.logo = fund.logo
        published_fund.name = fund.name
        db.add(published_fund)

    # use existing fund or create a new one
    client.funds_portfolio.append(published_fund or FundProfile(uuid=fund.uuid,
                                                    name=fund.name,
                                                    logo=fund.logo)
                                )

    db.add(client)
    db.commit()

    return db.scalars(select(ClientOrganization).where(ClientOrganization.funds_portfolio.any(Fund.uuid == fund.uuid))).all()


@router.delete('/funds/{fund_id}/client', tags=[RouterTags.funds])
def remove_fund_client(db: DbSession, fund: FundById, organization_id: str) -> list[OrgSchema]:
    client = db.get(ClientOrganization, organization_id)

    client.funds_portfolio = [f for f in client.funds_portfolio if f.uuid != fund.uuid]

    db.add(client)
    db.commit()

    return db.scalars(select(ClientOrganization).where(ClientOrganization.funds_portfolio.any(Fund.uuid == fund.uuid))).all()


@router.patch('/funds/{fund_id}', tags=[RouterTags.funds])
def update_fund(fund: FundById,
                fund_patch: FundPatchSchema,
                db: DbSession) -> FundSchema:
    fund_frontend = db.get(FundProfile, fund.uuid)

    for attr, value in fund_patch.dict(exclude_unset=True).items():
        if value == getattr(fund, attr):
            continue

        if attr == 'logo' and value:
            logger.critical('saving logo')
            value = str(logo_url := IMAGES_URL +'/'+  str(save_fund_thumbnail(fund.uuid, value)))

        logger.info(f'fund.{attr} = {getattr(fund, attr)} -> {value}')

        setattr(fund, attr, value)

        if hasattr(fund_frontend, attr):
            setattr(fund_frontend, attr, value)


    db.add(fund)

    if fund_frontend:
        db.add(fund_frontend)

    db.commit()
    db.refresh(fund)

    return fund


@router.post('/funds/{fund_id}/publish', tags=[RouterTags.funds])
def publish_fund(fund: FundById,
                published: bool,
                db: DbSession) -> FundSchema:
    fund.published = published
    db.add(fund)
    db.commit()

    return fund

@router.get('/investors', tags=[RouterTags.investors])
def get_investors(db: DbSession, q: QueryParams, funds: Annotated[list[UUID] | None, Query()] = None) -> list[InvestorSchema]:
    filters = []
    if funds:
        filters = [Investor.funds.any(Fund.uuid.in_(funds))]

    return query_model(db, Investor, q, query_field='name', filters=filters)


@router.get('/investors/{investor_id}', tags=[RouterTags.investors])
def get_investor(investor_id: int, db: DbSession) -> InvestorSchema:
    if not (investor := db.get(Investor, investor_id)):
        raise HTTPException(detail="Investor not found", status_code=404)
    return investor
