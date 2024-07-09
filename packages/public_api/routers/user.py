from typing import Annotated
from uuid import UUID

from loguru import logger

from sqlalchemy import select
from fastapi import APIRouter, Body, Depends, HTTPException

from arbm_core.private.investors import Fund
from arbm_core.public.users import AutoProjectList, ClientUser
from arbm_core.public.projects import FundProfile

from dependencies import LoggedInUser, PrivateSession
from schemas.funds import FundFullSchema
from schemas.user import AutoList, CreateAutoList, PatchAutoList


router = APIRouter()


def get_user_profile(db: PrivateSession, current_user: LoggedInUser):
    return db.get(ClientUser, current_user.username)

UserProfile = Annotated[ClientUser, Depends(get_user_profile)]


def get_org_auto_list(db: PrivateSession, user: UserProfile, list_id: int):
    auto_list = db.get(AutoProjectList, list_id)
    if not auto_list or auto_list.organization != user.organization:
        raise HTTPException(status_code=404, detail='AutoList not found')
    return auto_list


ProtectedAutoList = Annotated[AutoProjectList, Depends(get_org_auto_list)]


@router.get("/funds")
def get_subscribed_funds(db: PrivateSession, user: UserProfile) -> list[FundFullSchema]:
    return [db.scalars(select(Fund).where(Fund.uuid == f.uuid)).one()
            for f in user.organization.funds_portfolio]


@router.post("/pipeline/funds/add", status_code=204)
def add_fund(db: PrivateSession, user: UserProfile, fund_uuid: UUID):
    fund = db.get(FundProfile, fund_uuid)

    user.pipeline_funds.append(fund)
    db.add(user)
    db.commit()


@router.post("/pipeline/funds/remove", status_code=204)
def remove_fund(db: PrivateSession, user: UserProfile, fund_uuid: UUID):
    fund = db.get(FundProfile, fund_uuid)

    user.pipeline_funds.remove(fund)
    db.add(user)
    db.commit()


@router.get("/autolists",
            response_model=list[AutoList],
            response_model_exclude={'projects': True},
)
def get_all_autolists(user: UserProfile):
    return user.organization.auto_project_lists


@router.get("/autolists/{list_id}")
def get_autolist(db: PrivateSession, user: UserProfile, auto_list: ProtectedAutoList) -> AutoList:
    return auto_list


@router.post("/autolists")
def create_autolist(db: PrivateSession, user: UserProfile, new_list: CreateAutoList) -> AutoList:
    created_list = AutoProjectList(
        organization=user.organization,
        author=user,
        **new_list.dict()
    ) # type: ignore
    logger.error(created_list)

    if len(user.organization.auto_project_lists) >= user.organization.max_auto_lists:
        created_list.active=False

    db.add(created_list)
    db.commit()
    db.refresh(created_list)
    return created_list


@router.patch("/autolists/{list_id}")
def update_autolist(db: PrivateSession, user: UserProfile, auto_list: ProtectedAutoList, list_update: PatchAutoList):
    if (active := list_update.dict().get('active')) and len([l for l in user.organization.auto_project_lists if l.active]) >= user.organization.max_auto_lists:
        raise HTTPException(status_code=400, detail="You have reached the maximum number of active auto lists")

    for key, value in list_update.dict(exclude_none=True).items():
        setattr(auto_list, key, value)

    db.add(auto_list)
    db.commit()
    db.refresh(auto_list)
    return auto_list



@router.delete("/autolists/{list_id}", status_code=204)
def delete_autolist(db: PrivateSession, auto_list: ProtectedAutoList):
    db.delete(auto_list)
    db.commit()


@router.get("/settings/reachout/template")
def get_reachout_template(user: UserProfile):
    return user.organization.reachout_template


@router.post("/settings/reachout/template")
def set_reachout_template(db: PrivateSession, user: UserProfile, template: Annotated[str, Body()]):
    if len(template) < 10:
        raise ValueError("Template is too short")

    if len(template) > 2000:
        raise ValueError("Template is too long")

    user.organization.reachout_template = template
    db.add(user)
    db.commit()