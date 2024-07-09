from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import ValidationError
import pytz
from sqlalchemy import select
from loguru import logger

from arbm_core.core.utils import get_one_or_create
from arbm_core.core.publishing import PublishingError, publish_project

from arbm_core.public.users import ClientOrganization, ClientUser
from arbm_core.public.projects import Project, UserProjectAssociation, FundProfile

from crm.dependencies import DbSession, PaginationParams, QueryParams
from crm.schemas.clients import OrgSchema, OrgCreateSchema, \
                                ClientUserSchema, ClientUserCreateSchema, ProjectUserSchema
from crm.schemas.entities import FundIdentitySchema
from crm.helpers import query_model
import util

router = APIRouter()


def get_client_org(org_id: str, db: DbSession) -> ClientOrganization:
    org = db.get(ClientOrganization, org_id)

    if not org:
        raise HTTPException(404, f"Organization not found: {org_id}")

    return org


def get_client_user(username: str, db: DbSession) -> ClientUser:
    user = db.get(ClientUser, username)

    if not user:
        raise HTTPException(404, f"User not found: {username}")

    return user


OrgExisting = Annotated[ClientOrganization, Depends(get_client_org)]
UserExisting = Annotated[ClientUser, Depends(get_client_user)]


PAGES_PERMISSIONS = ['curated_list','connected_ventures','team_reports','signals_search','deal_sources']


@router.get('/orgs')
def get_organizations(db: DbSession, query: QueryParams) -> list[OrgSchema]:
    return query_model(db, ClientOrganization, query, query_field='name', order_by=(ClientOrganization.membership.desc(), ClientOrganization.name))


@router.post('/orgs/{org_id}/portfolio')
def organization_portfolio_set(db: DbSession, org_id: str, fund_uuids: list[UUID]) -> list[UUID]:
    raise HTTPException(401, "endpoint disabled")
    org: ClientOrganization = db.get(ClientOrganization, org_id)

    if not org:
        raise HTTPException(404, f"Organization not found: {org_id}")

    new_portfolio = db.scalars(select(FundProfile).where(FundProfile.uuid.in_(fund_uuids))).all()

    if len(difference := set(fund_uuids).difference(set([f.uuid for f in new_portfolio]))):
        raise HTTPException(404, f"The following funds were not found: {', '.join([str(v) for v in difference])}")

    logger.info(len(difference))

    org.funds_portfolio = new_portfolio

    db.add(org)
    db.commit()

    return [f.uuid for f in org.funds_portfolio]


@router.post('/orgs')
def create_organization(db: DbSession, new_org: OrgCreateSchema) -> OrgSchema:
    db.add(ClientOrganization(**new_org.dict()))
    db.commit()
    return db.get(ClientOrganization, new_org.name)


@router.post('/orgs/pages/allow')
def allow_organization_page(db: DbSession, org: OrgExisting, page: str) -> OrgSchema:
    if page not in PAGES_PERMISSIONS:
        raise HTTPException(400, f"Invalid page name: {page}, must be one of {PAGES_PERMISSIONS}")

    org.allowed_pages = list(set(org.allowed_pages).add(page))
    db.commit()
    db.refresh(org)
    return org


@router.post('/orgs/pages/fordbid')
def forbid_organization_page(db: DbSession, org: OrgExisting, page: str) -> OrgSchema:
    if page not in PAGES_PERMISSIONS:
        raise HTTPException(400, f"Invalid page name: {page}, must be one of {PAGES_PERMISSIONS}")

    org.allowed_pages = list(set(org.allowed_pages).remove(page))
    db.commit()
    db.refresh(org)

    return org


@router.get('/users')
def get_users(db: DbSession, query: QueryParams) -> list[ClientUserSchema]:
    return query_model(db, ClientUser, query, query_field='username')


@router.post('/users')
def create_user(db: DbSession, new_user: ClientUserCreateSchema):
    org_id = new_user.organization_id
    org = db.get(ClientOrganization, org_id)

    if org is None:
        raise HTTPException(404, f"Organization not found with id '{org_id}'")

    existing = db.get(ClientUser, new_user.username)
    if existing is not None:
        raise HTTPException(405, f"User already exists with username '{new_user.username}'")

    plaintext_password, hashed_password = util.generate_password()

    new_user_dict = new_user.dict()
    new_user_dict.update(
        active=True,
        hashed_password=hashed_password
    )

    db.add(ClientUser(**new_user_dict))
    db.commit()

    return {
        'user': ClientUserSchema.from_orm(db.get(ClientUser, new_user.username)),
        'password': plaintext_password
    }


@router.post('/users/{username}/reset_password')
def reset_password(db: DbSession, user: UserExisting):
    plaintext_password, hashed_password = util.generate_password()
    user.hashed_password = hashed_password

    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        'user': ClientUserSchema.from_orm(user),
        'password': plaintext_password
    }


@router.post('/users/{username}/set_password')
def set_password(db: DbSession, user: UserExisting, new_password: str):
    user.hashed_password = util.hash_password(new_password)

    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        'user': ClientUserSchema.from_orm(user),
        'password': new_password
    }


@router.get('/users/{username}/projects')
def user_projects(user: UserExisting):
    # todo: add offset & limit params for user projects
    return {
            'feed_projects': [{
                'project_user_entry': p.to_dict(),
                'project': p.project.to_dict()
            } for p in sorted(user.feed_projects, key=lambda p: p.time_recommended, reverse=True)]
        }


@router.get('/users/{username}/projects/{project_uuid}')
def user_project(db: DbSession, user: UserExisting, project_uuid: str) -> ProjectUserSchema:
    published = db.get(UserProjectAssociation, (user.username, project_uuid))

    if not published:
        raise HTTPException(404, f'project {project_uuid} is not published for user {user.username}')

    return published

# @router.route('/users/{client_username}/project_entry/{project_uuid}', methods=['GET'])


# todo: do we need this? maybe return user stats here i.e. which orgs and users
# @router.get('/projects/{project_uuid}')


@router.post('/projects/publish/{client_org_id}', openapi_extra={
    "responses": {
        "200": {
            "content": {
                "application/json": {
                    "example": {
                        "published": ["project_uuid_1", "project_uuid_2"],
                        "errors": {"project_uuid_3": "error message"}
                    },
                    "schema": {
                        'type': 'object',
                        'required': ['published', 'errors'],
                        'properties': {
                            'published': {'type': 'array', 'items': {'type': 'string'}},
                            'errors': {'type': 'object', 'error': {'type': 'object'}}
                        }
                    }
                }
            }
        }
    }
})
def feed_project_add(db: DbSession, project_uuids: list[UUID], client_org: OrgExisting):
    """
    Publish projects with given UUIDSs to all users in organization

    :param client_org: organization to publish projects to
    :param project_uuids: list of project UUIDs to publish

    :return: dict with keys 'published' and 'errors' containing lists of published project UUIDs and errors respectively
    response example:
    """
    published = []
    errors = {}

    for uuid in project_uuids:
        try:
            published_project = publish_project(project_uuid=uuid, require_details_fields=False)
        except (PublishingError, ValidationError) as e:
            errors[uuid] = str(e)
            continue

        published.append(uuid)

        db.add(published_project)
        db.refresh(published_project)


        feed_entries = []
        for u in client_org.users:
            user_feed_entry, exists = get_one_or_create(db, UserProjectAssociation,
                                                            username=u.username,
                                                            project_id=uuid,
                                                            create_method_kwargs={
                                                                'time_recommended': util.utc_now()
                                                    })

            if exists and user_feed_entry.revoked:
                user_feed_entry.revoked = False
                user_feed_entry.time_recommended = datetime.now(tz=pytz.UTC)

            feed_entries.append(user_feed_entry)

        db.add_all(feed_entries)
        db.commit()

    return {'published': published, 'errors': errors}


@router.post('/projects/revoke/{client_org_id}', status_code=204)
def feed_project_revoke(db: DbSession, project_uuid: Annotated[UUID, Body()], client_org: OrgExisting):
    """
    Revoke project from all users in organization
    """
    feed_entries = db.scalars(select(UserProjectAssociation)
                    .filter(
                            UserProjectAssociation.project_id == project_uuid,
                            UserProjectAssociation.user.has(ClientUser.organization == client_org),
                        )
                    ).all()

    for feed_entry in feed_entries:
        feed_entry.revoked = True

    db.add_all(feed_entries)
    db.commit()


# @router.get('/api/v1/public/clients')
def api_public_clients(db: DbSession, pagination: PaginationParams):
    clients = s.query(ClientUser).filter(ClientUser.active==True,
                                            ClientUser.organization.has(ClientOrganization.membership!='free')).all()

    return jsonify({
        'clients': [c.to_dict() for c in clients]
    })
