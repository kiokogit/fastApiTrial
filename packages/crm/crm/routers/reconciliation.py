from enum import Enum, auto

from loguru import logger
from fastapi import APIRouter

from arbm_core.private.projects import TrackedProject
from arbm_core.private.linkedin import LinkedinCompany
from arbm_core.private.investors import  Investor

from dependencies import DbSession

from projects import reconciliation


router = APIRouter()


class Reconciliatable(Enum):
    project = auto()
    investor = auto()
    linkedin_company = auto()


def get_model(object_type: Reconciliatable):
    match object_type:
        case Reconciliatable.project:
            return TrackedProject
        case Reconciliatable.investor:
            return Investor
        case Reconciliatable.linkedin_company:
            return LinkedinCompany
        case _:
            raise NotImplementedError(f'Reconciliation not implemented for object type: {object_type}')


@router.get('/reconciliation/{object_type}')
def reconciliation_view(db: DbSession, object_type: Reconciliatable):
    raise NotImplementedError
    duplicate_groups = reconciliation.get_duplicate_objects(db,
                                                            get_model(object_type),
                                                            'name')

    duplicates = []
    for i, group in enumerate(duplicate_groups[:10]):
        group['objects'] = [o.to_dict() for o in group['objects']]

        duplicates.append(group)

        logger.info(f'{i+1} / {len(duplicate_groups)} groups serialized')

    return


@router.post('/reconciliation/{object_type}')
def reconciliate(db: DbSession, object_type: Reconciliatable):
    raise NotImplementedError
    return reconciliation.reconciliate(db, get_model(object_type))
