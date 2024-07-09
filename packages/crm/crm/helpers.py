import re
from collections import defaultdict
from collections.abc import Iterable

from sqlalchemy import select
from pydantic import BaseModel

from loguru import logger

from crm.dependencies import QueryParams
from crm.filters import prepare_query


def query_model(db, model, query: QueryParams, *, query_field: str | None,
                order_by = None, filters: list | None = []):
    orm_query = select(model)

    if filters:
        orm_query = orm_query.where(*filters)

    if query.q and query_field:
        orm_query = orm_query.where(
            getattr(model, query_field).ilike(prepare_query(query.q))
    )

    if order_by is not None:
        if isinstance(order_by, Iterable):
            orm_query = orm_query.order_by(*order_by)
        else:
            orm_query = orm_query.order_by(order_by)
    else:
        order_by = getattr(model, query_field) if query_field else model.id

    res = db.scalars(
        orm_query.offset(query.offset).limit(query.limit)
    ).all()

    return res


def patch_object(model, patch_schema: BaseModel | dict):
    if isinstance(patch_schema, BaseModel):
        patch_schema = patch_schema.dict(exclude_unset=True)

    for field, value in patch_schema.items():
        if value is not None:
            setattr(model, field, value)

    return model


def lookup_obj(db,
               orm_class,
               query_string: str,
               query_field: str,
               limit: int = 5
):
    limit = min(limit, 10)
    allowlist = r'[^a-zA-Z0-9\.]' if query_field == 'website' else r'[^a-zA-Z0-9]'

    query_lower = query_string.strip().lower()
    query_cleaned = re.sub(rf'{allowlist}', '%', query_lower)
    query_final = f'%{query_cleaned}%'

    logger.debug(f'searching for {orm_class.__name__} by value of {query_field}={query_final}'
                 f'(raw query "{query_string}")')

    # search by field value
    res = db.scalars(select(orm_class).where(
        getattr(orm_class, query_field).ilike(query_final)).limit(limit)).all()

    logger.debug(f'found {len(res)} results for query {query_field}={query_final}')

    return res


def group_by(objects, on_field: str, default: str):
    groups = defaultdict(set)

    for o in objects:
        key = getattr(o, on_field) or default # 'No type'
        groups[key].add(o)

    return groups
