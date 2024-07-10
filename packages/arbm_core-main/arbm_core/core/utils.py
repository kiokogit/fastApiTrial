from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, NoResultFound


def get_one_or_create(session,
                      model,
                      create_method='',
                      create_method_kwargs=None,
                      filter_expression=None,
                      **kwargs):
    lookup_query = select(model)

    if filter_expression is not None:
        lookup_query = lookup_query.filter(filter_expression)

    if any(kwargs):
        lookup_query = lookup_query.filter_by(**kwargs)

    try:
        return session.scalars(lookup_query).unique().one(), True
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.flush()
            return created, False
        except IntegrityError:
            session.rollback()
            return session.scalars(lookup_query).unique().one(), True
