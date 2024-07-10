from loguru import logger

from .merge import extend_relationship
from ..private.projects import TrackedProject, ProjectAnalytics


def merge_analytics(a: ProjectAnalytics, b: ProjectAnalytics):
    raise NotImplemented
    relationship_attrs = [
        'leaders',

        'categories',
        'tags',
        'details',
    ]
    for attr_name in relationship_attrs:
        extend_relationship(a, b, attr_name)


def merge_projects(a: TrackedProject, b: TrackedProject):
    raise NotImplemented
    if a is None or b is None:
        raise ValueError(f'a: {a} or b: {b} is None')

    logger.info(f'merging a (id: {a.id}) and b (id: {b.id})')

    attrs_update_if_none = [
        'website',
        'investor_type',
        'type',

        'twitter_url',
        'linkedin_url',

        'linkedin_last_parsed',
        'twitter_followed',
    ]
    for attr_name in attrs_update_if_none:
        update_if_none(a, b, attr_name)

    relationship_attrs = [
        'links',
        'fund_signals',
    ]
    for attr_name in relationship_attrs:
        extend_relationship(a, b, attr_name)
