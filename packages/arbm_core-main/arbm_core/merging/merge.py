from loguru import logger

from ..private.linkedin import LinkedinProfile

from ..private.investors import Investor

from ..private.linkedin import LinkedinPersonal


class MergingError(Exception):
    pass


def update_if_none(a, b, attr_name):
    logger.info(f'updating {attr_name}')

    a_attr = getattr(a, attr_name)
    b_attr = getattr(b, attr_name)

    none_attrs = [val is None for val in (a_attr, b_attr)]

    logger.info(f'a.{attr_name} = {a_attr},'
                f' b.{attr_name} = {b_attr}')

    logger.info(all([val is not None for val in (a_attr, b_attr)]))
    if all([val is not None for val in (a_attr, b_attr)]) and a_attr != b_attr:
        raise MergingError(f"both values for {attr_name} are set (a: {a_attr}, b: {b_attr}), merging cannot proceed!")

    if any(none_attrs):
        not_none = a_attr or b_attr
        setattr(a, attr_name, not_none)

    logger.info(f'a.{attr_name}={getattr(a, attr_name)}\n')


def extend_relationship(a, b, attr_name):
    logger.info(f'updating {attr_name}')

    logger.info(f'a.{attr_name}={getattr(a, attr_name, [])}')
    logger.info(f'b.{attr_name}={getattr(b, attr_name, [])}')

    a_relationships: list = getattr(a, attr_name, [])
    a_relationships.extend(getattr(b, attr_name, []))

    a_attr = "\n".join([str(v) for v in getattr(b, attr_name)])
    b_attr = "\n".join([str(v) for v in getattr(b, attr_name)])
    logger.info(f'a.{attr_name}: {a_attr},\n'
                f'b.{attr_name}: {b_attr}')

    setattr(a, attr_name, a_relationships)

    a_rel = "\n".join([str(v) for v in getattr(a, attr_name)])
    logger.info(f'extended a.{attr_name}: {a_rel}\n')


def merge_by_fields(objects: tuple, const_fields: list, relationship_fields: list, excluded_fields: list):
    a, b = objects

    all_fields =  const_fields + relationship_fields + excluded_fields
    if any([f not in a.__dict__ for f in all_fields]):
        raise ValueError('merge path must be specified for all fields in model')

    for attr_name in const_fields:
        update_if_none(a, b, attr_name)

    for attr_name in relationship_fields:
        extend_relationship(a, b, attr_name)


def _merge_linkedin_profile(a: LinkedinProfile, b: LinkedinProfile):
    raise NotImplemented
    for url in b.urls:
        a.urls.append(url)

    a.sourced_projects.extend(b.sourced_projects)
    b.sourced_projects = []

    print('a liked posts before: ', a.liked_posts)
    print('b liked posts before: ', b.liked_posts)

    a.liked_posts.extend(b.liked_posts)
    b.liked_posts = []

    print('a liked posts after: ', a.liked_posts)
    print('b liked posts after: ', b.liked_posts)

    a.mentions.extend(b.mentions)
    b.mentions = []


def merge_linkedin_personals(a: LinkedinPersonal, b: LinkedinPersonal):
    _merge_linkedin_profile(a, b)


def merge_investors(a: Investor, b: Investor):
    if a is None or b is None:
        raise ValueError(f'a: {a} or b: {b} is None')

    logger.info(f'mering a (id: {a.id}) and b (id: {b.id})')

    attrs_update_if_none = [
        'role',
        'investor_type',
        'type',

        'twitter_url',
        'linkedin_url',

        'linkedin_last_parsed',
        'twitter_followed',
    ]
    relationship_attrs = [
        'funds',

        'twitter_subscriptions',
        'linkedin_activity',
    ]

    merge_by_fields((a,b), attrs_update_if_none, relationship_attrs, ['_sa_instance_state'])
