import time
from typing import Callable

import arbm_core.private as back_db
import arbm_core.private.projects as db_projects
from arbm_core.merging.merge import MergingError, merge_investors, merge_linkedin_personals
from arbm_core.merging.merge_projects import merge_linkedin_projects
from arbm_core.private.projects import ProjectStatus, TrackedProject
from loguru import logger
from sqlalchemy import and_, asc, func, or_, text


def get_duplicate_objects(s, orm_object, key_field: str, custom_filters: list[Callable[[list], bool]]):
    orm_field = getattr(orm_object, key_field)

    logger.info('getting duplicate fields')

    duplicate_fields = s.query(
                            func.count(orm_field), orm_field
                        ).filter(
                            orm_object.removed == False
                        ).group_by(
                            orm_field
                        ).having(
                            func.count(orm_field) > 1
                        ).order_by(asc(func.lower(orm_object.name))).all()

    logger.info('getting duplicate objects')

    duplicate_objects = s.query(orm_object) \
        .filter(orm_field.in_([field_val for cnt, field_val in duplicate_fields])) \
        .order_by(asc(getattr(orm_object, key_field))).order_by(asc(orm_object.id)).all()

    duplicate_groups = []
    existing_groups = set()
    if 'not_aliases' in orm_object.__dict__:
        for o in duplicate_objects:
            group = {
                key_field: (key := getattr(o, key_field)),
                'objects': (duplicates := [o] + [other for other in duplicate_objects
                                                 if getattr(other, key_field) == key
                                                 and other is not o
                                                 and not (other in o.not_aliases or o in other.not_aliases)]),
                'count': len(duplicates),
            }

            # if one of the filters returns false, skip this group
            if custom_filters:
                if not all([f(group['objects']) for f in custom_filters]):
                    continue

            if group['count'] > 1 and not frozenset(group['objects']) in existing_groups:
                existing_groups.add(frozenset(group['objects']))
                duplicate_groups.append(group)
    else:
        logger.info('non-alias is not defined')

        for i, (cnt, key) in enumerate(duplicate_fields):
            duplicate_groups.append({
                key_field: key,
                'count': cnt,
                'objects': [o for o in duplicate_objects if getattr(o, key_field) == key]
            })
            logger.info(f'{i+1} / {len(duplicate_fields)} groups rendered')

    return duplicate_groups


def mark_separate(s, objects):
    updated_objects = {}

    for i, t in enumerate(objects):
        updated_objects[t.id] = {
            'id': t.id,
            'action': 'mark_separate',
            'status': 'set_not_alias'
        }

        # list exhausted
        if i == len(objects) - 1:
            break

        t.not_aliases.extend(objects[i:])
        s.add(t)

    return updated_objects


def merge_objects(s, objects, merge_fn: Callable):
    updated_objects = {}

    t = objects.pop(0)
    updated_objects[t.id] = {
        'id': t.id,
        'action': 'merge',
        'status': 'retained'
    }

    while objects:
        b = objects.pop(0)

        # if merging failed along the way, stop merging
        try:
            merge_fn(t, b)
        except MergingError as e:
            updated_objects[b.id] = {
                'id': b.id,
                'action': 'merge',
                'status': 'failed',
                'target': t.id,
                'error': str(e),
            }
            break

        updated_objects[b.id] = {
            'id': b.id,
            'action': 'merge',
            'status': 'removed',
            'target': t.id,
        }

        b.removed = True

        s.add(b)
        s.add(t)

        s.commit()

    # update status for objects for which merge wasn't attempted
    for obj in objects:
        if obj.id not in updated_objects:
            updated_objects[obj.id] = {
                'id': obj.id,
                'action': 'merge',
                'status': 'skipped',
                'target': t.id
            }

    return updated_objects


def reconciliate(s, orm_model, data):
    action = data['action']
    object_ids = [int(idx) for idx in data['object_ids']]

    if len(object_ids) < 2:
        raise ValueError("at least two object ids must be supplied")

    logger.info(f'performing action {action} on objects with ids {object_ids}')
    target_objects: list[orm_model] = []
    for idx in object_ids:
        target_objects.append(s.get(orm_model, idx))

    if not len(target_objects) == len(object_ids):
        raise ValueError("one or more object ids were not found")

    updated_objects = {}

    if action == 'separate':
        updated_objects = mark_separate(s, target_objects)

    elif action == 'merge':
        match orm_model:
            case db_projects.Investor:
                updated_objects = merge_objects(s, target_objects, merge_investors)
            case db_projects.LinkedinPersonal:
                updated_objects = merge_objects(s, target_objects, merge_linkedin_personals)

    res = {
        'action': action,
        'updated_objects': updated_objects,
        'timestamp': time.time()
    }

    return jsonify(res)


def projects_match():
    with back_db.Session() as s:
        linkedin_projects = s.query(TrackedProject).filter(and_(TrackedProject.source == 'linkedin',
                                                                TrackedProject.twitter == None,
                                                                TrackedProject.linkedin_profile != None)) \
            .filter(or_(TrackedProject.status == ProjectStatus.pending,
                        TrackedProject.status == ProjectStatus.accepted)).all()
        response = []

        for lp in linkedin_projects:

            # find by websites with levenshteib
            matching_website = []
            if lp.website:
                query = lp.website.lower().strip()
                stmt = text(
                    "SELECT *, levenshtein(lower(website), :q) FROM discovered_projects where source='twitter' ORDER BY levenshtein(lower(website), :q) ASC LIMIT 5;")
                res = s.execute(stmt, {"q": query})
                matching_website = list(res)

                if not matching_website[0][-1] < (threshold := 2):
                    matching_website = []
                else:
                    matching_website = [m for m in matching_website if m[-1] < threshold]

                # matching_website = s.query(TrackedProject)\
                #     .filter(and_(TrackedProject.twitter!=None, TrackedProject.linkedin_profile==None))\
                #     .filter(TrackedProject.website==lp.website).all()

            query = lp.title.lower()
            stmt = text(
                "SELECT *, levenshtein(lower(title), :q) FROM discovered_projects where source='twitter' and length(title) > 1 ORDER BY levenshtein(lower(title), :q) ASC LIMIT 5;")
            res = s.execute(stmt, {"q": query})
            matching_projects = list(res)

            logger.info(matching_projects[0])
            if not matching_projects[0][-1] < (threshold := 2):
                matching_projects = []
            else:
                matching_projects = [m for m in matching_projects if m[-1] < threshold]

            match = {
                'linkedin': lp.to_dict(),
                'twitter': [{'project': p[:-1], 'score': p[-1]} for p in matching_website]
                           + [{'project': p[:-1], 'score': p[-1]} for p in matching_projects],
                'min_distance': matching_projects[0][-1] if matching_projects else 0,
            }

            if match['twitter']:
                response.append(match)

        response = sorted(response, key=lambda x: x['min_distance'])

        return jsonify(response)
