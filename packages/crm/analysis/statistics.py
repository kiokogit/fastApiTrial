import datetime

from arbm_core.private import Session
from arbm_core.private.projects import (
    DetailsEntry,
    FieldConfig,
    ProjectAnalytics,
    ProjectStatus,
    ProjectTagsAssociation,
    TrackedProject,
)
from loguru import logger
from sqlalchemy import and_, desc, distinct, func, or_, select, intersect

active_projects_filter = [
    TrackedProject.status == ProjectStatus.accepted,
    TrackedProject.analytics != None
]


def get_projects_fully_tagged(s, tag_names, details_names, filter_statuses: list | None = None):
    status_filter = True

    if filter_statuses:
        status_filter = TrackedProject.status.in_(filter_statuses)

    stmt = select(ProjectTagsAssociation.project_id)\
        .join(TrackedProject, ProjectTagsAssociation.project_id == TrackedProject.id)\
        .where(status_filter)\
        .where(ProjectTagsAssociation.tag_type.in_(tag_names))\
        .group_by(ProjectTagsAssociation.project_id)\
        .having(func.count(distinct(ProjectTagsAssociation.tag_type)) == len(tag_names))
    q_tags = s.scalars(stmt).all()

    q_details = s.scalars(select(DetailsEntry.project_id)
        .join(TrackedProject, TrackedProject.id == DetailsEntry.project_id)
        .where(status_filter)
        .where(DetailsEntry.type.in_(details_names))
        .group_by(DetailsEntry.project_id)
        .having(func.count(distinct(DetailsEntry.type)) == len(details_names))
    ).all()

    q = s.query(func.count(TrackedProject.id)).filter(TrackedProject.id.in_(q_tags), TrackedProject.id.in_(q_details)).all()

    return q


def get_tag_statistics(filter_statuses: list, tags: list | None):
    with Session() as s:
        accepted_project_ids = s.scalars(select(TrackedProject.id).filter(TrackedProject.status.in_(filter_statuses))).all()

        enabled_tags = tags or s.scalars(select(FieldConfig.field_name).filter(FieldConfig.enabled == True)).all()

        def get_datapoint_filters(datapoint_model, type_field: str):
            return [
                getattr(datapoint_model, type_field).in_(enabled_tags),
                datapoint_model.project_id.in_(accepted_project_ids),
                datapoint_model.effective_dates.contains(datetime.date.today())
            ]

        details_stats = s.execute(select(DetailsEntry.type, TrackedProject.status,
                                         func.count(func.distinct(DetailsEntry.project_id))
                                ).join(TrackedProject, TrackedProject.id == DetailsEntry.project_id).filter(
                                    *get_datapoint_filters(DetailsEntry, 'type')
                                ).group_by(DetailsEntry.type, TrackedProject.status)
                                .order_by(DetailsEntry.type, desc(TrackedProject.status))
                        ).all()

        tags_stats = s.execute(select(ProjectTagsAssociation.tag_type,
                                      TrackedProject.status,
                                      func.count(func.distinct(ProjectTagsAssociation.project_id)),
                                ).join(TrackedProject, TrackedProject.id == ProjectTagsAssociation.project_id)
                                .filter(
                                    *get_datapoint_filters(ProjectTagsAssociation, 'tag_type')
                                ).group_by(ProjectTagsAssociation.tag_type, TrackedProject.status)
                                .order_by(ProjectTagsAssociation.tag_type, desc(TrackedProject.status))
                        ).all()

        return dict(
            enabled_entries=enabled_tags,
            tagged_details=[tuple(el) for el in details_stats],
            tagged_tags=[tuple(el) for el in tags_stats],
            # tagged_tags=[tuple(el) for el in tags_stats],
        )


def get_untagged_projects(s, tag_names: list[str], detail_names: list[str]):
    tag_filters = []
    detail_filters = []

    for tag_name in tag_names:
        tag_filters.append(ProjectAnalytics.tags.any(ProjectTagsAssociation.tag_type == tag_name))
    for detail_name in detail_names:
        detail_filters.append(ProjectAnalytics.details.any(DetailsEntry.type == detail_name))

    q = s.query(TrackedProject) \
        .filter(TrackedProject.analytics != None) \
        .filter(
        or_(
            ~TrackedProject.analytics.has(and_(*tag_filters)),
            ~TrackedProject.analytics.has(and_(*detail_filters))
        )
    ).filter(TrackedProject.status.in_([ProjectStatus.accepted, ProjectStatus.review, ProjectStatus.published, ProjectStatus.discovered]))\
            .order_by(TrackedProject.status.desc())

    logger.info(f'found {q.count()} untagged projects')

    untagged_projects = q.limit(100).all()
    logger.info(f'processing {len(untagged_projects)} untagged projects')
    logger.info(f'project status: {untagged_projects[0].status}')

    untagged_projects_dict = {}

    for p in untagged_projects:
        p_tags = p.analytics.tags or []
        p_details = p.analytics.details or []

        # print(', '.join(set([t.type for t in p.analytics.tags] or ['No tags'])))
        # print(p.title, p.uuid)

        untagged_projects_dict[p.uuid] = {
            # 'project': p,
            'parsed_tags': list(set(tag_names).intersection(set([t.tag_type for t in p_tags]))),
            'parsed_details': list(set(detail_names).intersection(set([d.type for d in p_details]))),
        }

        # print(f'missing tags: {set(tag_names).difference(set([t.tag_type for t in p_tags]))}')
        # print(f'missing details: {set(detail_names).difference(set([d.type for d in p_details]))}')
        # print()

    return untagged_projects_dict


def get_new_projects(s, threshold: datetime.datetime, project_type: str | None = None):
    q = select(func.count('*')).where(TrackedProject.status_changed > threshold)
    match project_type:
        case 'startup':
            q = q.where(TrackedProject.is_startup == True)
        case 'b2b':
            q = q.where(TrackedProject.is_b2b == True)
        case None:
            ...

    new_projects_count = s.scalars(q).all()
    return new_projects_count
