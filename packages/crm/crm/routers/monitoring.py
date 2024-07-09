from datetime import timedelta, datetime, time

from fastapi import APIRouter
from sqlalchemy import select, func, or_
import pytz

from arbm_core.private.queue import QueuedItem
from arbm_core.private.projects import TrackedProject, ProjectStatus
from arbm_core.private.linkedin import LinkedinLike
from arbm_core.private.logging import LogEntry

from analysis.statistics import get_projects_fully_tagged, get_tag_statistics, get_new_projects
from parsing.linkedin_enrichment import QUEUE_MAX_PRIORITY
from crm.dependencies import DbSession, PaginationParams
from util import utc_now


router = APIRouter()


statuses_with_tags = [ProjectStatus.accepted, ProjectStatus.review, ProjectStatus.published]

@router.get("/tags")
def get_tag_stats(db: DbSession):
    by_status = list(db.execute(select(TrackedProject.status, func.count(TrackedProject.id))
                                    .filter(TrackedProject.status.in_(statuses_with_tags))
                                    .group_by(TrackedProject.status)
                            ).all())

    total_published = sum([count for _, count in list(by_status)])

    n_fully_tagged = len(get_projects_fully_tagged(db, tag_names=['verticals',
                                                                   #'industries'
                                                                   ],
                                                       details_names=['description',
                                                                      #'summary'
                                                                      ],
                                                       filter_statuses=statuses_with_tags))

    stats = dict(
        total_accepted=total_published,
        by_status={
            status: count for status, count in by_status
        },
        projects_fully_tagged=n_fully_tagged,
        accepted_not_tagged=total_published-n_fully_tagged,
    )
    return {}
    return stats


@router.get("/tags/by_type")
def get_tagger_stats():
    return {}
    return get_tag_statistics(statuses_with_tags, tags=['description', 'summary', 'Description', 'verticals',
                                                        'competing_space', 'headline', 'product_types', 'company_types'])


@router.get("/parsing")
def get_parser_stats(db: DbSession) -> dict:
    parsing_types = ['linkedin_likes_enrich_v2', 'competitors_mapping']

    likes_queued_count = db.execute(select(func.count()).filter(LinkedinLike.processed == False)).scalar_one()
    likes_processed_count = db.execute(select(func.count()).filter(LinkedinLike.processed == True)).scalar_one()

    parser_stats = []
    for parsing_type in parsing_types:
        q_signals_in_queue = select([func.count('*')]).\
                        where(QueuedItem.object_type == parsing_type)

        n_signals_queued = db.execute(q_signals_in_queue.where(
                QueuedItem.popped == False, QueuedItem.priority <= QUEUE_MAX_PRIORITY
            )
        ).scalar_one()
        n_signals_processed = db.execute(
            q_signals_in_queue.where(QueuedItem.popped == True)
        ).scalar_one()
        signals_last_hour = db.execute(select(func.count()).filter(QueuedItem.object_type==parsing_type,
                                                QueuedItem.popped==True,
                                                QueuedItem.time_popped > utc_now() - timedelta(hours=1))
                                ).scalar_one()
        signals_10_min = db.execute(select(func.count()).filter(QueuedItem.object_type==parsing_type,
                                                QueuedItem.popped==True,
                                                QueuedItem.time_popped > utc_now() - timedelta(minutes=10))
                                ).scalar_one()

        project_type = 'b2b' if parsing_type == 'competitors_mapping' else 'startup'

        parser_stats.append({
            'new_projects': get_new_projects(db, datetime.combine(datetime.now(tz=pytz.UTC), time()), project_type=project_type),
            'type': parsing_type,
            'queued': n_signals_queued,
            'processed': n_signals_processed,
            'signals_last_hour': signals_last_hour,
            'signals_10_min': signals_10_min
        })

    q_likes_in_queue = select([func.count('*')])
    likes_queued_count = db.execute(q_likes_in_queue.where(LinkedinLike.processed == False)).scalar_one()
    likes_processed_count = db.execute(q_likes_in_queue.where(LinkedinLike.processed == True)).scalar_one()

    # n_logs = db.execute(select(func.count()) \
    #          .filter(LogEntry.module=='linkedin_enrichment.py')).scalar_one()

    # logs = db.scalars(select(LogEntry)\
    #         .filter(LogEntry.module=='linkedin_enrichment.py')\
    #         .order_by(LogEntry.timestamp.desc())\
    #         .offset(pagination.offset)\
    #         .limit(pagination.limit)
    #     ).all()

    return {
        'parsers': parser_stats,
        'signals_last_hour': parser_stats[0]['signals_last_hour'],

        'signals_queued': parser_stats[0]['queued'],
        'signals_processed': parser_stats[0]['processed'],

        'likes_queued_count': likes_queued_count,
        'likes_processed_count': likes_processed_count,

        # 'logs': [l.to_dict() for l in logs],
        # 'n_logs': n_logs,
        # 'page_size': pagination.limit,
    }


@router.get('/logs')
def parsing_extension_stats(db: DbSession, pagination: PaginationParams, filter_types: list[str] | None = None):
    query_logs_count = select(func.count())\
        .filter(LogEntry.module=='linkedin_enrichment.py')
    query_logs = select(LogEntry)\
        .filter(LogEntry.module=='linkedin_enrichment.py')\

    if filter_types:
        filter_by_types = or_([LogEntry.event == t for t in filter_types])

        query_logs = query_logs.filter(filter_by_types)
        query_logs_count = query_logs_count.filter(filter_by_types)

    n_logs = db.execute(
        query_logs_count
    ).scalar_one()

    logs = db.scalars(
        query_logs
        .order_by(LogEntry.timestamp.desc())\
        .offset(pagination.offset)\
        .limit(pagination.limit)
    ).all()

    event_types = db.scalars(select(LogEntry.event).distinct()).all()

    extension_stats = {
        'logs': [l.to_dict() for l in logs],
        'n_logs': n_logs,
        'page_size': pagination.limit,
        'event_types': event_types
    }

    return extension_stats
