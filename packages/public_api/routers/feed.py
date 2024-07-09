import os
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import desc, select, func

from loguru import logger

from arbm_core.public.projects import Project, UserProjectAssociation
from arbm_core.public.users import ClientUser
from arbm_core.core import MongoDb
from arbm_core.core.signals import get_signals_multiple_funds

from schemas.feed import Feed
from schemas.project import ProjectSchema
from dependencies import LoggedInUser, PrivateSession, QueryParams
from utils import get_project_feedback, get_user_feed_projects, get_user_project, log_user_event, \
        get_filter_options

router = APIRouter()


try:
    env_threshold = os.environ.get('RESURFACING_THRESHOLD', '15')
    if not env_threshold.isnumeric():
        raise ValueError()
    RESURFACING_THRESHOLD = int(env_threshold)
    if not (0 < RESURFACING_THRESHOLD < 60):
        raise ValueError()
except ValueError:
    RESURFACING_THRESHOLD = 15


@router.get("")
def feed(request: Request, current_user: LoggedInUser) -> Feed:
    log_user_event(user=current_user,
                event=request.url.path,
                details={'ip': request.client,})

    projects = get_user_feed_projects(current_user, archived=False)
    history_projects = get_user_feed_projects(current_user)

    return {'projects': projects, **get_filter_options(history_projects)}


@router.get("/history")
def feed_history(request: Request, current_user: LoggedInUser) -> Feed:
    projects = get_user_feed_projects(current_user)

    return {'projects': projects, **get_filter_options(projects)}


@router.get('/live')
def live_feed(
              current_user: LoggedInUser,
              session: PrivateSession,
              q: QueryParams) -> list:
    funds_in_feed = {f.uuid: f for f in current_user.organization.funds_portfolio}

    all_signals = get_signals_multiple_funds(MongoDb, fund_uuids=list(funds_in_feed.keys()))
    dealflow = all_signals[q.offset:q.offset+q.limit]
    dealflow_projects = [p['project_uuid'] for p in dealflow]

    projects = session.scalars(select(Project).where(Project.uuid.in_([s['project_uuid'] for s in dealflow]))).all()
    projects = sorted(projects, key=lambda p: dealflow_projects.index(p.uuid))

    signals = [{'project': p, 'funds': [funds_in_feed[f_uuid] for f_uuid in s['funds']]} for p, s in zip(projects, dealflow)]

    return signals

    # return {'projects': projects_data, **get_filter_options(projects_data)}


@router.get('/resurfaced')
def feed_resurfaced(request: Request, current_user: LoggedInUser, s: PrivateSession) -> Feed:
    raise HTTPException(501, 'not implemented')

    log_user_event(user=current_user,
                event=request.url.path,
                details={'ip': request.client,})

    recent_threshold = datetime.combine(datetime.now(),
                                        datetime.max.time()) - timedelta(days=RESURFACING_THRESHOLD)

    stmt = select(
                        UserProjectAssociation.project_id,
                    #    func.count(UserProjectAssociation.username).label("n_users"),
                        func.count(UserProjectAssociation.username).filter(UserProjectAssociation.rating == 3).label("great_ratings"),
                        func.count(UserProjectAssociation.username).filter(UserProjectAssociation.rating == 2).label("good_ratings"),
                        ).\
                    join(ClientUser).\
                    filter(ClientUser.organization_id==current_user.organization_id,
                            ClientUser.active==True,
                            #   UserProjectAssociation.rating > 1,
                                UserProjectAssociation.time_recommended <= recent_threshold
                            ).\
                    group_by(UserProjectAssociation.project_id).\
                    having(func.count(UserProjectAssociation.username).filter(UserProjectAssociation.rating == 2)
                            + func.count(UserProjectAssociation.username).filter(UserProjectAssociation.rating == 3) > 0).\
                    order_by(desc("great_ratings"), desc("good_ratings"))

    ids_rated_good = [r[0] for r in  s.execute(stmt).all()]

    candidate_projects = s.scalars(select(Project).filter(Project.uuid.in_(ids_rated_good))).unique().all()
    print(f'found {len(candidate_projects)} projects with good/great ratings')

    res = []
    for p in candidate_projects:
        recent_signal_count = 0

        timeline = generate_timeline(s, signals=p.compute_timeline() or [], group_attr='fund_uuid')
        for signal in reversed(timeline):
            if signal['signal_date'] >= recent_threshold.date():
                recent_signal_count += 1

        if not recent_signal_count:
            continue

        try:
            user_project = get_user_project(s, p.uuid, current_user=current_user)
        except Exception:
            continue

        feed_entry = {
            'project': p,
            'project_user_info': user_project,
            'comments': get_project_feedback(s, current_user, user_project=user_project)
        }

        res.append(feed_entry)

    return {'projects': res, **get_filter_options(res)}
