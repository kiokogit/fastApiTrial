import datetime
from uuid import UUID
from collections import defaultdict
from pprint import pprint

from loguru import logger

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select

from arbm_core.private.projects import TrackedProject
from arbm_core.private.investors import Fund
from arbm_core.public.projects import Project, UserProjectAssociation

from dependencies import LoggedInUser, PrivateSession
from schemas.funds import FundSchema
from schemas.feed import ProjectEntry, SafeProjectList
from utils import convert_timeline, get_user_feed_projects, \
                                    get_project_feedback, get_user_project, \
                                    log_user_event
from notifications import notify_feedback


router = APIRouter()


@router.get('/favourites')
def favourites(current_user: LoggedInUser) -> SafeProjectList:
    return get_user_feed_projects(current_user, favourite=True)


@router.get('/{project_id}')
def projects(project_id: UUID, current_user: LoggedInUser, s: PrivateSession) -> ProjectEntry:
    p = s.get(Project, project_id)
    project_user = s.get(UserProjectAssociation, (current_user.username, project_id))

    return {
            'project': p,
            'project_user_info': project_user,
        }


@router.get('/{project_id}/comments')
def projects_comments(request: Request, project_id: UUID, current_user: LoggedInUser, s: PrivateSession):
    p = get_user_project(s, project_id, current_user)

    if not p.feedback:
        raise HTTPException(status_code=200, detail='team comments can only be viewed after the user submits feedback')

    all_feedback = get_project_feedback(s, current_user, user_project=p)

    log_user_event(user=current_user,
        event=request.url.path,
        details={
                'ip': request.client,
                'project_uuid': str(project_id),
                'action': 'comments',
                'comments': all_feedback,
        })

    return {
            'comments': all_feedback,
        }


@router.post('/{project_id}/favourite')
def favourite(request: Request, project_id: UUID, current_user: LoggedInUser, s: PrivateSession):
    p = get_user_project(s, project_id, current_user, raise_if_none=None)

    if p is None:
        p = UserProjectAssociation(username=current_user.username, project_id=project_id)

    p.favourite = not p.favourite

    log_user_event(user=current_user,
            event=request.url.path,
            details={
                    'ip': request.client,
                    'project_uuid': str(project_id),
                    'action': 'favourite' if p.favourite else 'unfavourite',
                    'is_favourite': p.favourite,
            })

    s.add(p)
    s.commit()

    return {'status': 'success', 'favourite': p.favourite}


@router.post('/{project_id}/contacted')
def contacted(request: Request, project_id: UUID, current_user: LoggedInUser, s: PrivateSession):
    p = get_user_project(s, project_id, current_user)

    p.contacted = not p.contacted

    log_user_event(user=current_user,
            event=request.url.path,
            details={
                    'ip': request.client,
                    'project_uuid': str(project_id),
                    'action': 'contacted' if p.contacted else 'not contacted',
                    'is_contacted': p.contacted,
            })

    s.add(p)
    s.commit()

    return {'status': 'success', 'contacted': p.contacted}


@router.post('/{project_id}/rating')
def rating(request: Request, project_id: UUID, rating: int, current_user: LoggedInUser, s: PrivateSession):
    if not 0 <= rating <= 3:
        return {'status': 'error', 'msg': 'rating value must be between 0 and 3'}

    log_user_event(user=current_user,
                event=request.url.path,
                details={
                        'ip': request.client,
                        'project_uuid': str(project_id),
                        'rating': rating,
                })

    p = get_user_project(s, project_id, current_user)

    p.rating = rating if rating > 0 else None  # set to null if 0 value is provided (i.e. rating not set)
    s.add(p)
    s.commit()
    return {'status': 'success', 'rating': p.rating}


@router.post('/{project_id}/feedback')
def feedback(request: Request, project_id: UUID, feedback: str, current_user: LoggedInUser, s: PrivateSession):
    log_user_event(user=current_user,
                event=request.url.path,
                details={
                        'ip': request.client,
                        'project_uuid': str(project_id),
                        'feedback': feedback,
                })

    p = get_user_project(s, project_id, current_user)

    if not len(feedback) > 0:
        raise HTTPException(status_code=400, detail="Feedback must have at least 1 character")

    p.feedback = feedback
    s.add(p)
    s.commit()

    notify_feedback(s, p)

    return {'status': 'success', 'feedback': p.feedback}


@router.post('/{project_id}/timeline')
def timeline(request: Request, project_id: UUID, current_user: LoggedInUser, s: PrivateSession, detailed: bool = False):
    log_user_event(user=current_user,
                event=request.url.path,
                details={
                        'ip': request.client,
                        'project_uuid': str(project_id),
                })
    p = s.scalars(select(TrackedProject).filter_by(uuid=project_id)).one()

    p: TrackedProject = s.scalars(select(TrackedProject).where(TrackedProject.uuid == project_id)).one_or_none()
    if not p:
        raise HTTPException(404, f'project not found with uuid {project_id}')

    timeline = p.signals
    if not timeline:
        timeline = convert_timeline(p.compute_timeline())

    for signal in timeline:
        signal['signal_date'] = datetime.date(year=signal['_id']['timeframe']['year'],
                                             month=signal['_id']['timeframe']['month'],
                                             day=1)
        f = s.scalars(select(Fund).where(Fund.uuid == signal['_id']['fund_uuid'])).one_or_none()
        if f is None:
            logger.error(f'fund with uuid {signal["fund_uuid"]} wasn\'t found')
            continue

        signal['fund'] = FundSchema.from_orm(f)

        del signal['_id']

    timeline = sorted(timeline, key=lambda x: x['signal_date'], reverse=True)

    # old code
    # load each fund into a model
    if detailed:
        by_year = {}

        for signal in timeline:
            if signal['signal_date'].year not  in by_year:
                by_year[signal['signal_date'].year] = {'total': 0, 'count_direct': 0, 'count_indirect': 0, 'months': {}}

            if signal['signal_date'].month not in by_year[signal['signal_date'].year]['months']:
                by_year[signal['signal_date'].year]['months'][signal['signal_date'].month] = {'total': 0, 'count_direct': 0, 'count_indirect': 0, 'signals': []}

            by_year[signal['signal_date'].year]['months'][signal['signal_date'].month]['signals'].append(signal)

            by_year[signal['signal_date'].year]['total'] += signal['total']
            by_year[signal['signal_date'].year]['count_direct'] += signal['count_direct']
            by_year[signal['signal_date'].year]['count_indirect'] += signal['count_indirect']

            by_year[signal['signal_date'].year]['months'][signal['signal_date'].month]['total'] += signal['total']
            by_year[signal['signal_date'].year]['months'][signal['signal_date'].month]['count_direct'] += signal['count_direct']
            by_year[signal['signal_date'].year]['months'][signal['signal_date'].month]['count_indirect'] += signal['count_indirect']

        timeline = by_year

            # for month in year.get('months', []):
            #     funds_with_counts = []
            #     for signal in month.get('signals', []):
            #         f = s.scalars(select(Fund).where(Fund.uuid == signal['fund_uuid'])).one_or_none()

            #         if f is None:
            #             logger.error(f'fund with uuid {signal["fund_uuid"]} wasn\'t found')
            #             continue

            #         funds_with_counts.append({'fund': FundSchema.from_orm(f), 'count': signal['count']})

            #     month['signals'] = funds_with_counts

    #convert to list[{signal_date, fund}, ...] format
    # if not detailed:
    #     flattened = []

    #     # iterate over dict to get the dates and extract signal
    #     for year in timeline:
    #         for month in year.get('months', []):
    #             # ensure each funds appears only once per month
    #             uuids_in_month = set()
    #             funds_in_month = []
    #             for signal in month.get('signals', []):
    #                 if signal['fund'].uuid not in uuids_in_month:
    #                     uuids_in_month.add(signal['fund'].uuid)
    #                     funds_in_month.append(signal)

    #             year_month = datetime.date(year=year['year'], month=month['month'], day=1)

    #             # append monthly entries to the list
    #             flattened.extend([{
    #                 'signal_date': year_month,
    #                 'fund': f
    #             } for f in funds_in_month])

    #     timeline = flattened

    return {'status': 'success', 'timeline': timeline}


@router.get('/{project_id}/actions/contact')
def projects_actions(request: Request, project_id: UUID, current_user: LoggedInUser, s: PrivateSession):
    p = get_user_project(s, project_id, current_user)

    log_user_event(user=current_user,
            event=request.url.path,
            details={
                    'ip': request.client,
                    'project_uuid': str(project_id),
                    'action': 'contact',
            })

    if not p.project or not p.project.contacts:
        raise HTTPException(status_code=400, detail='could not get projects contacts list')

    return {
        'contacts': [
                {
                    'works_in_company': None,
                    'name': c.name,

                    'email': c.email,
                    'linkedin': c.linkedin,

                    'image': c.img,
                    'position': c.role,

                    'recommended': c.recommended,
                }
                for c in p.project.contacts
            ],
        }
