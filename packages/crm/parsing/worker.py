import re

from celery import Celery
from sqlalchemy import select

from arbm_core.core import MongoDb
from arbm_core.private import Session
from arbm_core.private.investors import Fund

from api_external.iscraper import activity_details, profile_activity
from crm.routers.collections import load_collection


app = Celery('tasks', broker='redis://localhost:6379/')


def parse_activity(user_id, activity_id, titles_collection):
    activity = activity_details(activity_id=activity_id)

    reactions = activity['engagement']['reactions']

    for like in reactions:
        if not (liker := like['user']):
            continue

        # skip own likes
        if liker['profile_id'] == user_id:
            continue

        # filter likers for having a relevant position
        positions = list(load_collection(db=MongoDb, name=titles_collection).find(limit=100))
        pattern = '|'.join(['(?:\\b' + pos + '\\b)' for pos in positions])

        title = liker['title']

        if matched := re.match(pattern, title, re.IGNORECASE):
            print(title, 'matched position', matched)
            print(liker['profile_id'])
            print()

            profile_urn = f"https://linkedin.com/in/{liker['entity_urn']}/"
            print(f'https://www.linkedin.com/embed/feed/update/{item["activity_id"]}')

            # break
            with Session() as db:
                try:
                    like = LinkedinLike(
                        profile_url=profile_urn,
                        post_id=item,
                        liker_id=liker['profile_id'],
                        liker_name=liker['first_name'] + ' ' + liker['last_name'],
                    )
                    signal = InvestorSignalSchema(
                        post_url=f'https://www.linkedin.com/embed/feed/update/{item["activity_id"]}',
                        investor_url=f'https://www.linkedin.com/in/{user_id}/',
                        activity_type=item['activity_type'],
                        leader_url=validate_linkedin_url('https://www.' + remove_protocol(profile_urn)),
                        leader_name=liker['first_name'] + ' ' + liker['last_name'],
                    )

                    process_like(db, like, signal)
                except (HTTPError, ApiError, LinkedinEnrichError) as e:
                    e_str = e.to_dict() if isinstance(e, LinkedinEnrichError) else str(e)

                    log_event(db,
                            type=EventType.error,
                            module=MODULE_NAME,
                            event=e.__class__.__name__,
                            message={
                                'event': 'error',
                                'object': 'exception',
                                'exception': e_str,
                            }
                )

                continue


def parse_investor(profile_id):
    # get investor's activity via api
    # for every new item
    activity = profile_activity(profile_id=profile_id, per_page=50)

    # todo: get titles appropriate for the parsing type (startups / b2b)
    titles = []

    for item in activity:
        parse_activity(profile_id, item['activity_id'], titles)


@app.task
def parse_fund(fund_uuid):
    with Session() as s:
        fund = s.scalars(select(Fund).where(Fund.uuid == fund_uuid)).one()

        for investor in fund.investors:
            parse_investor(investor.profile_id)
