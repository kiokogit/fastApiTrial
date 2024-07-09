import re
import os
from pprint import pprint

from requests import HTTPError

from arbm_core.private import Session
from arbm_core.private.projects import LinkedinLike
from arbm_core.private.logging import EventType
from api_external import ApiError

from api_external.iscraper import activity_details, profile_activity

from projects.schemas.signals import InvestorSignalSchema
from parsing import LinkedinEnrichError
from parsing.linkedin_enrichment import process_like
from util import log_event, validate_linkedin_url, remove_protocol

MODULE_NAME = os.path.basename(__file__)


def test_new_project():
    pass


def test_existing_project():
    pass


def test_repeat_like():
    pass


def test_project_match_thesis():
    pass


def get_user_activity(user_id):
    user_activity = profile_activity(profile_id=user_id, per_page=10)

    for item in user_activity['activities']:
        likes = item['social_stats']['num_likes']
        pprint(item['activity_type'] + ', ' + item['created_at'])
        pprint(str(likes) + ' likes')

        if likes < 100:
            activity = activity_details(activity_id = item['activity_id'])

            reactions = activity['engagement']['reactions']

            for like in reactions:
                liker = like['user']

                if not liker:
                    continue

                # skip own likes
                if liker['profile_id'] == user_id:
                    continue

                title = liker['title']
                positions = ('ceo', 'cto', 'cfo', 'coo', 'founder')
                pattern = '|'.join(['(?:\\b' + pos + '\\b)' for pos in positions])

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

        print('')

if __name__ == '__main__':
    get_user_activity('denys-gurak')