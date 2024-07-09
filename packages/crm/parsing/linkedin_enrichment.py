import json
import os
import sys
import time
from pprint import pformat, pprint
import time
import traceback
from typing import Any, Callable
import uuid

from pydantic import ValidationError

from sqlalchemy import select
from loguru import logger
from requests.exceptions import HTTPError

from arbm_core.private import Session
from arbm_core.private.queue import QueuedItem
from arbm_core.private.logging import EventType
from arbm_core.private.projects import ProjectStatus
from arbm_core.private.linkedin import LinkedinLike
from arbm_core.private.investors import Investor
from arbm_core.core.publishing import publish_project, PublishingError
from notifying.admin import notify_parsing_error
from projects import FilterPreconditionException, FilteringEvent, FilteringException, LogEvent, ProjectException

from projects.linkedin_utils import extract_project_data, load_profile_from_like, parse_company_data
from projects.linkedin_utils import fetch_cached
from projects.project_init import inject_project
from projects.schemas.linkedin import ProjectLinkedinDetailsSchema
from projects.schemas.project import ProjectData
from projects.schemas.signals import FundIdSchema, LinkedinPostSignal

import util
from openai.error import RateLimitError
from api_external import ApiError, iscraper
from api_external.iscraper import profile_company_details_v3
from parsing import LinkedinEnrichError, NoMatchesException
from parsing.content_queue import PersistentQueue
from projects import DuplicateProjectsError
from util import UrlParsingError, get_linkedin_id, log_event, utc_now
from projects.schemas.signals import LinkedinSourceSchema

MODULE_NAME = os.path.basename(__file__)

logger.remove()
logfile = util.project_root() / "logs/parsing" / "extension_parsing.log"
logger.add(sys.stderr,
           format="[Extension Parsing] {time} {name} {level}: {message}",
           level="TRACE")
# logger.add(sys.stdout, format="{time} {level} {message}", level="DEBUG")
logger.add(logfile.absolute(), rotation="00:00", format="[Extension Parsing] {time} {name} {level}: {message}",
           level="DEBUG", backtrace=True, diagnose=True)


QUEUE_MAX_PRIORITY = 15


def find_matching_profile(liker_name: str, liker_title: str, search_profiles: list[dict]) -> dict | None:
    matched_profile = None

    if len(search_profiles) > 0:
        for found_profile in search_profiles:
            if found_profile['title'] == liker_name:
                found_id = found_profile['universal_id']

                enriched_profile = iscraper.profile_details(profile_id=found_id)

                # check if the profile found matches the profile parsed from likes by title
                if liker_title == enriched_profile.get('sub_title', ''):
                    matched_profile = enriched_profile
                    break

                # wait before checking next result to not abuse api
                time.sleep(1)

    return matched_profile


def get_current_companies_urls(all_employments: list) -> list[str]:
    # get current employments
    current_employment_urls = []

    for employment in all_employments:
        employment_dates = employment.get('date')

        if employment_dates is None:
            logger.error('employment dates not found')
            continue

        end_date = employment_dates.get('end', {})
        company_url = util.nested_get(employment, 'company', 'url')

        if any(end_date.values()) or not company_url:
            continue

        current_employment_urls.append(company_url)

    return current_employment_urls


def get_profile_data(profile_id: str | None, profile_url: str, profile_name: str, profile_keyword: str, profile_title: str) -> dict:
    """
    Given linkedin like, try to get profile details from API by name and keyword
    :return: json dict of profile details from API
    """
    # lookup profile by url
    logger.debug(f'profile_id provided: {profile_id}')
    if not profile_id:
        profile_id = util.get_linkedin_id(profile_url)
        logger.debug(f'profile_id not found, using value from profile url: {profile_id}')

    profile_details = iscraper.profile_details_v3(profile_id=profile_id)

    if not profile_details:
        logger.error(f"could not get profile details by url or id '{profile_url} "
                     f"/ {profile_id}',"
                     f" trying to lookup profile by name/keywords")

        # search for profile by title and primary keyword, e.g. ceo, cto, founder, etc
        query = f"{profile_name} {profile_keyword}"
        res = iscraper.linkedin_search(keyword=query)
        found_profiles = res.get('results', [])

        # try to find result which matches the profile parsed from likes
        profile_details = find_matching_profile(profile_name, profile_title, found_profiles)

        if not profile_details:
            # no need to sleep as we've slept within the find function
            raise LinkedinEnrichError(f'could not find profile with url {profile_url}'
                                    f' or matching query "{query}" ({profile_name} - {profile_title})!')

    return profile_details


def enrich_companies(company_urls):
    companies_data = []

    for url in company_urls:
        try:
            company_id = util.get_linkedin_id(url, profile_type='company')
        except UrlParsingError:
            logger.error(f'error parsing linkedin id from url: "{url}"')
            continue

        companies_data.append(profile_company_details_v3(profile_id=company_id))

    return companies_data


def fetch_companies(s, profile_employments: list
                    ) -> list[dict]:
    """
    For every current employment, try to get company info from db, or use API to retrieve linkedin data,
    and filter for companies matching criteria
    :param s:
    :param profile_employments: dict of current employments for the profile
    :return:
    """
    current_employments = get_current_companies_urls(profile_employments)
    logger.debug(f'getting data for {len(current_employments)} current position(s)')

    enriched_companies = []
    uncached_companies = current_employments.copy()

    for company_url in current_employments:
        try:
            get_linkedin_id(company_url, profile_type='company')
        except UrlParsingError:
            logger.error(f'error parsing linkedin id from url: "{company_url}", skipping')
            continue

        db_company = fetch_cached(s, profile_type='company', max_age=60, company_url=company_url)

        if db_company:
            enriched_companies.append(db_company)
            uncached_companies.remove(company_url)

    logger.debug(f'using api to get data for {len(current_employments)} companies')
    companies_api_data = enrich_companies(uncached_companies)
    #logger.critical(f'got enriched company data: {pformat(companies_api_data)}')

    enriched_companies.extend(companies_api_data)

    return enriched_companies


def fetch_profile_data(s, like):
    # lookup profile in db by the profile url from like
    db_profile = fetch_cached(s, profile_type='personal', max_age=60,
                                leader_urls=[like.profile_url],
                                leader_ids=[like.liker_id])

    if db_profile and db_profile.get('position_groups'):
        return db_profile

    logger.debug('could not find cached employment data, fetching via api...')

    enriched_profile = get_profile_data(profile_id=like.liker_id,
                                        profile_url=like.profile_url,
                                        profile_name=like.liker_name,
                                        profile_keyword=like.liker_keyword,
                                        profile_title=like.liker_title)
    return enriched_profile


def update_linkedin_project(s, events_group, events_queue, project_data, linkedin_source, project_type):
    try:
        # logger.critical(pformat(linkedin_schema.dict(exclude_unset=True)))\
        if linkedin_source.signal.investing_entity.entity_type != 'investor':
            raise RuntimeError(f"unexpected investing_entity type encountered where 'investor' was expected: \
                                {linkedin_source.signal.investing_entity.entity_type}")

        investor: Investor = s.scalars(select(Investor)
                    .where(Investor.id == linkedin_source.signal.investing_entity.id)
        ).unique().one()

        signals = [FundIdSchema(id=f.id) for f in investor.funds]

        project = inject_project(s,
                                    signals=signals,
                                    project_data=project_data,
                                    source=linkedin_source,
                                    events_group=events_group,
                                    events_queue=events_queue,
                                    project_type=project_type,
                                )

        # if the project is accepted, update information in public database
        if project.status == ProjectStatus.accepted:
            publish_project(project_uuid=project.uuid)

    except (FilteringException, ProjectException) as e:
        msg = 'Project did not pass filtering'
        logger.error(f'{msg}: {e}')

        events_queue.append(e.cause)

    except FilterPreconditionException as e:
        event = FilteringEvent(
            group_id = events_group,
            event_name='filtering_failed',
            display_name='Filter pre-condition failed',
            details=e.args[0],
            data=[e.inputs] if e.inputs else []
        )

        msg = 'Filter pre-condition failed'
        logger.error(f'{msg}: {e}')
        events_queue.append(event)
    except DuplicateProjectsError as e:
        logger.error(f'found multiple projects with identity {linkedin_source.linkedin_details.title}' \
                        f' {linkedin_source.linkedin_details.website} ({linkedin_source.linkedin_details.linkedin_url})')
        log_event(s,
                    type=EventType.error,
                    module=MODULE_NAME,
                    event='CaughtException',
                    message={
                        'class': e.__class__.__name__,
                        'exception': str(e),
                    }
                    )
    except PublishingError as e:
        logger.error(f'Failed to publish project. Details: {e}')


def process_like(s, like: LinkedinLike, linkedin_signal: LinkedinPostSignal, project_type: str):
    logger.debug(f"Processing projects " \
                f"from profile {like.liker_name} ({like.profile_url})...")

    profile_data = fetch_profile_data(s, like)
    if not (profile_employments := profile_data.get('position_groups')):
        raise LinkedinEnrichError(f"profile employments not found"
                                  f" for profile {like.profile_url}")

    # get matching companies from the profile, if any
    companies_data = fetch_companies(s, profile_employments)

    profile_events: list[LogEvent] = []

    for company_json in companies_data:
        linkedin_schema: ProjectLinkedinDetailsSchema = parse_company_data(company_json)

        try:
            project_data: ProjectData = extract_project_data(linkedin_schema)
        except ValidationError as e:
            log_event(s,
                        type=EventType.error,
                        module=MODULE_NAME,
                        event='LinkedinEnrichError',
                        message={
                            'display_name': 'Linkedin data failed validation',
                            'event_name': 'validation_failed',
                            'data': {
                                'project_data': linkedin_schema.dict(),
                                'errors': e.errors()
                            }
                        }
                    )
            continue
        # except LinkedinEnrichError as e:
        #     logger.error(f'LinkedinEnrichError occured: {pformat(e.to_dict())}')

        #     log_event(s,
        #                 type=EventType.error,
        #                 module=MODULE_NAME,
        #                 event='LinkedinEnrichError',
        #                 message={
        #                     'event': 'error',
        #                     'object': 'exception',
        #                     'exception': e.to_dict(),
        #                 }
        #             )
        #     continue

        events_group = uuid.uuid4()

        linkedin_source = LinkedinSourceSchema(
            signal=linkedin_signal,
            company_url=linkedin_schema.linkedin_url,
            linkedin_details=linkedin_schema,
        )

        update_linkedin_project(s, events_group, profile_events, project_data,
                                linkedin_source, project_type)

    for event in profile_events:
        event_category = 'FilteringEvent' if isinstance(event, FilteringEvent) else 'ProjectEvent'
        log_event(s,
                    type=EventType.info,
                    module=MODULE_NAME,
                    event=event_category,
                    message=event.dict()
                )

    # passed = [r for r in profile_events if r.passed]
    # logger.debug(f"filtered profile companies: " \
    #              f"{len(passed)}/{len(profile_events)} passed")

    # if passed:
    #     log_event(s,
    #                 type=EventType.info,
    #                 module=MODULE_NAME,
    #                 event='CompaniesMatched',
    #                 message={
    #                     'results': profile_events
    #                 }
    #     )

    #     # if profile has matched companies, store it in the database
    #     profile = load_profile_from_like(s, like, profile_data)
    #     s.add(profile)
    #     s.commit()
    # else:
    #     log_event(s,
    #                 type=EventType.info,
    #                 module=MODULE_NAME,
    #                 event='NoMatches',
    #                 message={
    #                     'results': profile_events
    #                 }
    #     )


class LinkedinEnrichment:
    def __init__(self, queue: PersistentQueue, parsing_type):
        self._limit_per_second = 10
        self.queue: PersistentQueue = queue
        self.parsing_type = parsing_type

    def main(self):
        logger.info("Processing queued like signals")

        with Session() as s:
            # priority limits the max number of times like will be tried to parse
            # since every failed attempt increases the priority by 1

            while next_items := self.queue.peek(1, priority=QUEUE_MAX_PRIORITY):
                queue_item = next_items[0]
                like_pk = queue_item.object_key.split(';=;=;')

                logger.info(f"Got queued like with key: {like_pk},"
                            f"priority {queue_item.priority}")

                try:
                    like = s.get(LinkedinLike, like_pk)

                    if not like:
                        raise LinkedinEnrichError(f"like not found for profile \
                                                  {queue_item.object_key}")

                    # logger.critical(pformat(json.loads(queue_item.data)))

                    raw_signal = json.loads(queue_item.data)
                    if not raw_signal.get('picked_up_date'):
                        raw_signal['picked_up_date'] = queue_item.time_queued.date()

                    signal = LinkedinPostSignal(**raw_signal)
                    process_like(s, like, signal, project_type=parsing_type)
                except HTTPError as e:
                    if e.response.status_code == 502:
                        logger.error('502 error, sleeping for 10 seconds before retrying')
                        time.sleep(10)
                        continue

                    queue_item.priority += 5
                    s.add(queue_item)
                    s.commit()

                    e_str = e.to_dict() if isinstance(e, LinkedinEnrichError) else str(e)

                    log_event(s,
                              type=EventType.error,
                              module=MODULE_NAME,
                              event='CaughtException',
                              message={
                                  'class': e.__class__.__name__,
                                  'exception': e_str,
                              }
                    )
                    continue

                except (ApiError, LinkedinEnrichError) as e:
                    queue_item.priority += 1
                    s.add(queue_item)
                    s.commit()

                    e_str = e.to_dict() if isinstance(e, LinkedinEnrichError) else str(e)

                    log_event(s,
                              type=EventType.error,
                              module=MODULE_NAME,
                              event='CaughtException',
                              message={
                                  'class': e.__class__.__name__,
                                  'exception': e_str,
                              }
                   )

                    continue
                except RateLimitError as e:
                    log_event(s,
                              type=EventType.error,
                              module=MODULE_NAME,
                              event='CaughtException',
                              message={
                                  'class': e.__class__.__name__,
                                  'exception': str(e),
                              }
                   )

                    raise e
                except Exception as e:
                    s.rollback()

                    queue_item.priority += 3
                    s.add(queue_item)
                    s.commit()

                    logger.critical(f'unexpected exception occured')
                    logger.critical(e)

                    log_event(s,
                              type=EventType.error,
                              module=MODULE_NAME,
                              event='CaughtException',
                              message={
                                  'class': e.__class__.__name__,
                                  'exception': str(e),
                              }
                   )

                    continue

                self.queue.remove(queue_item.object_key)

                like.processed = True
                like.date_processed = utc_now()
                s.add(like)

                s.commit()
                time.sleep(1)

            logger.info("No more likes in queue, finishing")


def add_to_queue(queue: list):
    queue.append(1)


if __name__ == '__main__':
    if not len(sys.argv) > 2:
        print('Usage: python3 linkedin_enrichment.py <startup|competitors>')

    parsing_type = sys.argv[1]
    print(parsing_type)

    match parsing_type:
        case 'startup':
            queue_id = 'linkedin_likes_enrich_v2'
        case 'b2b':
            queue_id = 'competitors_mapping'
        case _:
            raise ValueError('Unsupported parsing category')

    # parse oldest likes first with fifo
    leader_queue = PersistentQueue(queue_id, delete_on_pop=False, mode='fifo')

    try:
        lookup = LinkedinEnrichment(leader_queue, parsing_type=parsing_type)
        lookup.main()
    except Exception as e:
        logger.critical(f'Unexpected exception occured: {e}')
        logger.critical(e)
        notify_parsing_error(header=f'Unexected error occured while parsing for type "{parsing_type}"',
                             error=traceback.format_exc())
