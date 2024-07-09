import datetime
import json
from pprint import pformat, pprint
import time
from bson import CodecOptions, UuidRepresentation

from loguru import logger

from pydantic import ValidationError
from requests import HTTPError
from sqlalchemy import select, func, desc, nulls_last, text

from bson.codec_options import TypeCodec, TypeRegistry

from arbm_core.core import MongoDb
from arbm_core.core.publishing import publish_project, PublishingError
from arbm_core.core.signals import YearMonth, AddSignal, save_signal, get_signals_for_project
from arbm_core.private import Session
from arbm_core.private.projects import TrackedProject, ProjectStatus
from arbm_core.private.investors import Fund, Investor
from arbm_core.private.linkedin import LinkedinInvestorActivityAssociation, LinkedinPost, LinkedinProfile, LinkedinCompany

from analysis import AnnotationError
from api_external.iscraper import profile_company_details_v3
from projects import FilterPreconditionException, FilteringEvent
from projects.linkedin_utils import extract_project_data, parse_company_data
from projects.project_filtering import filter_company, filter_is_startup, filter_signal
from projects.schemas.filters import FilterResult
from projects.schemas.linkedin import ProjectLinkedinDetailsSchema
from projects.schemas.project import ProjectData

from projects.schemas.signals import FundIdSchema, InvestorIdSchema, LinkedinPostSignal, LinkedinSourceSchema
from util import UrlParsingError, get_linkedin_id, elapsed_timer, utc_now


class DateCodec(TypeCodec):
    @property
    def python_type(self):
        # the Python type acted upon by this type codec
        return datetime.date
    @property
    def bson_type(self):
        # the BSON type acted upon by this type codec
        return str

    def transform_python(self, value):
        return value.strftime('%Y-%m-%d')

    def transform_bson(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()

date_codec = DateCodec()
type_registry = TypeRegistry([date_codec])


def get_logs_collection(prod: bool):
    codec_options = CodecOptions(type_registry=type_registry, uuid_representation=UuidRepresentation.STANDARD)
    if prod:
        return MongoDb.get_collection('migration_logs', codec_options=codec_options)
    else:
        return MongoDb.get_collection('test_migration_logs', codec_options=codec_options)


def log_migration_event(prod: bool, event: FilteringEvent):
    event_dict = event.dict()
    event_dict['timestamp'] = utc_now()

    if prod:
        get_logs_collection(prod).insert_one(event_dict)
    else:
        get_logs_collection(prod).insert_one(event_dict)


def get_fund_projects(s, fund, excluded_projects) -> tuple[list[int], set[int]]:
    founder_ids = set()

    for i, investor in enumerate(fund.investors):
        if len(investor.linkedin_activity) == 0:
            continue

        logger.debug(f'processing {i+1}th out of {len(fund.investors)} investors, {investor.name}, having {len(investor.linkedin_activity)} interactions')

        founders_query = text(f"""
                                select distinct id from linkedin_profiles
                                where id in (
                                    select liker_linkedin_id from linkedin_post_likers where linkedin_post_id in (
                                        select linkedin_post_id from linkedin_investor_activity_association where investor_id = {investor.id}
                                    )
                            )
                            """)
        founder_ids.update(s.scalars(founders_query).all())

        # if len(founder_ids) == 0:
            # continue

    # logger.info(f'{len(list(excluded_projects))} were already processed:')
    excluded = ','.join([str(p["project_id"]) for p in list(excluded_projects)])
    # logger.info(excluded)

    projects_query = f"""
                                            select distinct id from discovered_projects
                                                where id in (select tracked_project_id from linkedin_companies where id in(
                                                    select (company_profile_id) from linkedin_profile_projects
                                                        where source_profile_id in ({','.join([str(id) for id in founder_ids])})
                                                    )
                                                ) """
    if excluded != '':
        projects_query += f' and id not in ({excluded})'

    sourced_project_ids = s.scalars(text(projects_query)).all()

    return sourced_project_ids, founder_ids


def parse_linkedin_data(s, fund_project_ids, fund) -> list[tuple[int, ProjectLinkedinDetailsSchema]]:
    linkedin_profiles: list[tuple[int, ProjectLinkedinDetailsSchema]] = []
    failed = 0

    for i, project_id in enumerate(fund_project_ids):
        logger.info(f'processing project {i+1} out of {len(fund_project_ids)} with id {project_id}')
        project = s.get(TrackedProject, project_id)

        if project.linkedin_profile.raw_data is None:
            company_url = project.linkedin_profile.linkedin_url
            logger.info(f'project {project_id} with url {company_url} has no linkedin data, requesting through API...')

            try:
                company_id = get_linkedin_id(company_url, profile_type='company')
            except UrlParsingError:
                logger.error(f'failed to parse url from {company_url}, skipping...')
                continue

            try:
                project.linkedin_profile.raw_data = profile_company_details_v3(profile_id=company_id)
                s.add(project.linkedin_profile)
                s.commit()
                s.refresh(project.linkedin_profile)
            except HTTPError as e:
                logger.error(f'failed to get linkedin data for {company_url}, skipping...')
                logger.error(e)
                MongoDb.processed_projects.update_one({'project_id': project.id, 'fund_id': fund.id}, {'$set': {'status': 'HTTPError'}}, upsert=True)
                continue

        try:
            linkedin_schema: ProjectLinkedinDetailsSchema = parse_company_data(project.linkedin_profile.raw_data)
        except ValidationError as e:
            failed += 1
            logger.error(e)
            logger.error(pformat(project.linkedin_profile.raw_data))
            MongoDb.processed_projects.update_one({'project_id': project.id, 'fund_id': fund.id}, {'$set': {'status': 'validation_error'}}, upsert=True)
            continue

        linkedin_profiles.append((project_id, linkedin_schema))

        # logger.info(pformat(linkedin_schema.dict()))
    if failed > 0:
        logger.error(f'failed to parse {failed}/{len(fund_project_ids)} projects')
    else:
        logger.error(f'all projects parsed sucessfully')

    return linkedin_profiles


def get_project_sources(s, project: TrackedProject,
                        project_schema: ProjectLinkedinDetailsSchema,
                        fund) -> list[tuple[LinkedinSourceSchema, YearMonth]]:
    sources = []


    # select investor_id from investor_fund_association where fund_id = 234
    # print(f"""
    #                                 select a.investor_id, p.id as post_id, l.liker_linkedin_id from linkedin_posts p
    #                                 left join linkedin_investor_activity_association a on p.id = a.linkedin_post_id
    #                                 left join linkedin_post_likers l on l.linkedin_post_id = p.id
    #                                 where liker_linkedin_id in (select source_profile_id from linkedin_profile_projects
    #                                                                     where company_profile_id = (select id from linkedin_companies where tracked_project_id = {project.id})
    #                                                             )
    #                                 and a.investor_id in (select investor_id from investor_fund_association where fund_id = {fund.id});

    #                 """)

    interactions = s.execute(text(f"""
                                    select a.investor_id, p.id as post_id, l.liker_linkedin_id from linkedin_posts p
                                    left join linkedin_investor_activity_association a on p.id = a.linkedin_post_id
                                    left join linkedin_post_likers l on l.linkedin_post_id = p.id
                                    where liker_linkedin_id in (select source_profile_id from linkedin_profile_projects
                                                                        where company_profile_id in (select id from linkedin_companies where tracked_project_id = {project.id})
                                                                )
                                    and a.investor_id in (select investor_id from investor_fund_association where fund_id = {fund.id});

                    """)).all()

    # raise RuntimeError
    # for founder in project.linkedin_profile.source_profiles:
    #     for investor in fund.investors:
    #         founder_likes = select(linkedin_post_likers_table.c.linkedin_post_id).where(linkedin_post_likers_table.c.liker_linkedin_id == founder.id)

    #         q = select(LinkedinInvestorActivityAssociation)\
    #             .filter(LinkedinInvestorActivityAssociation.investor_id==investor.id,
    #                     LinkedinInvestorActivityAssociation.linkedin_post_id.in_(
    #                         founder_likes
    #                     ))

    #         common_posts: list[LinkedinInvestorActivityAssociation] = s.scalars(q).all()

    #         if common_posts:
    #             logger.info(f'found {len(common_posts)} signals between {investor.name} and {founder.name}')
    # print(interactions)
    for (investor_id, post_id, founder_id) in interactions:
        # print(investor_id, post_id, founder_id)

    # for investor_activity in common_posts:
        investor = s.get(Investor, investor_id)
        post = s.get(LinkedinPost, post_id)
        founder = s.get(LinkedinProfile, founder_id)
        print(investor.name, post.id, founder.name)

        linkedin_source = LinkedinSourceSchema(
            signal=LinkedinPostSignal(
                investing_entity=InvestorIdSchema(id=investor.id),
                picked_up_date=post.estimate_posted_date(),
                post_id=post.id,
                leader_id=founder.id,
            ),
            company_url=project_schema.linkedin_url,
            linkedin_details=project_schema,
        )
        sources.append((linkedin_source, YearMonth(year=post.estimate_posted_date().year,
                                                    month=post.estimate_posted_date().month)))

    return sources


def check_hard_filters(s, prod, fund, project_id, project_data):
    company_filters_result: FilterResult = filter_company(s, project_data)

    MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'filter_company_data_passed' if company_filters_result.passed else 'filter_company_data_failed'})

    if not company_filters_result.passed:
        log_migration_event(prod, FilteringEvent(
                                    group_id=fund.uuid,
                                    event_name='company_filter_failed',
                                    display_name='Company did not pass filters',
                                    data=[company_filters_result]
                            ))

    return company_filters_result.passed


def check_is_startup(s, prod, fund: Fund, project_id, project_data):
    try:
        is_startup = filter_is_startup(s, project_data)
    except FilterPreconditionException as e:
        logger.error('project filter failed')
        logger.error(e)
        MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'filter_prerequisites_failed'})
        return False
    except ValueError as e:
        logger.error('project filter failed')
        logger.error(e)
        MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'unexpected_filter_failure'})
        return False

    MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'filter_is_startup_passed' if is_startup.passed else 'filter_is_startup_failed'})


    if not is_startup.passed:
        log_migration_event(prod, FilteringEvent(
                group_id=fund.uuid,
                event_name='filter_is_startup_passed' if is_startup.passed else 'filter_is_startup_failed',
                display_name='Company is a valid startup' if is_startup.passed else 'Company is not a startup',
                data=[is_startup]
        ))

    return is_startup.passed


def check_thesis_matches(s, prod, fund: Fund, project_id, project_data):
    try:
        out = filter_signal(s, FundIdSchema(id=fund.id), project_data)
    except FilterPreconditionException as e:
        logger.error('project filter failed')
        logger.error(e)
        MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'filter_prerequisites_failed'})
        return False
    except (ValueError, AnnotationError) as e:
        logger.error('project filter failed')
        logger.error(e)
        MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'unexpected_filter_failure'})
        return False

    MongoDb.processed_projects.insert_one({'project_id': project_id,
                                                'fund_id': fund.id,
                                                'status': 'filter_signal_passed' if out.passed else 'filter_signal_failed'})

    log_migration_event(prod, FilteringEvent(
            group_id=fund.uuid,
            event_name='filter_signal_passed' if out.passed else 'filter_signal_failed',
            display_name='Signal matched' if out.passed else 'Signal did not match',
            data=[out]
    ))

    return out.passed


def migrate_fund_signals(s, fund: Fund, prod: bool):
    logger.info(f'migrating signals for fund {fund.name}')

    processed_projects = MongoDb.processed_projects.find(
                            {'fund_id': fund.id,
                             'status': {
                                        '$in': [
                                                # projects fully processed
                                                'published',
                                                # projects did not pass filters
                                                'filter_company_data_failed',
                                                'filter_is_startup_failed',
                                                'filter_signal_failed',
                                                # some other error - manually follow up later!
                                                'filter_prerequisites_failed',
                                                'validation_error',
                                                'HTTPError',
                                                'unexpected_filter_failure'
                                                ]
                                        }
                            },
                            {'project_id': 1, '_id': 0}
                        )

    with elapsed_timer() as time_fund:
        fund_project_ids, founder_ids = get_fund_projects(s, fund, excluded_projects=processed_projects)

    logger.critical(fund_project_ids)

    logger.info(f'found {len(fund_project_ids)} projects for fund {fund.name}')
    logger.info(f'took {time_fund()} seconds to find projects for fund {fund.name}')

    chunk_size = 10

    for i in range(len(fund_project_ids) // chunk_size + 1):
        with elapsed_timer() as time_fund:
            logger.info(f'processing chunk #{i+1}, {i*chunk_size}-{(i+1)*chunk_size} projects')

            projects_linkedin_data = parse_linkedin_data(s, fund_project_ids[i*chunk_size : (i + 1) * chunk_size], fund)

            filtered_project_ids: list[tuple[int, ProjectLinkedinDetailsSchema]] = []
            for project_id, linkedin_schema in projects_linkedin_data:
                logger.debug('extracting project data from linkedin data')

                project_data: ProjectData = extract_project_data(linkedin_schema)


                logger.debug('running const filters')

                if not check_hard_filters(s, prod, fund, project_id, project_data):
                    continue


                logger.debug('running GPT filters')

                if not check_is_startup(s, prod, fund, project_id, project_data):
                    continue

                if not check_thesis_matches(s, prod, fund, project_id, project_data):
                    continue

                filtered_project_ids.append((project_id, linkedin_schema))


            logger.info(f'{len(filtered_project_ids)} / {len(projects_linkedin_data)} projects passed for fund {fund.name}')

            for pid, linkedin_schema in filtered_project_ids:
                project = s.get(TrackedProject, pid)

                # move project to discovered so that it can be tagged with GPT
                if project.status != ProjectStatus.accepted:
                    project.status = ProjectStatus.review
                    s.add(project)
                    s.commit()

                #publish_project(project_uuid=project.uuid, require_details_fields=False)

                sources: list[tuple[LinkedinSourceSchema, YearMonth]] = get_project_sources(s, project, linkedin_schema, fund)

                logger.info(f'found {len(sources)} signals for project {project}')

                for linkedin_source, timeframe in sources:
                    mongo_signal = AddSignal(
                        project_uuid=project.uuid,
                        fund_uuid=fund.uuid,
                        timeframe=timeframe,
                        source=linkedin_source
                    )

                    project.add_signal(
                        mongo_signal
                    )

                MongoDb.processed_projects.update_one({'project_id': project.id, 'fund_id': fund.id}, {'$set': {'status': 'published'}}, upsert=True)


                # probably only move here when all GPT tags are present
                # if prod and project.status != ProjectStatus.accepted:
                    # project.status = ProjectStatus.published

        logger.info(f'processed chunk #{i+1} in {time_fund()} seconds, {len(fund_project_ids) - (1+i) * chunk_size} projects remaining')
        time.sleep(5)


def migrate_project_signals(s, match_thesis):
    i = 0
    limit = 50

    projects_filter = [TrackedProject.status.in_([ProjectStatus.accepted, ProjectStatus.pending])]

    total_projects = s.execute(select(func.count()).select_from(TrackedProject).where(*projects_filter)).scalar()

    logger.info(total_projects)

    while True:
        print(f'processing {i + limit} out of {total_projects}')

        projects: list[TrackedProject] = s.scalars(
            select(TrackedProject).where(*projects_filter).order_by(nulls_last(desc(TrackedProject.status_changed))).limit(limit).offset(i)
        ).all()

        for project in projects:
            print(project)
            if get_signals_for_project(MongoDb, project.uuid):
                print('project has signals, skipping...')
                continue

            if not project.linkedin_profile:
                print('project has no linkedin profile, skipping...')
                continue

            investor_interactions: list[LinkedinInvestorActivityAssociation] = project.linkedin_profile.get_investor_interactions()

            # post: LinkedinPost = interaction.post
            # funds: list[Fund] = interaction.investor.funds
            # print(len(investor_interactions), 'posts')
            for raw_signal in investor_interactions:
                if not raw_signal.post.post_url:
                    print('post has no url, skipping...')
                    continue

                for fund in raw_signal.investor.funds:
                    if match_thesis:
                        raise NotImplementedError

                    leaders = set(raw_signal.post.likers).intersection(set(project.linkedin_profile.source_profiles))
                    # print(len(leaders), 'leaders')
                    for leader in list(leaders)[:1]:
                        leader_signal = LinkedinPostSignal(
                                            picked_up_date=raw_signal.discovered_date,
                                            post_id=raw_signal.post.id,
                                            leader_id=leader.id,
                                            investing_entity=InvestorIdSchema(id=raw_signal.investor.id),
                                )

                        signal = AddSignal(
                            project_uuid=project.uuid,
                            fund_uuid=fund.uuid,
                            timeframe=YearMonth(
                                year=raw_signal.discovered_date.year,
                                month=raw_signal.discovered_date.month
                            ),
                            source=leader_signal
                        )

                        save_signal(MongoDb, signal)
                        # logger.critical(pformat(f'saving {signal.dict()} five times...'))
                        # for i in range(5):
                            # save_signal(MongoDb, signal)

                        raise RuntimeError

        if len(projects) > 0:
            i += limit
        else:
            break


def publish_projects(s, fund):
    dealflow = set()
    for year in fund.compute_signals():
        for month in year.get('months', []):
            for signal in month.get('signals', []):
                dealflow.add(signal['project_uuid'])

    logger.info(f'found {len(dealflow)} projects in {fund.name}\'s dealflow')

    unpublished = s.scalars(select(TrackedProject.uuid).where(~TrackedProject.status.in_([ProjectStatus.published, ProjectStatus.accepted]), TrackedProject.uuid.in_(list(dealflow)))).all()

    for p_uuid in unpublished:
        p = s.execute(select(TrackedProject).where(TrackedProject.uuid==p_uuid)).scalar()
        if p is None:
            continue
        
        try:
            publish_project(project_uuid=p_uuid, require_details_fields=False)
        except PublishingError as e:
            logger.error(f'failed publishing project: {e}')

            MongoDb.processed_projects.insert_one({'project_id': p.id,
                                                'fund_id': fund.id,
                                                'status': 'publishing_failed'})
 
            continue

        p.status = ProjectStatus.published
        s.add(p)
        s.commit()


if __name__ == "__main__":
    with Session() as s:
        # migrate_project_signals(s, match_thesis=False)
        sequoia: Fund = s.get(Fund, 234)
        automotive: Fund = s.get(Fund, 412)
        porsche: Fund = s.get(Fund, 283)
        goodyear: Fund = s.get(Fund, 267)
        fm_cap: Fund = s.get(Fund, 395)

        #funds_to_migrate = [sequoia, automotive, porsche, goodyear, fm_cap]
        funds_to_migrate = [s.get(Fund, fid) for fid in (223, 239, 241, 265, 271, 275, 277, 280, 298, 300, 302, 304, 306, 309, 311, 370)]

        for fund in funds_to_migrate:
            migrate_fund_signals(s, fund=fund, prod=False)
            publish_projects(s, fund)

        # except Exception as e:
        #     logger.error('unexpected error encountered:')
        #     logger.error(e)
        #     logger.error(e.__traceback__)
        #     logger.error('sleeping for 5 minutes...')
        #     time.sleep(300)
