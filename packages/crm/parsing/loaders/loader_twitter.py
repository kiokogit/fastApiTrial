import datetime

import pandas as pd
from arbm_core.private import Session
from arbm_core.private.projects import ProjectSource, ProjectStatus, TrackedProject
from arbm_core.private.investors import Investor
from arbm_core.private.twitter import TwitterParsingResult, TwitterProject, insert_parsing_records
from loguru import logger
from sqlalchemy import or_

import parsing.extraction.twitter_extraction
from alpha_columns import PrjCols
from web.app import TIMEZONE

# logger.remove()
# logger.add(sys.stdout, format="{level} {message}")


def insert_projects(projects: pd.DataFrame):
    new_projects = 0

    with Session() as s:
        for i, row in projects.iterrows():
            if i % 10 == 0 and i != 0:
                logger.info(f"Processed {i} projects")

            twitter_project = s.query(TwitterProject).filter_by(user_id=row[PrjCols.twitter_uid]).one_or_none()

            if twitter_project is None:
                logger.warning("TwitterProject doesn't exist yet, creating new")
                logger.warning(f"Founders for the project: '{row['founders']}', founder ids: '{row['founders_ids']}'")

                twitter_project = TwitterProject(
                    user_id=row[PrjCols.twitter_uid],
                    founders=list(set(row['founders'].strip().split(','))) if (row['founders'].strip()) else [],
                    founders_ids=list(set(row['founders_ids'].strip().split(','))) if (row['founders_ids'].strip()) else [],
                )
            else:
                logger.warning("TwitterProject already exists, updating")
                logger.warning(f"founders: {row['founders']}, {row['founders_ids']}")

                if row['founders'] is not None and not pd.isna(row['founders']) and row['founders'].strip() != '':
                    logger.info(f"Project already exists but founders found in row, founders value is {twitter_project.founders}")
                    logger.info(f"Updating founders with {row['founders']}")

                    founders = set(twitter_project.founders) if twitter_project.founders else set()
                    if row['founders'].strip() != '':
                        founders.update(row['founders'].strip().split(','))
                    twitter_project.founders = list(founders)

                    logger.info(f"twitter_project.founders now: {twitter_project.founders}")

                    logger.info(f"Updating founders_ids with {row['founders_ids']}")
                    founders_ids = set(twitter_project.founders_ids) if twitter_project.founders_ids else set()
                    if row['founders_ids'].strip() != '':
                        founders_ids.update(row['founders_ids'].strip().split(','))
                    twitter_project.founders_ids = list(founders_ids)

                    logger.info(f"twitter_project.founders_ids now: {twitter_project.founders_ids}")

            tracked_project = s.query(TrackedProject).filter(TrackedProject.twitter.has(user_id=row[PrjCols.twitter_uid])).one_or_none() \
                              or TrackedProject(
                                    title=row[PrjCols.name],
                                    website=str(row[PrjCols.website]),
                                    source=ProjectSource.twitter,
                                    status=ProjectStatus.discovered,
                                    twitter=twitter_project,
                                )
            twitter_project.tracked_project = tracked_project

            if twitter_project.founders_ids and twitter_project.founders_ids != '':
                logger.warning(f"{twitter_project} has founder ids associated: '{twitter_project.founders_ids}'")
                p_data = s.query(TwitterParsingResult).filter(or_(TwitterParsingResult.user_id == row[PrjCols.twitter_uid],
                                                              TwitterParsingResult.user_id.in_([i for i in twitter_project.founders_ids if not isinstance(i, str)]))).all()
            else:
                logger.warning(f"{twitter_project} has no founder ids associated")
                p_data = s.query(TwitterParsingResult).filter(
                    (TwitterParsingResult.user_id == row[PrjCols.twitter_uid])).all()

            if twitter_project.founders_ids and not p_data:
                logger.critical("Project has founders_ids but parsing data not found:")
                logger.critical(twitter_project.founders_ids)
                logger.critical(twitter_project.p_data)

            for parsed in p_data:
                parsed.twitter_project = twitter_project
                s.add(parsed)

            if len(twitter_project.parsed_data) > 0:
                twitter_project.discovered_date = sorted((p for p in twitter_project.parsed_data), key=lambda x: x.time_parsed)[0].time_parsed

            twitter_project.investors.extend(
                s.query(Investor).filter(Investor.twitter_url.in_([o.query_username for o in twitter_project.parsed_data])).all()
            )

            if twitter_project.discovered_date is not None and twitter_project.discovered_date > datetime.datetime.now(tz=TIMEZONE) - datetime.timedelta(days=5):
                logger.info(f"new project: {twitter_project}")
                new_projects += 1

            s.add(tracked_project)
            s.add(twitter_project)
            s.commit()
    logger.info(f"New projects extracted: {new_projects}")
    return new_projects


async def load_parsed_subscriptions(df):
    # prepare df
    df.rename({
            "userId": PrjCols.twitter_uid,
            "followersCount": PrjCols.twitter_followers,
            "twitter_url": PrjCols.twitter_url,
            "profileUrl": PrjCols.twitter_url,
            "name": PrjCols.name,
            "website": PrjCols.website,
        },
        axis=1, inplace=True)

    # load parser records
    logger.info("Loading twitter parsing records into db")
    with Session() as s:
        logger.debug("getting parsed count")
        parsed_count = s.query(TwitterParsingResult).count()
        logger.debug(f"parsed count: {parsed_count}")

        j = 0
        for i, row in df.iterrows():
            insert_parsing_records(s, row)

            j += 1
            if j % 100 == 0:
                cnt = j
                logger.info(f"loading parsing records, {round((cnt / len(df) * 100), 3)}% loaded"
                             f" ({cnt} out of {len(df)})")

        parsed_count = s.query(TwitterParsingResult).count() - parsed_count
        logger.info(f"{parsed_count} parsing records loaded")

        logger.info("Extracting projects from parsing records")
        extracted = await parsing.extraction.twitter_extraction.twitter_extract_projects(df, True)
        logger.info(f"extracted {len(extracted)} projects from parsing records")

        project_count = s.query(TwitterProject).count()
        logger.warning(f"Inserting extracted projects to database")
        logger.info(f"project count before: {project_count}")

        new_projects = insert_projects(extracted)

        project_count = s.query(TwitterProject).count()
        logger.warning(f"Finished inserting extracted projects to database")
        logger.info(f"project count after: {project_count}")
        return new_projects
