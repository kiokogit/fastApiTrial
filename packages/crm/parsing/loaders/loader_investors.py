import pandas as pd
from arbm_core.private import Session as BackendSession
from arbm_core.private.investors import Fund, Investor
from arbm_core.private.twitter import TwitterParsingResult, TwitterProject
from loguru import logger
from sqlalchemy import func
from sqlalchemy.orm import Session

import util
from util import clean_url


def prepare_df(df):
    df = df.fillna("")
    df.columns = df.columns.str.lower()

    if "investor_type" not in df:
        df["investor_type"] = None

    df.rename(columns={
            'linkedin url': 'linkedin_url',
            'twitter url': 'twitter_url',
            'company': 'fund'
        },
        inplace=True
    )

    return df


def load_investors(investors_df: pd.DataFrame):
    investors_df = prepare_df(investors_df)

    investors_loaded = 0
    investors_updated = 0

    with BackendSession() as s:
        for i, row in investors_df.iterrows():
            try:
                if not row['name'] or row['name'].strip() == '':
                    logger.debug(f"Skipping row with an invalid name: {row['name']}")
                    continue

                fund = s.query(Fund).filter(func.lower(Fund.name) == func.lower(str(row["fund"]))).one_or_none()
                if not fund:
                    logger.info(f"Fund not found with name: {row['fund']}, creating...")
                    fund = Fund(name=row["fund"],
                                type=row.get("type", None),
                                logo=row.get("logo", None))
                    s.add(fund)
                elif not fund.logo:
                    fund.logo = row.get("logo", None)
                    s.add(fund)

                logger.info(f"loading investor with name {row['name']}")

                investor = s.query(Investor).filter_by(name=row["name"]).one_or_none()

                twitter_username = None
                twitter_projects = []
                if "twitter_url" in row and investor and investor.twitter_url is None:
                    twitter_username = util.twitter_url_to_username(row["twitter_url"])

                    twitter_projects = (
                        s.query(TwitterProject)
                            .filter(
                            TwitterProject.parsed_data.any(TwitterParsingResult.query_username == twitter_username))
                            .all()
                    )
                if investor is None:
                    investor = Investor(
                        name=row["name"],
                        funds=[fund],
                        role=row["role"],
                        investor_type=row["investor_type"],
                        linkedin_url=clean_url(row["linkedin_url"]),
                        twitter_url=twitter_username,
                        twitter_subscriptions=twitter_projects,
                    )
                    investors_loaded += 1
                else:
                    investor.funds.append(fund)

                    if not investor.twitter_url and row["twitter_url"]:
                        investor.twitter_url = twitter_username
                        investor.twitter_subscriptions = twitter_projects
                        return f"Updated twitter_url for investor {investor}, "
                    if not investor.linkedin_url and row["linkedin_url"]:
                        investor.linkedin_url = clean_url(row["linkedin_url"])
                        return f"Updated linkedin_url for investor {investor}, "
                    investors_updated += 1

                s.add(investor)
                s.commit()
            except KeyError as e:
                key = e.args[0]
                return f"Column '{key}' must be present in csv!"

    return investors_loaded, investors_updated


def load_funds():
    engine = get_engine(False)
    with Session(engine) as s:
        for investor in s.query(Investor).distinct(Investor.name):
            fund_name = investor.fund

            fund = s.query(Fund).filter(func.lower(Fund.name) == func.lower(fund_name)).one_or_none()

            if not fund:
                logger.info(f"Fund not found with name: {fund_name}, creating...")
                fund = Fund(name=fund_name,
                            type=investor.type)

            investor.funds.append(fund)
            s.add(fund)
            s.commit()
