import argparse
import datetime

import pandas as pd
from arbm_core.private import Session
from arbm_core.private.linkedin import (
    LinkedinInvestorActivityAssociation,
    LinkedinPersonal,
    LinkedinPost,
    LinkedinUrl,
)
from arbm_core.private.investors import (
    Investor
)
from loguru import logger

from util import utc_now


class LinkedinImportError(BaseException):
    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        self.errors = errors


def get_error_for_row(row: pd.Series):
    return row['error'] if ('error' in row and row['error'] and not pd.isna(row['error'])) else None


def load_activity(activity: pd.DataFrame) -> dict:
    activity[activity.columns.difference(["likeCount", "commentCount", "viewCount"])]\
        = activity[activity.columns.difference(["likeCount", "commentCount", "viewCount"])].fillna('')

    activity[["likeCount", "commentCount", "viewCount"]] = activity[["likeCount", "commentCount", "viewCount"]].fillna(
        0
    )

    stats = {
        'activity_loaded': 0,
        'activity_updated': set(),
        'posts_updated': set(),
    }

    with Session() as s:
        profile_links = set(activity["profileUrl"].values)
        investors = set([r[0] for r in s.query(Investor.linkedin_url).filter(Investor.linkedin_url.in_(profile_links)).distinct()])

        if investors_missing := profile_links.difference(investors):
            logger.warning(f"investor(s) not found: {investors_missing}")
        #     raise LinkedinImportError(f"investor(s) not found: {investors_missing}",
                                      # {'profiles': investors_missing})

        for _, r in activity.iterrows():
            if err := get_error_for_row(r):
                logger.warning(f"parser returned an error for row: {err}")
                logger.warning(f"row affected: \n{r}")
                logger.warning(f"skipping")
                continue

            investor_list = s.query(Investor).filter_by(linkedin_url=r["profileUrl"]).all()

            if not investor_list:
                logger.warning(f'investor not found: {r["profileUrl"]}, skipping row')
                continue

            post = s.query(LinkedinPost).filter_by(post_url=r["postUrl"]).one_or_none()

            if not post:
                post = LinkedinPost(
                    post_url=r["postUrl"],
                    relative_post_date=r["postDate"],
                    parsed_date=utc_now(),

                    text=r["postContent"],
                    like_count=r["likeCount"],
                    comment_count=r["commentCount"],
                    view_count=r["viewCount"],

                    # shared urls
                    shared_url=r["sharedPostUrl"] if "sharedPostUrl" in r else None,
                    shared_company_url=r["sharedPostCompanyUrl"] if "sharedPostCompanyUrl" in r else None,
                    shared_profile_url=r["sharedPostProfileUrl"] if "sharedPostProfileUrl" in r else None,
                )
            else:
                post_updated = False
                for post_attr, parsed_attr in {'text': 'postContent',
                                               'like_count': 'likeCount',
                                               'comment_count': 'commentCount',
                                               'view_count': 'viewCount',
                                               'relative_post_date': 'postDate',
                                               'shared_url': 'sharedPostUrl',
                                               'shared_company_url': 'sharedPostCompanyUrl',
                                               'shared_profile_url': 'sharedPostProfileUrl'}.items():

                    if getattr(post, post_attr) is None and parsed_attr in r and r[parsed_attr]:
                        setattr(post, post_attr,  r[parsed_attr])
                        post_updated = True
                if post_updated:
                    stats['posts_updated'].add(post.id)

            # don't overwrite if activity item discovered already
            for investor in investor_list:
                if r["postUrl"] in [a.post.post_url for a in investor.linkedin_activity]:
                    for a in investor.linkedin_activity:
                        if a.post.post_url == r["postUrl"]:
                            if not a.activity_type:
                                a.activity_type = r["action"]
                                stats['activity_updated'].add(a)
                else:
                    investor_to_post = LinkedinInvestorActivityAssociation(
                        investor_id=investor.id, activity_type=r["action"], discovered_date=r["timestamp"]
                    )
                    investor_to_post.post = post
                    investor.linkedin_activity.append(investor_to_post)

                    stats['activity_loaded'] += 1

                    s.add(investor_to_post)

                s.add(investor)
            s.add(post)
            s.commit()

    return stats


def load_likers(likers: pd.DataFrame):
    likers_loaded = 0

    with Session() as s:
        # load last 1000 rows
        for i, r in likers[-1000:].iterrows():
            post = s.query(LinkedinPost).filter_by(post_url=r["postUrl"]).one_or_none()

            if not post:
                logger.error(f"Post not found: {r['postUrl']}")
                continue

            if err := get_error_for_row(r):
                logger.warning(f"parser returned an error for row: {err}")
                logger.warning(f"row affected: \n{r}")
                logger.warning(f"skipping")
                continue

            # profileLink, name, firstName, lastName, degree, job, reactionType, postUrl, timestamp
            liker = s.query(LinkedinPersonal).where(
                LinkedinPersonal.urls.any(LinkedinUrl.url == r["profileLink"])
            ).one_or_none()

            if not liker:
                liker = LinkedinPersonal(
                    name=r["name"],
                    urls=[LinkedinUrl(url=r["profileLink"])],
                    degree=r["degree"],
                    job=r["job"],
                )

            if liker in post.likers:
                logger.info(f"liker already loaded {r['name']} for post {post.id}")
                continue

            post.likers.append(liker)
            logger.info(f"loaded liker {liker.name} for post {post.id}")
            logger.info(f"likers loaded: {likers_loaded}")

            likers_loaded += 1

            post.likers_parsed_date = utc_now()

            s.add(liker)
            s.add(post)
            s.commit()
    return likers_loaded


def linkedin_load_file(filename, type):
    df = pd.read_csv(filename)

    match type:
        case "activity":
            # load investor activity
            load_activity(df)
        case "likes":
            # load post likes
            load_likers(df)
        case _:
            raise ValueError("invalid linkedin input type supplied")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True)
    parser.add_argument("--type", type=str, required=True)

    args = parser.parse_args()

    linkedin_load_file(args.file, args.type)
