from collections import Counter

from arbm_core.public.projects import Project, UserProjectAssociation
from arbm_core.public.users import ClientOrganization, ClientUser
from loguru import logger
from sqlalchemy import distinct, func, select, tuple_

from reports import UserStatsSchema


def org_activity_by_user(s, filter_projects):
    query_user_projects = (
        s.query(
            ClientUser.username,
            func.count(
                distinct(
                    tuple_(UserProjectAssociation.username == ClientUser.username, UserProjectAssociation.project_id)
                )
            ),
        )
        .join(UserProjectAssociation)
        .filter(*filter_projects)
    )

    users_ratings = (
        query_user_projects.filter(UserProjectAssociation.rating != None).group_by(ClientUser.username).all()
    )

    users_feedbacks = (
        query_user_projects.filter(UserProjectAssociation.feedback != None).group_by(ClientUser.username).all()
    )

    users_ratings = Counter({u: c for u, c in users_ratings})
    users_feedbacks = Counter({u: c for u, c in users_feedbacks})

    users_activity = Counter()

    users_activity.update(users_ratings)
    users_activity.update(users_feedbacks)

    logger.debug(f"user activity acquired: {users_activity}")

    return users_activity


def mvp_user(s, stats_by_user: list[UserStatsSchema]) -> ClientUser | None:
    max_score = max([s.activity_percentage() for s in stats_by_user])

    if max_score == 0:
        return

    best_users = [u for u in stats_by_user if u.activity_percentage() == max_score]

    if len(best_users) == 1:
        return s.get(ClientUser, best_users[0].username)
    elif len(best_users) > 1:

        reversed_alphabetic = sorted([u.username for u in best_users], reverse=True)
        best_users = sorted(best_users,
                            key=lambda u: (u.find_last_feedback,
                                           reversed_alphabetic.index(u.username)),
                            reverse=True)

        return s.get(ClientUser, best_users[0].username)
    else:
        raise RuntimeError


def get_user_rated_projects(s, username: str, rating: int | None, filters: list,
                            feedback_required: bool = False):
    # avoid modifying filters in-place
    filters = filters.copy()

    filters.append(UserProjectAssociation.username == username)

    if feedback_required:
        filters.append(UserProjectAssociation.feedback != None)

    match rating:
        case None:
            pass
        case 0:
            filters.append(UserProjectAssociation.rating == None)
        case _:
            filters.append(UserProjectAssociation.rating == rating)

    return s.query(UserProjectAssociation).filter(*filters).all()


def get_stats_per_user(s, org: ClientOrganization,
                       filter_projects,
                       threshold_to) -> list[UserStatsSchema]:
    stats_users = []

    filter_feedback = filter_projects + [
        UserProjectAssociation.feedback_posted <= threshold_to
    ]

    for user in org.users:
        if not user.active:
            continue

        user_stats = {
            "username": user.username,
            "all_projects": get_user_rated_projects(s, user.username, None, filter_projects),
            "great_projects": get_user_rated_projects(s, user.username, 3, filter_projects),
            "good_projects": get_user_rated_projects(s, user.username, 2, filter_projects),
            "unfit_projects": get_user_rated_projects(s, user.username, 1, filter_projects),
            "unrated_projects": get_user_rated_projects(s, user.username, 0, filter_projects),
            "projects_with_feedback": get_user_rated_projects(
                s, user.username, None, filter_feedback, feedback_required=True
            ),
        }

        n_issued_projects = len(user_stats["all_projects"])
        n_rated_projects = len(user_stats["all_projects"]) - len(user_stats["unrated_projects"])
        n_feedback_projects = len(user_stats["projects_with_feedback"])

        user_stats["rated_projects_percentage"] = (
            round(n_rated_projects / n_issued_projects * 100) if n_issued_projects else 0
        )
        user_stats["feedback_projects_percentage"] = (
            round(n_feedback_projects / n_issued_projects * 100) if n_issued_projects else 0
        )

        logger.debug(f"{user.username} got {n_issued_projects} projects and rated {n_rated_projects}"
                     f"% rated: {user_stats['rated_projects_percentage']}, "
                     f"% feedback: {user_stats['feedback_projects_percentage']}")

        stats_users.append(UserStatsSchema(**user_stats))

    return stats_users



def report_project_groups(s, org: ClientOrganization, filter_projects):
    PROJECT_GROUPS = {
        'great': {
                'filter': [UserProjectAssociation.rating == 3],
                'having': None,
            },
        'one_positive': {
                'filter': [UserProjectAssociation.rating > 1],
                'having': func.count('*') == 1,
            },
        'unfit_no_feedback': {
                'filter': [UserProjectAssociation.rating == 1,
                        UserProjectAssociation.feedback == None],
                'having': None,
            },
        'unrated': {
                'filter': [UserProjectAssociation.rating == None],
                'having': func.count('*')
                                    == len([u for u in org.users if u.active]),
            },
    }

    user_projects_query = select(UserProjectAssociation.project_id).filter(*filter_projects)

    report_projects = {}
    for group, group_config in PROJECT_GROUPS.items():
        project_ids_query = user_projects_query.filter(*group_config['filter'])\
                         .group_by(UserProjectAssociation.project_id)

        if (having_stmt := group_config.get('having')) is not None:
            project_ids_query = project_ids_query.having(having_stmt)

        group_projects = s.scalars(
            select(Project).filter(
                Project.uuid.in_(
                    s.scalars(project_ids_query).all()
                )
            )
        ).all()

        report_projects[group] = group_projects

    return report_projects

    # there is no .all() method for PropComparator (e.g. on collection of the relationship),
    # hence we use inverted any to exclude ANY project which *DOES* have *non-null* rating
    # unrated_projects = (
    #     s.query(Project)
    #     .filter(Project.client_users.any(and_(*filter_projects)))
    #     .filter(~Project.client_users.any(UserProjectAssociation.rating != None))
    #     .all()
    # )

    # one_positive_response_projects = (
    #     s.query(Project)
    #     .filter(
    #         Project.uuid.in_(
    #             [
    #                 .filter(UserProjectAssociation.rating > 1)
    #                 .group_by(UserProjectAssociation.project_id)
    #                 .having(func.count(UserProjectAssociation.project_id) == 1)
    #                 .all()
    #             ]
    #         )
    #     )
    #     .all()
    # )