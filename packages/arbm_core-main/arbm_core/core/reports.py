from sqlalchemy import func, and_

from ..public.projects import Project, UserProjectAssociation


def dynamic_report(date_from: datetime.date | None = None, date_to: datetime.date | None = None,
                   current_user: User = Depends(get_current_active_user)):
    if (date_from and date_to) and date_from > date_to:
        raise ValueError("start of date range cannot be greater than it's end!")

    if date_to and not date_from:
        raise ValueError("end of date range cannot be supplied without start of date range!")

    if (date_from and date_from > datetime.date.today()) or (date_to and date_to > datetime.date.today()):
        raise ValueError("date range cannot etend into future!")

    if not date_from:
        # set default range to the past week
        threshold_from = datetime.date.today() - datetime.timedelta(days=7)
        threshold_to = datetime.date.today()
    else:
        threshold_from = date_from
        threshold_to = date_to or date_from + datetime.timedelta(days=7)

    logger.info(f'getting report data from {threshold_from} to {threshold_to}')

    with Session() as s:
        org = s.get(ClientOrganization, current_user.organization_id)
        logger.info(f'organization: {org.name}')

        filter_projects = [
            UserProjectAssociation.user.has(organization=org),
            UserProjectAssociation.time_recommended >= threshold_from,
            UserProjectAssociation.time_recommended <= threshold_to,
            UserProjectAssociation.revoked == False
        ]

        users_ratings = s.query(ClientUser.username, func.count(distinct(tuple_(
            UserProjectAssociation.username == ClientUser.username,
            UserProjectAssociation.project_id
        )))) \
            .join(UserProjectAssociation) \
            .filter(and_(*filter_projects + [UserProjectAssociation.rating != None])) \
            .group_by(ClientUser.username).all()

        users_feedbacks = s.query(ClientUser.username, func.count(distinct(tuple_(
            UserProjectAssociation.username == ClientUser.username,
            UserProjectAssociation.project_id
        )))) \
            .join(UserProjectAssociation) \
            .filter(and_(*filter_projects + [UserProjectAssociation.feedback != None])) \
            .group_by(ClientUser.username).all()

        users_ratings = Counter({u: c for u, c in users_ratings})
        users_feedbacks = Counter({u: c for u, c in users_feedbacks})

        logger.debug(f'user ratings for {org.name}: {users_ratings}')
        logger.debug(f'user feedbacks for {org.name}: {users_feedbacks}')

        users_activity = Counter()

        users_activity.update(users_ratings)
        users_activity.update(users_feedbacks)

        logger.debug(f'user activity acquired: {users_activity}')

        if not users_activity:
            return

        username_most_active = users_activity.most_common(1)[0][0]
        user_most_active: ClientUser = s.get(ClientUser, username_most_active)

        team_rated_count = 0
        team_feedback_count = 0
        team_total_projects = 0
        stats_users = []
        for user in org.users:
            if not user.active:
                continue

            user_stats = {
                'username': user.username,
            }

            user_stats['all_projects'] = get_user_rated_projects(s, user.username, None, filter_projects)

            user_stats['great_projects'] = get_user_rated_projects(s, user.username, 3, filter_projects)
            user_stats['good_projects'] = get_user_rated_projects(s, user.username, 2, filter_projects)
            user_stats['unfit_projects'] = get_user_rated_projects(s, user.username, 1, filter_projects)
            user_stats['unrated_projects'] = get_user_rated_projects(s, user.username, 0, filter_projects)

            user_stats['projects_with_feedback'] = get_user_rated_projects(s, user.username, None, filter_projects,
                                                                           feedback_required=True)

            logger.info(len(user_stats['all_projects']))
            logger.info(len(user_stats['unrated_projects']))

            n_issued_projects = len(user_stats['all_projects'])
            n_rated_projects = len(user_stats['all_projects']) - len(user_stats['unrated_projects'])
            n_feedback_projects = len(user_stats['projects_with_feedback'])

            team_total_projects += n_issued_projects
            team_rated_count += n_rated_projects
            team_feedback_count += n_feedback_projects

            logger.debug(f'{user.username} got {n_issued_projects} projects and rated {n_rated_projects}')
            logger.debug(f'% rated: {round(n_rated_projects / n_issued_projects * 100)}')

            user_stats['rated_projects_percentage'] = round(
                n_rated_projects / n_issued_projects * 100) if n_issued_projects else 0
            user_stats['feedback_projects_percentage'] = round(
                n_feedback_projects / n_issued_projects * 100) if n_issued_projects else 0

            stats_users.append(UserStatsSchema(**user_stats))

        user_projects_query = s.query(UserProjectAssociation.project_id) \
            .filter(*filter_projects) \
 \
                great_projects_ids = user_projects_query \
                    .filter(UserProjectAssociation.rating == 3) \
                    .group_by(UserProjectAssociation.project_id).all()
        unfit_no_feedback_ids = user_projects_query \
            .filter(UserProjectAssociation.rating == 1, UserProjectAssociation.feedback == None) \
            .group_by(UserProjectAssociation.project_id).all()

        great_projects = s.query(Project).filter(Project.uuid.in_([r[0] for r in great_projects_ids])).all()
        unfit_no_feedback = s.query(Project).filter(Project.uuid.in_([r[0] for r in unfit_no_feedback_ids])).all()

        # there is no .all() method for PropComparator (e.g. on collection of the relationship),
        # hence we use inverted any to exclude ANY project which DOES have non None rating
        unrated_projects = s.query(Project) \
            .filter(Project.client_users.any(and_(*filter_projects))) \
            .filter(~Project.client_users.any(UserProjectAssociation.rating != None)).all()

        one_positive_response_projects = s.query(Project).filter(Project.uuid.in_
            (
            [r[0] for r in s.query(UserProjectAssociation.project_id) \
                .filter(*filter_projects) \
                .filter(UserProjectAssociation.rating > 1) \
                .group_by(UserProjectAssociation.project_id).having(
                func.count(UserProjectAssociation.project_id) == 1).all()]
        )
        ).all()

        report = {
            'most_active_user': user_most_active.username,

            'team_rated_projects_percentage': round(
                team_rated_count / team_total_projects * 100) if team_total_projects else 0,
            'team_feedback_projects_percentage': round(
                team_feedback_count / team_total_projects * 100) if team_total_projects else 0,

            'users_stats': stats_users,

            'great_projects': [p.__dict__ for p in great_projects],
            'unrated_projects': [p.__dict__ for p in unrated_projects],
            'one_response_projects': [p.__dict__ for p in one_positive_response_projects],
            'unfit_no_feedback': [p.__dict__ for p in unfit_no_feedback],
        }

        return report