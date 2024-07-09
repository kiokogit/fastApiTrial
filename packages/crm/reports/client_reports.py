import datetime
from pprint import pprint

import pytz
from arbm_core.private import Session
from arbm_core.public.projects import UserProjectAssociation
from arbm_core.public.users import ClientOrganization, OrganizationReport
from fastapi.encoders import jsonable_encoder
from loguru import logger
from sqlalchemy import select

from reports.client_utils import get_stats_per_user, mvp_user, report_project_groups
from util import TIMEZONE, dt_fmt, utc_now


def get_report(s, org: ClientOrganization, type: str) -> OrganizationReport | None:
    match type:
        case "weekly":
            return (
                s.query(OrganizationReport)
                .filter(
                    OrganizationReport.organization == org,
                    OrganizationReport.report_type == "weekly",
                    OrganizationReport.revoked == False,
                )
                .order_by(OrganizationReport.start_date.desc(), OrganizationReport.time_generated.desc())
                .first()
            )
        case _:
            raise NotImplementedError


def compute_report(
    s, organization_id: str, date_from: datetime.date, date_to: datetime.date, report_type: str
) -> OrganizationReport | None:
    if not date_to or not date_from:
        raise ValueError("start and end of the date range must be supplied!")

    if date_from >= date_to:
        raise ValueError("start of date range cannot be greater or equal than it's end!")

    if date_from > datetime.date.today() or date_to > datetime.date.today():
        raise ValueError("date range cannot extend into future!")


    org = s.get(ClientOrganization, organization_id)
    client_tz = org.get_timezone()

    threshold_from = client_tz.localize(datetime.datetime.combine(date_from,
                                                                  datetime.datetime.min.time()))
    threshold_to = client_tz.localize(datetime.datetime.combine(date_to,
                                                                datetime.datetime.max.time()))

    logger.info(f"getting report data from {dt_fmt(threshold_from)} to {dt_fmt(threshold_to)} "
                f"for {organization_id}")

    filter_projects = [
        UserProjectAssociation.user.has(organization=org),
        UserProjectAssociation.time_recommended >= threshold_from,
        UserProjectAssociation.time_recommended <= threshold_to,
        UserProjectAssociation.revoked == False,
    ]

    users_activity = get_stats_per_user(s, org, filter_projects, threshold_to)
    if not users_activity:
        logger.info(
            f"user activity not found for {organization_id}"
            f" between {threshold_from} and {threshold_to}, returning"
        )
        return

    team_rated_count = sum([s.n_projects_rated() for s in users_activity])
    team_feedback_count = sum([s.n_projects_feedback() for s in users_activity])
    team_total_projects = sum([s.n_total_projects() for s in users_activity])

    project_highlights = report_project_groups(s, org, filter_projects)
    best_user = mvp_user(s, users_activity)

    team_scores = {"rated": 0, "feedback": 0}

    if team_total_projects:
        team_scores['rated'] = round(team_rated_count / team_total_projects * 100)
        team_scores['feedback'] = round(team_feedback_count / team_total_projects * 100)

    report = {
        "team_rated_projects_percentage": team_scores['rated'],
        "team_feedback_projects_percentage": team_scores['feedback'],

        "most_active_user": best_user.username if best_user else None,
        "users_stats": users_activity,

        "great_projects": [p.__dict__ for p in project_highlights['great']],
        "unrated_projects": [p.__dict__ for p in project_highlights['unrated']],
        "one_response_projects": [p.__dict__ for p in project_highlights['one_positive']],
        "unfit_no_feedback": [p.__dict__ for p in project_highlights['unfit_no_feedback']],
    }

    return OrganizationReport(
        organization_id=organization_id,
        report_type=report_type,
        start_date=threshold_from.date(),
        end_date=threshold_to.date(),
        contents=jsonable_encoder(report),
    )


def make_weekly_report(s, organization: ClientOrganization):
    date_to = datetime.date.today()
    date_from = date_to - datetime.timedelta(days=7)

    weekly_report = compute_report(s, organization.name, date_from=date_from, date_to=date_to, report_type="weekly")
    s.add(weekly_report)
    s.commit()


def generate_monthly_report(s, organization: ClientOrganization):
    date_to = datetime.date.today()
    date_from = date_to.replace(day=1)

    monthly_report = compute_report(s, organization.name, date_from=date_from, date_to=date_to, report_type="monthly")
    s.add(monthly_report)
    s.commit()


@logger.catch()
def main():
    with Session() as public_s:
        orgs_report_enabled: list[ClientOrganization] = (
            public_s.query(ClientOrganization)
            .filter(ClientOrganization.summary_day != None)
            .order_by(ClientOrganization.name)
            .all()
        )

        logger.debug(f'found {len(orgs_report_enabled)} client organisations '
                     'with weekly report enabled: '
                     f'{", ".join([org.name for org in orgs_report_enabled])}')

        for org in orgs_report_enabled:
            client_tz = pytz.timezone(org.timezone) if org.timezone else TIMEZONE

            today_local = utc_now().astimezone(tz=client_tz)
            weekday_local = today_local.strftime("%A").lower()
            is_report_day = weekday_local == org.summary_day

            logger.info(
                f'Weekly report for {org.name} is scheduled for {"today" if is_report_day else org.summary_day}' +
                ("" if is_report_day else f" but {org.name}'s local time is {weekday_local}")
            )

            reports_this_week = (
                public_s.query(OrganizationReport)
                .filter(
                    OrganizationReport.organization == org,
                    OrganizationReport.report_type == "weekly",
                    OrganizationReport.revoked != True,
                    OrganizationReport.end_date > datetime.date.today() - datetime.timedelta(days=7),
                )
                .all()
            )

            should_generate = weekday_local == org.summary_day
            if not should_generate:
                continue

            if len(reports_this_week) > 0:
                logger.debug(f"reports covering this week already exist! ({len(reports_this_week)} found)."
                            f" cancelling generation for {org.name}")
                logger.debug(f"Report threshold from: {datetime.date.today() - datetime.timedelta(days=7)}")
                for report in reports_this_week:
                    logger.debug(f"Previous report dates: {report.start_date} - {report.end_date}")
                continue

            logger.info(f"Generating weekly report for {org.name}")
            make_weekly_report(public_s, org)


def generate_historical_reports():
    with Session() as s:
        org = s.get(ClientOrganization, "Holman")

        # return
        date_to = datetime.date(year=2023, month=6, day=11)# - datetime.timedelta(days=7)
        # date_from = datetime.date(year=2023, month=5, day=17)
        date_from = date_to - datetime.timedelta(days=6)

        report = compute_report(s, org.name,
                                date_from=date_from,
                                date_to=date_to,
                                report_type="weekly")
        s.add(report)
        s.commit()


def mvp_all_time():
    with Session() as s:
        from collections import defaultdict
        mvps = defaultdict(int)

        reps = s.scalars(select(OrganizationReport).filter_by(organization_id='Holman', revoked=False)).all()
        for r in reps:
            mvp = r.contents['most_active_user']
            mvps[mvp] += 1

        pprint(mvps)


def compare_reports():
    with Session() as s:
        org: ClientOrganization = s.get(ClientOrganization, "Holman")
        reports = sorted([r for r in org.reports if r.end_date <= datetime.datetime.strptime("2023-05-10", "%Y-%m-%d").date()], key=lambda r: r.end_date, reverse=True)

        from collections import defaultdict
        reports[9] = OrganizationReport(contents=defaultdict(list))
        reports[11] = OrganizationReport(contents=defaultdict(list))
        reports[13] = OrganizationReport(contents=defaultdict(list))

        for pair_ifx in range(0, len(reports), 2):
            pair = reports[pair_ifx: pair_ifx + 2]
            if len(pair) == 1:
                break

            a, b = pair
            print(pair_ifx, a.start_date, '-', a.end_date, b.start_date, '-', b.end_date)
            print(a.contents['most_active_user'], b.contents['most_active_user'])
            print(a.contents['team_feedback_projects_percentage'], b.contents['team_feedback_projects_percentage'])
            print()


def generate_test_report():
    with Session() as public_s:
        org = public_s.get(ClientOrganization, "ARBM")
        make_weekly_report(public_s, org)


if __name__ == "__main__":
    # mvp_all_time()
    # compare_reports()
    # generate_historical_reports()
    main()
