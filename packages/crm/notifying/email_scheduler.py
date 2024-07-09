import math
import os
import random
import smtplib
import sys
import time
from datetime import date, datetime, timedelta
from email.headerregistry import Address
from textwrap import TextWrapper
from uuid import UUID

import arbm_core
from arbm_core.public.projects import Project, UserProjectAssociation
from arbm_core.public.users import ClientOrganization, ClientUser, Email, OrganizationReport
from email_validator import EmailNotValidError, validate_email
from loguru import logger
from sqlalchemy import and_, or_

import util
from messaging.email import smtp_send
from notifying.admin import notify_email_failed, notify_sent_email
from reports.client_reports import get_report
from util import HR, utc_now


class NotificationError(Exception):
    pass


class EmailCancelled(Exception):
    pass


logger.remove()
logfile = util.project_root() / "logs/email_scheduler" / f"email_scheduler.log"


def formatter(record):
    base_format = "[Email Scheduler] {time} {name} {level}: "

    lines = str(record["message"]).splitlines()
    record["message"] = lines[0]
    base = base_format.format_map(record)

    indent = "\n" + " " * len(base)
    reformatted = base + indent.join(lines[:])

    record["extra"]["reformatted"] = reformatted
    return "{extra[reformatted]}\n{exception}"


logger.add(
    sys.stderr,
    format=formatter,
    level="DEBUG",
    backtrace=True,
    diagnose=True,
)
logger.add(
    logfile.absolute(),
    rotation="00:00",
    format="[Email Scheduler] {time} {name} {level}: {message}",
    level="DEBUG",
    backtrace=True,
    diagnose=True,
)


TEST_RECIPIENTS = os.environ.get("SMTP_TEST_RECIPIENTS")
SEND_RETRIES = os.environ.get("SMTP_SEND_RETRIES", 3)
RETRY_INTERVAL = 5

if not isinstance(SEND_RETRIES, int):
    if not SEND_RETRIES.isnumeric():
        raise ValueError("SMTP_SEND_RETRIES env var must be an integer!")

    SEND_RETRIES = min(int(SEND_RETRIES), 10)


filter_active_organization = ClientOrganization.membership == "premium"
sender_clients = Address("Anton from LookingGlass", "anton", "arbmintel.com")
sender_admin = Address("ARBM Bot", "admin", "arbmintel.com")

# ========== EMAIL UTILS ==========


def SCHEDULE_TIME(tz):
    return tz.localize(datetime.now().replace(hour=9, minute=00, second=0, microsecond=0))


def SEND_EMAIL_TIME(tz):
    return tz.localize(datetime.now().replace(hour=10, minute=00, second=0, microsecond=0))


def get_email_time(type, tz):
    match type:
        case "projects_summary_weekly":
            return tz.localize(datetime.now().replace(hour=9, minute=00, second=0, microsecond=0))
        case _:
            return SEND_EMAIL_TIME(tz)


def should_send(type, organization: ClientOrganization):
    tz = organization.get_timezone()
    sat, sun = [5, 6]

    logger.debug(
        f"local time at {organization.name}'s TZ ({tz}) is {util.dt_fmt(utc_now().astimezone(tz))}\n"
        f"send time for the email {type} is after"
        f" {util.dt_fmt(get_email_time(type, tz))}"
        f"\nhence, should send is {utc_now().astimezone(tz) > get_email_time(type, tz)}"
    )

    time_valid = utc_now().astimezone(tz) > get_email_time(type, tz)

    today_i = organization.local_time().weekday()
    is_workday = today_i < sat

    match type:
        case "projects_summary_weekly":
            report_day_i = time.strptime(organization.summary_day, "%A").tm_wday

            wday_valid = report_day_i in [sat, sun] or today_i >= report_day_i

            # send notification between monday and friday, after report date
            return is_workday and time_valid and wday_valid
        case _:
            return is_workday and time_valid


def get_subject(email_type: str, default: str | None = None) -> str:
    match email_type:
        case "feedback_notify":
            return "LookingGlass feedback notification"
        case "access_request_notify":
            return "New Access request for LookingGlass"
        case "projects_summary_daily":
            return "LookingGlass Daily Project Summary"
        case "projects_summary_weekly":
            return "LookingGlass Weekly Projects Summary"
        case _:
            return default or "LookingGlass notification"


def get_user_email(user) -> Address | None:
    if not user.email:
        logger.error(f"user {user.username} does not have an email!")
        return

    # todo: use email validator
    try:
        email = validate_email(user.email)
        email.normalized
    except EmailNotValidError as e:
        logger.error(f"{e}\nuser {user.username}'s email is invalid: {user.email}")
        return

    return Address(user.username, email.local_part, email.domain)


def get_project_kwargs(project):
    project_url = f"https://terminal.twotensor.com/terminal/project/{project.uuid}"

    return {
        "title": project.title,
        "signals": ", ".join([f.name for f in project.funds]),
        "categories": ", ".join(project.verticals or []),
        "about": project.about or "",
        "website": project.website,
        "project_link": project_url,
    }


def send_email(s, scheduled_email, user, subject):
    if not (receiver := get_user_email(user)):
        return

    sent = False
    error_msg = None

    if not scheduled_email.approved:
        error_msg = (
            f"{scheduled_email} email was not approved" f"for user {user} @ {user.organization}, " f"sending cancelled"
        )

        logger.error(error_msg)
        notify_email_failed(error_msg)
        return

    try:
        sent = smtp_send(
            subject=subject,
            contents_plaintext=scheduled_email.plaintext,
            contents_html=scheduled_email.html,
            sender=sender_clients,
            receiver=receiver,
        )
    except smtplib.SMTPException as e:
        error_msg = f"""email was not sent to the user {user} @ {user.organization}
            ({user.email}).
            \n\n
            Error encountered:\n
            {e}
            \nEmail text:
            \n{scheduled_email.plaintext}"""

        logger.critical(error_msg)
        notify_email_failed(error_msg)

        raise e

    scheduled_email.sent = sent
    s.add(scheduled_email)
    s.commit()

    # notify admins of the sent email
    notify_sent_email(user, subject, scheduled_email.plaintext, content_html=scheduled_email.html)


# ========== EMAIL GENERATION ==========


def make_weekly_email(s, org: ClientOrganization, recipient: ClientUser) -> tuple[str, str]:
    MODE = 'regular'
    report: OrganizationReport = get_report(s, org, "weekly")

    if report is None:
        raise NotificationError(f"could not find any report for {org}, cancelling weekly email")

    report_day = time.strptime(org.summary_day, "%A").tm_wday

    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())

    if report.end_date > start_of_week + timedelta(days=report_day):
        raise NotificationError(f"the latest report's end date is on {report.end_date}"
                                f" which is after {start_of_week + timedelta(days=report_day)}, cancelling weekly email")

    report_data = report.contents
    users_stats_report: dict[str, dict] = {stats["username"]: stats for stats in report_data["users_stats"]}

    for k, v in users_stats_report.items():
        user_orm: ClientUser = s.get(ClientUser, k)

        v["user_firstname"] = user_orm.firstname
        v["user_lastname"] = user_orm.lastname

        v["user_rated_projects_percentage"] = v["rated_projects_percentage"]
        v["user_feedback_percentage"] = v["feedback_projects_percentage"]
        v["user_performance_percentage"] = v["feedback_projects_percentage"]

        v["user_rated_projects_count"] = len(v["all_projects"]) - len(v["unrated_projects"])
        v["user_great_projects_count"] = len(v["great_projects"])
        v["user_good_projects_count"] = len(v["good_projects"])
        v["user_unfit_projects_count"] = len(v["unfit_projects"])
        v["user_feedback_projects_count"] = len(v["projects_with_feedback"])

        if v["rated_projects_percentage"] >= 80:
            color = "#16c060"
        elif v["rated_projects_percentage"] >= 30:
            color = "#ffd209"
        else:
            color = "#e36969"

        v["color"] = color

    # sort users by score, mvp user always comes first
    stats_all_users = {
        k: v
        for k, v in sorted(
            users_stats_report.items(),
            key=lambda u: (u[0] == report_data["most_active_user"], u[1]["rated_projects_percentage"]),
            reverse=True,
        )
    }

    templates_root = util.project_root() / "const/email_templates"

    email_html_src = (templates_root / "email-weekly-final-table.html").read_text()
    email_team_no_activity = (templates_root / "email-weekly-team-no-activity.html").read_text()
    email_user_no_activity = (templates_root / "email-weekly-user-no-activity.html").read_text()
    user_stats_template = (templates_root / "weekly-user-stats-template.html").read_text()
    project_table_full = (templates_root / "email-weekly-project-table.html").read_text()
    user_table_entry = (templates_root / "email-weekly-project-table-entry.html").read_text()
    # user_feedback_template_src = (util.project_root() / 'const/email_templates' / 'weekly-user-feedback.html').read_text()
    projects_row_src = (templates_root / "email-weekly-projects-unfit.html").read_text()
    minimal_project_src = (templates_root / "email-weekly-project-unfit.html").read_text()
    mvp_user_src = (templates_root / "weekly_report" / "mvp_user.html").read_text()
    no_mvp_user_src = (templates_root / "weekly_report" / "no_mvp_user.html").read_text()

    def get_user_ratings(project_users: list[ClientUser], rating: int) -> str:
        return ", ".join([f"{u.user.firstname} {u.user.lastname}" for u in project_users if u.rating == rating]) or "-"

    def rating_to_str(rating):
        match rating:
            case 1:
                return "Unfit"
            case 2:
                return "Good"
            case 3:
                return "Great"
            case None:
                return "N/A"
            case _:
                raise ValueError

    def get_rated_project_cards(template, projects_with_ratings: list[UUID]):
        project_cards = []

        for p_uuid in projects_with_ratings:
            project: Project = s.get(Project, p_uuid)

            # users_with_feedback = [u for u in project.users_recommended(organization_id=org.name) if u.feedback != None]

            user_entries = ""
            for u in project.users_recommended(organization_id=org.name):
                if u.rating is not None:
                    user_entries += user_table_entry.format(
                        user_firstname=u.user.firstname,
                        user_lastname=u.user.lastname,
                        feedback=u.feedback or "N/A",
                        rating=rating_to_str(u.rating),
                        color="000",
                    )

            project_cards.append(
                template.format(
                    **get_project_kwargs(project),
                    users_great=get_user_ratings(project.users_recommended(organization_id=org.name), 3),
                    users_good=get_user_ratings(project.users_recommended(organization_id=org.name), 2),
                    users_unfit=get_user_ratings(project.users_recommended(organization_id=org.name), 1),
                    user_entries=user_entries,
                )
            )

        return project_cards

    great_project_cards = get_rated_project_cards(
        project_table_full, [p["uuid"] for p in report_data["great_projects"]]
    )
    one_positive_response_project_cards = get_rated_project_cards(
        project_table_full, [p["uuid"] for p in report_data["one_response_projects"]]
    )

    unrated_projects_cards = []
    unfit_projects_cards = []

    user_stats_cards = []

    for _, user_stats in stats_all_users.items():
        user_stats_cards.append(user_stats_template.format(**user_stats))

    for p in report_data["unrated_projects"]:
        unrated_p = s.get(Project, p["uuid"])
        unrated_projects_cards.append(minimal_project_src.format(**get_project_kwargs(unrated_p)))

    for p in report_data["unfit_no_feedback"]:
        unfit_p: Project = s.get(Project, p["uuid"])

        unfit_projects_cards.append(minimal_project_src.format(**get_project_kwargs(unfit_p)))

    def make_grid(template_row, row_items):
        rows = []
        for i in range(math.ceil(len(row_items) / 3)):
            p, p2, p3 = (row_items[i : (i + 1) * 3] + ["", "", ""])[:3]

            row = template_row.format(
                project_1=p,
                project_2=p2,
                project_3=p3,
            )
            rows.append(row)

        return "".join(rows)

    stats_per_user_html = "".join(user_stats_cards)

    great_projects_html = "".join(great_project_cards)
    one_positive_hmtl = "".join(one_positive_response_project_cards)
    unrated_projects_html = make_grid(projects_row_src, unrated_projects_cards)
    unfit_no_feedback_html = make_grid(projects_row_src, unfit_projects_cards)

    other_projects_html = "".join(one_positive_response_project_cards + unrated_projects_cards + unfit_projects_cards)

    # pprint(report_data)
    mvp_username = report_data["most_active_user"]
    mvp_user_dict = {}

    for s in report_data["users_stats"]:
        if s["username"] == mvp_username:
            mvp_user_dict = s
            break

    mvp_user_html = mvp_user_src.format(**mvp_user_dict) if mvp_user_dict else no_mvp_user_src

    if recipient.username not in stats_all_users:
        raise EmailCancelled(f"user {recipient.username} was not found in weekly report, "
                                f"they won't receive a weekly email")

    if report_data["team_rated_projects_percentage"] == 0 and report_data["team_feedback_projects_percentage"] == 0:
        # if team has no data, try to send a weekly feed email
        MODE = 'team_no_activity'

    elif stats_all_users[recipient.username]["user_rated_projects_percentage"] == 0:
        MODE = 'user_no_activity'

    email_args = dict(
        company_name=org.name,
        recipient_name=f"{recipient.firstname} {recipient.lastname}",

        mvp_user_section=mvp_user_html,
        stats_per_user=stats_per_user_html,

        great_projects=great_projects_html or "<h4>No matching projects</h5>",
        one_positive_projects=one_positive_hmtl or "<h4>No matching projects</h5>",

        unfit_projects=unfit_no_feedback_html or "<h4>No matching projects</h5>",
        unrated_projects=unrated_projects_html or "<h4>No matching projects</h5>",

        other_projects=other_projects_html or "<h4>No matching projects</h5>",

        report_url=f"https://terminal.twotensor.com/report/{report.uuid}",
        feed_url="https://terminal.twotensor.com/",
    )

    match MODE:
        case 'regular':
            email_html = email_html_src.format(**email_args)
        case 'user_no_activity':
            email_html = email_user_no_activity.format(**email_args)
        case 'team_no_activity':
            email_html = email_team_no_activity.format(**email_args)
        case _:
            raise ValueError(f'unrecognised weekly email type: {MODE}')


    return email_html, email_html


def make_daily_email(user: ClientUser, projects: list[Project]) -> tuple[str, str]:
    template_daily_plaintext_src = (
        util.project_root() / "const/email_templates" / "daily-email-plaintext.html"
    ).read_text()
    template_daily_src = (util.project_root() / "const/email_templates" / "daily-email-template.html").read_text()

    now = utc_now()
    for p in user.feed_projects:
        logger.trace(f"Project recommended / now: {util.dt_fmt(p.time_recommended)} / {util.dt_fmt(now)}")
        logger.trace(f"Project date / today: {p.time_recommended.date()} / {(now - timedelta(days=1)).date()}")

    projects_with_feedback = [
        p.project.title
        for p in user.feed_projects
        if p.time_recommended < now - timedelta(hours=24)
        and p.time_recommended.date() == (now - timedelta(days=1)).date()
        and p.feedback
    ]

    logger.info(
        f"found {len(projects_with_feedback)} projects with feedback "
        f"for user {user.username}: {''.join(projects_with_feedback)}"
    )

    if projects_with_feedback:
        projects_feedback = random.choices(
            projects_with_feedback, k=random.randint(1, min(2, len(projects_with_feedback)))
        )

        feedback_str = "Greetings, and thank you for your feedback on {projects}.".format(
            projects=" and ".join(projects_feedback)
        )
    else:
        feedback_str = "Greetings."

    # Fund1 | Fund1 and Fund2 | Fund1, Fund2, ..., and FundN
    funds = list(set([f.name for p in projects for f in p.funds]))[:5]
    funds_str = ", and ".join([", ".join(funds[:-1]), funds[-1]]) if len(funds) > 2 else " and ".join(funds)

    project_entries = []
    for p in projects:
        template_daily_project_src = (
            util.project_root() / "const/email_templates" / "daily-project-template.html"
        ).read_text()
        project_args = get_project_kwargs(p)

        # plaintext layout as a backup
        about_plaintext = "\n".join(TextWrapper().wrap(project_args["about"]))
        project_plaintext = f"""{p.title}\n\n{about_plaintext}\n\nview project: {project_args['project_link']}\n"""

        # html project card formatted with project parameters
        project_html = template_daily_project_src.format(
            **project_args,
            recipient_name=f"{user.firstname} {user.lastname}",
        )

        project_entries.append((project_plaintext, project_html))

    projects_plaintext = ""
    projects_html = ""
    for project_plaintext, project_html in project_entries:
        projects_plaintext += "\n" + project_plaintext
        projects_html += project_html

    contents_plaintext = template_daily_plaintext_src.format(
        projects=projects_plaintext,
        recipient_name=f"{user.firstname} {user.lastname}",
        projects_feedback=feedback_str,
        signals=funds_str,
        signature="Anton",
    )

    contents_html = template_daily_src.format(
        projects=projects_html,
        recipient_name=f"{user.firstname} {user.lastname}",
        projects_feedback=feedback_str,
        signals=funds_str,
        signature="Anton",
    )

    return contents_plaintext, contents_html


def make_cancelled_email(s, user: ClientUser, type: str, reason_cancelled: str):
    email = Email(username=user.username,
                  type=type,
                  time_scheduled=utc_now(),
                  plaintext=reason_cancelled,
                  html=reason_cancelled,
                  approved=False)

    s.add(email)
    s.commit()


def gen_email(s, type: str, org: ClientOrganization, user: ClientUser) -> Email:
    plaintext, html = None, None

    match type:
        case "projects_summary_daily":
            logger.debug(
                f"getting projects from "
                f"{util.dt_fmt(datetime.utcnow() - timedelta(hours=24))}"
                f" for user {user.username} from {user.organization_id}",
            )

            daily_projects = (
                s.query(UserProjectAssociation)
                .filter(
                    and_(
                        UserProjectAssociation.username == user.username,
                        UserProjectAssociation.live == True,
                        UserProjectAssociation.time_recommended > utc_now() - timedelta(hours=24),
                    )
                )
                .all()
            )

            if not daily_projects:
                raise EmailCancelled(f"no projects found for user {user.username} "
                                           "in the last 24 hours")

            plaintext, html = make_daily_email(user, [rec.project for rec in daily_projects])
        case "projects_summary_weekly":
            plaintext, html = make_weekly_email(s, org, user)

    email = Email(username=user.username, type=type, time_scheduled=utc_now(), plaintext=plaintext, html=html)

    s.add(email)
    s.commit()

    return email


# ========== EMAIL SENDING ==========


def send_admin_notification(s):
    notification_types = ["feedback_notify", "dataset_request", "terminal_request"]

    notifications = (
        s.query(Email)
        .filter(
            and_(
                or_(*[Email.type == email_type for email_type in notification_types]),
                Email.sent == False,
                Email.time_scheduled <= utc_now(),
            )
        )
        .all()
    )

    logger.debug(f"{'' if (alerts := len(notifications)) else 'not '}"
                 f"found {alerts or 'any '}pending admin notifications")

    for notification in notifications:
        logger.info(f"sending notification to {notification.user}")
        if not (receiver := get_user_email(notification.user)):
            logger.error(f"failed to send notification to user {notification.user}: email invalid")
            continue

        subject = get_subject(Email.type, "Admin notification")

        i, sent = 0, False
        while i < SEND_RETRIES and not sent:
            try:
                sent = smtp_send(
                    subject=subject,
                    contents_plaintext=notification.plaintext,
                    contents_html=notification.html,
                    sender=Address("Nemo from LookingGlass", "nemo", "arbmintel.com"),
                    receiver=receiver,
                )
            except smtplib.SMTPDataError as e:
                if e.smtp_code == 421:
                    retry_in = RETRY_INTERVAL * (i + 1)

                    logger.error(f"SMTP server is busy, will retry in {retry_in} seconds")

                    time.sleep(retry_in)
                    i += 1

                    continue

        # print(f"{notification} sent: {sent}")
        notification.sent = sent

        s.add(notification)
        s.commit()


def send_notification_emails(s, orgs: list[ClientOrganization], type: str, threshold_days: int = 0):
    for org in orgs:
        # isoweekday()'s Mon - Fri is 1 - 5; note this is different from weekday() which is 0 - 6
        if not (org_today := utc_now().astimezone(org.get_timezone()).isoweekday()) < 6:
            logger.info(
                f"Client notification emails are not sent out on weekends" f"({org.name}'s local date is {org_today})"
            )
            continue

        users_not_notified = [
            (
                user,
                notified := any(
                    [
                        e
                        for e in user.emails
                        if e.type == type and e.time_scheduled.date() >= date.today() - timedelta(days=threshold_days)
                    ]
                ),
                all(
                    [notified] +
                    [
                        (not e.approved)
                        for e in user.emails
                        if e.type == type and e.time_scheduled.date() >= date.today() - timedelta(days=threshold_days)
                    ]
                ),\
            )
            for user in org.users
        ]

        user_statuses = f"Users {type} notification status:\n" + "\n".join(
            [f'{u.username:20} {"not " * (not notified) + "error getting " * cancelled}notified'
             for u, notified, cancelled in users_not_notified]
        )
        logger.info(f"sending {type} emails scheduled for {org.name}..." f"\n\n{user_statuses}{HR}")

        emails_sent = 0
        for user in org.users:
            recent_emails = [
                e
                for e in user.emails
                if e.type == type and e.time_scheduled.date() >= date.today() - timedelta(days=threshold_days)
            ]

            # don't send more than one email of the same type
            if any(recent_emails):
                continue

            try:
                email: Email = gen_email(s, type, org, user)
                send_email(s, email, user, get_subject(type))
                emails_sent += 1
            except EmailCancelled as e:
                # if the email is cancelled, log reason and continue
                err_msg = (
                    f"'{type}' email was cancelled "
                    f"for user {user} @ {org.name}.\n"
                    f"Reason: {e}"
                )
                logger.error(err_msg)
            except NotificationError as e:
                # if the error is critical, notify admins
                err_msg = (
                    f"failed sending email of type {type} "
                    f"to user {user} @ {org.name}.\n"
                    f"Error caught: {e}"
                )
                logger.error(err_msg)

                # create a stub email with the error message to prevent repeat notifications
                make_cancelled_email(s, user, type, err_msg)
                notify_email_failed(err_msg)

        logger.info(f"sent {emails_sent} {type} emails for {org.name}{HR}")

@logger.catch
def process_emails():
    with arbm_core.private.Session() as s:
        # send daily emails
        subscribed_orgs = s.query(ClientOrganization).filter(ClientOrganization.membership == "premium").all()

        sorted_orgs = sorted(subscribed_orgs, key=lambda o: (o.local_time(), o.name), reverse=True)

        orgs_send_daily: list[ClientOrganization] = [o for o in sorted_orgs if should_send("projects_summary_daily", o)]

        orgs_with_report = [o for o in subscribed_orgs if o.summary_day]
        orgs_send_report = [o for o in orgs_with_report if should_send("projects_summary_weekly", o)]

        def make_schedule(all_orgs, send_orgs):
            return "\n".join(
                ["{0:20} {1}, ready to send: {2}".format(o.name, util.dt_fmt(o.local_time()), o in send_orgs) for o in all_orgs]
            )

        daily_schedule = make_schedule(sorted_orgs, orgs_send_daily)
        weekly_schedule = make_schedule(orgs_with_report, orgs_send_report)

        # client emails
        logger.info('Daily notifications are disabled for now')
        # logger.info("=== Daily notifications ===\n\n")
        # logger.info(f"Orgs with daily notifications:\n{daily_schedule}\n\n")

        # send_notification_emails(s, orgs_send_daily, "projects_summary_daily")  # daily summary emails

        logger.info("=== Weekly notifications ===\n\n")
        logger.info(f"Orgs with report:\n{weekly_schedule}\n\n")

        send_notification_emails(s, orgs_send_report, "projects_summary_weekly", 5)  # weekly report emails

        # admin emails
        logger.info("=== Admin notifications ===")
        send_admin_notification(s)


if __name__ == "__main__":
    process_emails()
