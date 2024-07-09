from datetime import date, timedelta, datetime
from pprint import pprint
from sqlalchemy import select, desc

from arbm_core.private import Session
from arbm_core.private.projects import TrackedProject
from arbm_core.private.investors import Fund

from arbm_core.public.users import ClientOrganization, ClientUser, OrganizationReport

from api_external.iscraper import profile_company_details_v3
from projects.linkedin_utils import parse_company_data
from reports.client_reports import compute_report
from projects.schemas.signals import LinkedinPostSignal


def query_project_funds():
    with Session() as s:
        res = s.query(TrackedProject.title)\
              .filter(TrackedProject.interested_funds.any(
                ProjectFundSignal.fund_id >= 248
        )).all()

        pprint(res)


def disable_unpaid_orgs():
    with Session() as s:
        unpaid_orgs: list[ClientOrganization] = s.scalars(select(ClientOrganization)\
                    .filter(ClientOrganization.membership == 'trial',
                       ClientOrganization.signup_date <
                       datetime.now() - timedelta(days=28))).all()

        for org in unpaid_orgs:
            print(org)
            users: list[ClientUser] = org.users
            for user in users:
                user.active = False
                print(user)
            org.membership = 'free'

        s.add_all(unpaid_orgs)
        # s.commit()


def get_company_data():
    data = profile_company_details_v3(profile_id='micromachines-mdpi')
    company_schema = parse_company_data(data)

    signal = LinkedinPostSignal(post_url='https://www.linkedin.com/posts/elaine-watson-b6491712_syntheticbiology-fruitflies-recombinantproteins-ugcPost-7072238791222038528-aZ4g', investor_id=1345, investor_url=None, activity_type='Activity Interaction (Another Investor)', days_since_posted=7, number_of_likes=88, leader_url='https://www.linkedin.com/in/ACoAAAgsorAB6sgtHQmP8yHm9EQ6usSRqqH2WwM', leader_name='Samad Ahadian, PhD')
    pprint(company_schema.dict())

    with Session() as s:
        event, project = create_or_update_manual(s, linkedin_signal=signal,
                    linkedin_details=company_schema)
        print(event + ':')
        pprint(project.to_dict())


def update_project_details():
    pass


def generate_historical_reports():
    with Session() as s:
        org = s.get(ClientOrganization, "Holman")
        mark = s.get(ClientUser, "Mark")

        make_weekly_email(s, org, recipient=mark)

        # last_report = get_report(s, org, type='weekly')
        # print(last_report)
        return


        last_report = s.scalars(select(OrganizationReport)\
                                .filter_by(organization_id=org.name,
                                           report_type='weekly')\
                                .order_by(desc(OrganizationReport.end_date))).first()

        date_from = last_report.end_date
        while True:
            date_from += timedelta(days=1)

            date_to = date_from + timedelta(days=6)

            if date_to >= date.today():
                break

            report = compute_report(s, org.name,
                                    date_from=date_from,
                                    date_to=date_to,
                                    report_type="weekly")

            print(report)
            date_from = date_to

        # s.add(report)
        # s.commit()


def test_should_send():
    with Session() as s:
        org = s.get(ClientOrganization, 'Holman')

        for user in org.users:
            recent_emails = [
                e
                for e in user.emails
                if e.type == 'projects_summary_weekly'
                and (e.time_scheduled.date() >= date.today() - timedelta(days=7))
            ]

            print(f"User {user.username} has {len(recent_emails)} recent emails")


if __name__ == '__main__':
    publish_projects()
    # test_should_send()

    # generate_historical_reports()

    # get_company_data()

    # disable_unpaid_orgs()
    # query_project_funds()