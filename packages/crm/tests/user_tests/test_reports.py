import datetime
import uuid

import pytest
from freezegun import freeze_time

from arbm_core.public.users import ClientUser
from arbm_core.public.projects import Project, UserProjectAssociation

from reports.client_reports import compute_report


@pytest.fixture
def user_add_project(session):
    feed_projects = []

    def _user_add_project(user: ClientUser, project: Project):
        feed_project = UserProjectAssociation(user=user, project=project)

        session.add(feed_project)
        session.commit()

        feed_projects.append(project)

        return feed_project

    yield _user_add_project

    for p in feed_projects:
        session.delete(p)

    session.commit()


@pytest.fixture
def test_users(make_client_user, make_client_org):
    test_org = make_client_org(summary_day='monday')

    user1 = make_client_user("Bob", test_org)
    user2 = make_client_user("Cameron", test_org)
    user3 = make_client_user("Zigfried", test_org)

    return user1, user2, user3


@pytest.fixture
def test_project(session):
    p = Project(
        uuid=uuid.uuid4(),
        title='test project'
    )

    session.add(p)
    session.commit()

    yield p

    session.delete(p)
    session.commit()


@pytest.fixture
def test_org(make_client_org):
    return make_client_org(summary_day='sunday')


class TestReportGeneration:
    @freeze_time("2023-07-01 09:00:00")
    def test_should_generate_report(self, session, test_org):
        pass

    def test_report_no_params(self, session, test_org):
        org_id = test_org.name

        with pytest.raises(ValueError):
            compute_report(session, org_id, None, None, 'weekly')

        with pytest.raises(ValueError):
            compute_report(session, org_id, None, datetime.date.today(), 'weekly')

        with pytest.raises(ValueError):
            compute_report(session, org_id, datetime.date.today(), None, 'weekly')


    def test_report_invalid_dates(self, session, test_org):
        org_id = test_org.name

        today = datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        yesterday = today - datetime.timedelta(days=1)

        with pytest.raises(ValueError):
            compute_report(session, org_id, tomorrow, today, 'weekly')

        with pytest.raises(ValueError):
            compute_report(session, org_id, today, tomorrow, 'weekly')

        with pytest.raises(ValueError):
            compute_report(session, org_id, today, yesterday, 'weekly')


    def test_no_activity(self, session, test_org):
        org_id = test_org.name
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        assert compute_report(session, org_id, yesterday, today, 'weekly') is None


    def test_report_mvp(self, session, test_org, test_users: list[ClientUser],
                        test_project):
        org_id = test_org.name
        today = datetime.date.today()
        report_start = today - datetime.timedelta(days=7)

        userA, userB, userC = test_users
        pA = UserProjectAssociation(user=userA,
                                    project=test_project,
                                    rating=3)
        pB = UserProjectAssociation(user=userB,
                                    project=test_project,
                                    rating=1)
        pC = UserProjectAssociation(user=userC,
                                    project=test_project,
                                    feedback="feedback without rating")

        session.add_all([userA, userB, userC])
        session.commit()

        # test that no user qualifies for MVP unless project fully ranked
        report = compute_report(session, org_id, report_start, today, 'weekly')
        assert report.contents['most_active_user'] is None

        userA.feed_projects[0].feedback = "feedback with rating"

        report = compute_report(session, org_id, report_start, today, 'weekly')
        assert report.contents['most_active_user'] == userA.username
        assert report.contents['team_rated_projects_percentage'] == 67
        assert report.contents['team_feedback_projects_percentage'] == 67
        assert len(report.contents['great_projects']) == 1
        assert len(report.contents['unfit_no_feedback']) == 1

        session.delete(pA)
        session.delete(pB)
        session.delete(pC)
        session.commit()


    def test_report_scores(self, session, test_org, test_users):
        user1_ratings = [(0, None), (0, None), (0, None), (0, None)]
        user2_ratings = [(0, None), (0, None), (0, None), (0, None)]
        user3_ratings = [(0, None), (0, None), (0, None), (0, None)]

        assert scores["team_rated_projects_percentage"]
        assert scores["team_feedback_projects_percentage"]

