import json

import pytest
from fastapi.testclient import TestClient

from arbm_core.private.investors import Fund
from app import app
from packages.public_api.schemas.project import ProjectSchema


client = TestClient(app)


@pytest.fixture
def make_dummy_project(session):
    dummy_projects = []

    # generate parameterised projects
    def _make_dummy_project(title, website):
        project = Project(
            title=title,
            website=website,
        )

        session.add(project)
        session.commit()

        dummy_projects.append(project)

        return project

    yield _make_dummy_project

    # cleanup
    for p in dummy_projects:
        session.delete(p)

    session.commit()


@pytest.fixture
def dumy_fund(session, make_dummy_project):
    fund = Fund()

    for i in range(10):
        make_dummy_project(f"project {i}", f"https://www.test_project{i}.com")

    session.add(fund)
    session.commit()

    yield fund

    session.delete(fund)
    session.commit()



def test_feed():
    pass


def test_feed_history():
    pass


# search
def test_search():
    pass


def test_dealflow(dumy_fund):
    response = client.post('/v1/funds/{fund_uuid}/dealflow/filter')

    assert response.status_code == 200
    assert isinstance(response.json(), list)

    dealflow = [ProjectSchema(**p) for p in json.loads(response.json())]
    dumy_fund.dealflow