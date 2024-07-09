import pytest

import arbm_core
from arbm_core.public.users import ClientOrganization, ClientUser

from util import utc_now


@pytest.fixture
def timestamp():
    return utc_now().timestamp()


@pytest.fixture(scope='session')
def session():
    s = arbm_core.private.Session()
    yield s
    s.close()


@pytest.fixture
def make_client_org(session, timestamp):
    fixture_orgs = []

    def _client_org(**kwargs):
        org_kwargs = dict(name=f'test_{timestamp}_organiztion', membership='premium')
        org_kwargs.update(kwargs)

        org = ClientOrganization(**org_kwargs)

        session.add(org)
        session.commit()
        return org

    yield _client_org

    for org in fixture_orgs:
        session.delete(org)

    session.commit()


@pytest.fixture
def make_client_user(session, timestamp):
    fixture_users = []

    def _client_user(username: str, user_org: ClientOrganization):
        user = ClientUser(organization=user_org,
                          username=f'test_{timestamp}_{username}',
                          active=True)
        session.add(user)
        session.commit()
        fixture_users.append(user)
        return user

    yield _client_user

    for user in fixture_users:
        session.delete(user)

    session.commit()
