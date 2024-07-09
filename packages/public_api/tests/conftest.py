import os
import pytest

from arbm_core.private import Session

def pytest_generate_tests(metafunc):
    os.environ['DB_HOST']
    os.environ['DB_NAME']
    os.environ['DB_USER']
    os.environ['DB_PASS']
    os.environ['DB_PORT'] = 5432

    os.environ['ECHO'] = 1


@pytest.fixture(scope='session')
def session():
    s = Session()
    yield s
    s.close()