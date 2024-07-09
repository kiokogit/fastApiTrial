import datetime

import pytest

import arbm_core.private


@pytest.fixture
def timestamp():
    return datetime.datetime.now().timestamp()


@pytest.fixture
def session():
    s = arbm_core.private.Session()
    yield s
    s.close()