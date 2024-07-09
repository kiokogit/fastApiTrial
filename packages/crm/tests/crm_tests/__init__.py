import pytest
from fastapi.testclient import TestClient

from crm.crm_api import app


@pytest.fixture
def get_token():
    pass


@pytest.fixture
def crm_client():
    return TestClient(app)
