from loguru import logger



def test_get_fund(crm_client):
    response = crm_client.get('/v1/common/funds/100')

    assert response.status_code == 200
    assert response.json() == {'id': 217,
                               'uuid': 'c71d9b4a-8b9f-4f81-89a5-0ad097be277c',
                               'name': 'Test fund',
                               'type': 'T1 FUND',
                               'logo': None,
                               'enabled': True,
                               'priority': 0}


def test_patch_fund(crm_client):
    response = crm_client.patch(
        '/v1/common/funds/100',
        headers={},
        json={
            # "id": 100,
            "name": "Other test name"
        },
    )

    logger.error(response.json())

    assert response.status_code == 200
    # debug(response.json())