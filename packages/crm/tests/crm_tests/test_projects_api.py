


class TestProjectSubmit:
    def test_project_parser_create(self, crm_client):
        crm_client.post('/v1/projects',
                        headers ={},
                        json={}
        )

    def test_project_parser_update(self, crm_client):
        pass

    def test_project_manual_investor_create(self, crm_client):
        pass

    def test_project_manual_investor_update(self, crm_client):
        pass

    def test_project_manual_fund_create(self, crm_client):
        pass

    def test_project_manual_fund_update(self, crm_client):
        pass
