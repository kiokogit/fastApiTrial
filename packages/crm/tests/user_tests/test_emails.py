import pytest

from freezegun import freeze_time
# from loguru import logger

from notifying.email_scheduler import should_send


class TestShouldSend:
    @pytest.mark.parametrize("report_day", ["monday", "tuesday", "wednesday",
                                            "thursday", "friday", "saturday", "sunday"])
    def test_should_send_on_monday(self, make_client_org, report_day):
        test_org = make_client_org(summary_day=report_day)

        print(test_org, test_org.summary_day)

        with freeze_time("2023-07-03 08:59:00"):
            assert should_send("projects_summary_weekly", test_org) is False

        with freeze_time("2023-07-03 09:15:00"):
            if test_org.summary_day in ['saturday', 'sunday', 'monday']:
                assert should_send("projects_summary_weekly", test_org) is True
            else:
                assert should_send("projects_summary_weekly", test_org) is False