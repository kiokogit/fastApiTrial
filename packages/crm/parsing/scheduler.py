from sqlalchemy import distinct, select

from arbm_core.private.investors import Fund
from arbm_core.public.users import client_funds_portfolio


from .worker import parse_fund


def score_funds():
    pass
    # get number of subscribers for fund
    # get target deal count (how much we need from the fund, e.g. 50 projects for b2b or 10 projects for startup fund)
    # get count of recent deals
    # get human-defined (override) priority 1 - 3 (1 being absolutely critical funds, 3 being default behaviour)
    # get last parsed date


def choose_next(s):
    """
    """
    # === todo: priority version
    # get all the funds, calculate all priorities
    # get the fund with the highest priority

    # === manual version
    # get all of the published funds
    published = s.scalars(select(distinct(client_funds_portfolio.c.fund_uuid))).all()

    # sort by funds' last parsed dates
    # and pick the least recently parsed fund
    fund_uuid = s.scalars(select(Fund.uuid).
                        where(Fund.uuid.in_(published)).
                        order_by(Fund.last_parsed.asc())
                      ).first()

    # parse all investors for that fund
    parse_fund.delay(fund_uuid=fund_uuid)


'''
we need to track which posts for each investor we have covered, check for the amount of new posts
'''
def schedule_fund_parsing():
    pass