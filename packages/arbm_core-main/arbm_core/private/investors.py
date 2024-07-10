from functools import partial

import pandas as pd

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship


import uuid
from collections import Counter

from arbm_core.core import MongoDb
from arbm_core.core.signals import get_signals_for_fund
from arbm_core.private import SignalSchema


from . import Base, Deletable
from .relationships import investor_fund_table
from .twitter import twitter_investor_table


class Fund(Base):
    __tablename__ = "fund"
    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), unique=True, default=uuid.uuid4, nullable=False)

    name = Column(Text, unique=True, nullable=False)
    website = Column(String, unique=True, nullable=True)

    type = Column(String)

    thesis = Column(String)

    published = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, nullable=False, default=0)

    logo = Column(Text)

    investors = relationship("Investor", secondary=investor_fund_table, back_populates="funds")

    last_parsed = Column(DateTime(timezone=True))

    # interacted_projects = relationship("ProjectFundSignal", back_populates="fund")

    Index("fund_name_unique_lowercased", func.lower("name"), unique=True)

    # total_signals
    # signals_per_quarter

    _FUND_ATTRS = list(set(['about',
                    'about_partners',
                    'motivation',
                    'team_size',
                    'team_locations',
                    'investments (number)',
                    'rounds_this_quarter',
                    'notable_investments',
                    'industry_focus',
                    'recent_co_investors',
                    'founded_year',
                    'total_aum',
                    'value_predicted',
                    'lead_days',
                    'average_lead_time',
                    # todo: compute the fields below dynamically
                    'investments_predicted',
                    ]))

    def __new__(cls):
        for attr in cls._FUND_ATTRS:
            setattr(cls, attr, property(
                                fget=partial(Fund.get_attr, attr_name=attr),
                                fset=partial(Fund.set_attr, attr_name=attr)
                            )
            )
        return object.__new__(cls)

    @property
    def fund_details(self):
        fund_data = MongoDb.funds.find_one({'_id': self.uuid}) or {}
        return {key: fund_data.get(key) for key in self._FUND_ATTRS}

    def get_attr(self, *, attr_name: str):
        if attr_name not in self._FUND_ATTRS:
            raise ValueError(f'Invalid fund attribute: {attr_name}')

        fund_data = MongoDb.funds.find_one({'_id': self.uuid}) or {}

        return fund_data.get(attr_name)

    def set_attr(self, value, *, attr_name: str):
        if attr_name not in self._FUND_ATTRS:
            raise ValueError(f'Invalid fund attribute: {attr_name}')

        # print(f'Fund.set_attr({attr_name}, {value}): setting {attr_name} to {value}')

        res = MongoDb.funds.update_one({'_id': self.uuid}, {'$set': {attr_name: value}}, upsert=True)
        # (res.modified_count)
        # print(res.raw_result)

    @property
    def total_signals(self):
        signals = self.compute_signals()

        return sum([year['total'] for year in signals])

    @property
    def signals_quarter(self):
        signals = self.compute_signals()

        months = [month['total'] for year in reversed(signals) for month in reversed(year['months'])]

        if not months:
            return 0

        last_quarter = sum(months[:3])

        return last_quarter

    @property
    def signals_month(self):
        signals = self.compute_signals()

        if not signals:
            return 0

        year = signals[-1]
        last_month = year['months'][-1]

        return last_month['total']

    def compute_signals(self, cutoff: DateTime | None = None) -> list[SignalSchema]:
        return get_signals_for_fund(MongoDb, self.uuid, cutoff=cutoff)

    def compute_signals_old(self) -> list[SignalSchema]:
        signals = []

        for investor in self.investors:
            for interaction in investor.linkedin_activity:
                post = interaction.post

                for founder in post.likers:
                    for company_linkedin in founder.sourced_projects:
                        project = company_linkedin.tracked_project

                        signals.append(
                            SignalSchema(
                                # really this should be always valid for date
                                date=post.estimate_posted_date() \
                                        # we put this as a fallback option.
                                        # additionally this *might* be more precised
                                        # if e.g. like was picked up *after* the post was first parsed
                                        or interaction.discovered_date,
                                fund_uuid=self.uuid,
                                project_uuid=project.uuid
                            )
                        )

        return signals

    def get_activity_repeats(self):
        fund_activity = Counter()

        for investor in self.investors:
            fund_activity += investor.get_activity_repeats()

        return fund_activity

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,

            'enabled': self.enabled,
            'priority': self.priority,

            'logo': self.logo,

            'investor_ids': [i.id for i in self.investors]
        }

    def __repr__(self):
        return " ".join([
            f"{self.name}",
           (f"({self.type})" if self.type else ""),
            f"[{self.priority}]"
         ])


class Investor(Base, Deletable):
    __tablename__ = "investor"
    id = Column(Integer, primary_key=True)
    name = Column(String)

    role = Column(String)
    investor_type = Column(String)

    type = Column(String)

    funds = relationship("Fund", secondary=investor_fund_table, back_populates="investors")

    twitter_url = Column(String, nullable=True)
    linkedin_url = Column(String, nullable=True, index=True, unique=False)

    linkedin_last_parsed = Column(DateTime)
    twitter_followed = Column(Boolean, default=False)

    twitter_subscriptions = relationship("TwitterProject", secondary=twitter_investor_table, back_populates="investors")
    linkedin_activity = relationship("LinkedinInvestorActivityAssociation", back_populates="investor")

    @property
    def projects_followed(self):
        return len(self.twitter_subscriptions)

    def get_activity_repeats(self):
        urls = []
        for interaction in self.linkedin_activity:
            if (target_url := interaction.post.shared_url) and not pd.isna(interaction.post.shared_url):
                urls.append(target_url)
            if (target_url := interaction.post.shared_company_url) and not pd.isna(interaction.post.shared_company_url):
                urls.append(target_url)
            if (target_url := interaction.post.shared_profile_url) and not pd.isna(interaction.post.shared_profile_url):
                urls.append(target_url)
        return Counter(urls)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,

            'removed': self.removed,

            'role': self.role,
            'investor_type': self.investor_type,

            'funds': [f.to_dict() for f in self.funds],

            'twitter_url': self.twitter_url,
            'linkedin_url': self.linkedin_url,

            'linkedin_last_parsed': self.linkedin_last_parsed,
            'twitter_followed': self.twitter_followed,

            'linkedin_activity': [a.to_dict() for a in self.linkedin_activity],
            'twitter_subscriptions': [s.to_dict(shallow=True) for s in self.twitter_subscriptions],
        }

    def __repr__(self):
        return f"{self.name} @ {','.join([str(f) for f in self.funds])}"