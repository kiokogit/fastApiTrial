from collections import defaultdict
import re
from urllib.parse import urlparse

from arbm_core.private.relationships import LinkedinInvestorActivityAssociation

from . import Base, Deletable
from .investors import Investor
from .relationships import linkedin_post_likers_table, linkedin_profile_projects_table


import pytz
from sqlalchemy import Boolean, BigInteger, Column, DateTime, ForeignKey, Integer, String, Table, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import object_session, relationship, validates


import datetime


linkedin_profiles_not_duplicates = Table(
    "not_duplicates_linkedin_profiles",
    Base.metadata,
    Column("linkedin_profile_a_id", ForeignKey("linkedin_profiles.id"), primary_key=True),
    Column("linkedin_profile_b_id", ForeignKey("linkedin_profiles.id"), primary_key=True),
)


class LinkedinProfile(Base, Deletable):
    __tablename__ = "linkedin_profiles"
    id = Column(Integer, primary_key=True)

    linkedin_id = Column(String)
    linkedin_url = Column(Text)
    urls = relationship("LinkedinUrl", back_populates="profile", lazy='joined')

    name = Column(String(128), nullable=False)

    raw_data = Column(JSONB)
    last_parsed = Column(DateTime(timezone=True))

    # field for tracking which investors are NOT aliases of each other
    not_aliases = relationship("LinkedinProfile", secondary=linkedin_profiles_not_duplicates, back_populates="not_aliases",
                  primaryjoin = id == linkedin_profiles_not_duplicates.c.linkedin_profile_a_id,
                  secondaryjoin = id == linkedin_profiles_not_duplicates.c.linkedin_profile_b_id,
    )

    type = Column(String(50))

    sourced_projects = relationship(
        "LinkedinCompany",
        secondary=linkedin_profile_projects_table,
        primaryjoin=id == linkedin_profile_projects_table.c.source_profile_id,
        secondaryjoin=id == linkedin_profile_projects_table.c.company_profile_id,
        backref="sources",
    )

    liked_posts = relationship("LinkedinPost", secondary=linkedin_post_likers_table, back_populates="likers",
                               lazy='joined')

    __mapper_args__ = {
        "polymorphic_identity": "linkedin_profile",
        "polymorphic_on": type,
    }

    @validates("raw_data")
    def validate_raw_data(self, key, raw_data):
        self.last_parsed = datetime.datetime.now(pytz.UTC)
        return raw_data

    def related_funds(self):
        related_investors = []
        for post in self.liked_posts:
            for i in post.investor_interactions:
                related_investors.append(i.investor)

        funds = []
        for i in related_investors:
            funds.extend(i.funds)
        funds = sorted(funds, key=lambda x: str(x))
        return set(funds)

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,

            'name': self.name,
            'urls': [str(u) for u in self.urls],

            'raw_data': self.raw_data,
            'last_parsed': self.last_parsed,

            'sourced_project_ids': [p.id for p in self.sourced_projects],
            'liked_posts': [p.to_dict() for p in self.liked_posts],
        }


class LinkedinCompany(LinkedinProfile):
    __tablename__ = "linkedin_companies"

    id = Column(Integer, ForeignKey("linkedin_profiles.id",
                                    name="linkedin_companies_id_fkey"),
                primary_key=True)

    tracked_project_id = Column(Integer, ForeignKey("discovered_projects.id", name="fk_linkedin_company_to_project_id"))
    tracked_project = relationship("TrackedProject", back_populates="linkedin_profile")

    source_profiles = relationship(
        "LinkedinProfile",
        secondary=linkedin_profile_projects_table,
        back_populates="sourced_projects",
        lazy='joined'
    )

    date_discovered = Column(DateTime(timezone=True), server_default=func.now())

    # investor_id = Column(Integer, ForeignKey("investor.id", name="fk_linkedin_investor_id"), nullable=False)
    # investor = relationship("Investor", back_populates="linkedin_activity")
    #
    # linkedin_project_id = Column(Integer, ForeignKey("projects_linkedin.id", name="fk_linkedin_activity_to_company_id"))
    # linkedin_project = relationship("LinkedinCompany", back_populates="investor_activity")

    # linkedin data
    founders = Column(Text)

    title = Column(String)
    website = Column(String)
    about = Column(Text)
    logo = Column(String)

    category = Column(String)
    industry = Column(String)
    specialities = Column(String)

    headquarters = Column(String)
    team_size = Column(Integer)
    location = Column(String)
    year = Column(Integer)

    company_size = Column(String)
    company_size_linkedin = Column(Integer)

    stage = Column(String)
    latest_funding = Column(BigInteger)
    last_round_date = Column(DateTime)

    __mapper_args__ = {
        "polymorphic_identity": "linkedin_company",
    }

    def get_activity(self):
        source_signals = {}

        for source in self.sources:
            source_likes_investor = []
            mutual_likes = []
            unknown = []

            for post in source.liked_posts:
                for i in post.investor_interactions:
                    # todo: account for investor likes source

                    if not i.activity_type or i.activity_type == 'unknown':
                        unknown.append(i)
                    elif i.activity_type.lower().strip() == "post":
                        source_likes_investor.append(i)
                    else:
                        mutual_likes.append(i)

            source_signals[f'{source.id}_{source.name}'] = {
                'source_likes_investor': source_likes_investor,
                'mutual_likes': mutual_likes,
                'unknown': unknown,
            }

        return source_signals

    def get_activity_by_fund(self):
        signals_by_source = self.get_activity()

        fund_signals = defaultdict(lambda: defaultdict(int))

        for source, signals_dict in signals_by_source.items():
            for signal_type, signals in signals_dict.items():
                for signal in signals:
                    for fund in signal.investor.funds:
                        fund_signals[fund.name][signal_type] += 1

        return fund_signals

    def get_investor_interactions(self) -> list[LinkedinInvestorActivityAssociation]:
        interactions: list[LinkedinInvestorActivityAssociation] = []

        for s in self.sources:
            for post in s.liked_posts:
                interactions.extend(post.investor_interactions)

        return interactions

    def get_investors(self):
        return set([interaction.investor for interaction in self.get_investor_interactions()])

    def is_valid_candidate(self):
        """
        Check project signals to determine if there's sufficient interaction
        with investor to consider project as candidate
        :return:
        """

        for source in self.sources:
            # todo: if project source is mentioned by investor, if so == strong interaction
            for mention in source.mentions:
                pass

            for post in source.liked_posts:
                if post.like_count > 300:
                    continue

                return True

    def to_dict(self):
        parent_dict = super().to_dict()

        this_dict = {
            'tracked_project_id': self.tracked_project_id,

            'date_discovered': self.date_discovered,

            'sources': [
                # {'name': s.name, 'id': s.id, 'urls': [str(u) for u in s.urls]}
                s.to_dict()
                for s in self.sources
            ],

            'activity_by_fund': self.get_activity_by_fund(),

            'team_size': self.team_size,
            'founders': self.founders,
            'title': self.title,
            'website': self.website,
            'about': self.about,
            'logo': self.logo,
            'category': self.category,
            'industry': self.industry,
            'specialities': self.specialities,
            'headquarters': self.headquarters,
            'location': self.location,
            'year': self.year,
            'company_size': self.company_size,
            'company_size_linkedin': self.company_size_linkedin,
            'stage': self.stage,
            'latest_funding': self.latest_funding,
        }

        parent_dict.update(this_dict)

        return parent_dict

    def __repr__(self):
        return f"{self.name} ({','.join([str(u) for u in self.urls])})"


class LinkedinPersonal(LinkedinProfile):
    __tablename__ = "linkedon_personals"

    id = Column(Integer, ForeignKey("linkedin_profiles.id"), primary_key=True)

    degree = Column(String(30))
    job = Column(Text)

    rejected = Column(Boolean, default=False)

    __mapper_args__ = {
        "polymorphic_identity": "linkedin_personal",
    }

    @property
    def degree_numeric(self):
        return int(re.match(r'(\d)', self.degree).group(1)) or None

    def is_investor(self):
        s = object_session(self)
        return any(s.query(Investor).filter(linkedin_url=self.linkedin_url).one_or_none())

    def to_dict(self):
        parent_dict = super().to_dict()

        this_dict = {
            'degree': self.degree,
            'job': self.job,
            'rejected': self.rejected,
        }

        parent_dict.update(this_dict)
        return parent_dict

    def __repr__(self):
        return f"{self.name} ({','.join([str(u) for u in self.urls])})"


class LinkedinPost(Base):
    __tablename__ = "linkedin_posts"
    id = Column(Integer, primary_key=True)

    post_url = Column(Text)
    parsed_date = Column(DateTime(timezone=True), nullable=False)

    relative_post_date = Column(String(30))

    shared_url = Column(Text, nullable=True)
    shared_company_url = Column(Text, nullable=True)
    shared_profile_url = Column(Text, nullable=True)

    text = Column(Text)

    like_count = Column(Integer)
    comment_count = Column(Integer)
    view_count = Column(Integer)

    likers = relationship("LinkedinProfile", secondary=linkedin_post_likers_table, back_populates="liked_posts")
    likers_parsed_date = Column(DateTime(timezone=True), nullable=True)

    investor_interactions = relationship("LinkedinInvestorActivityAssociation", back_populates="post")

    UniqueConstraint(post_url, name="uq_unique_linkedin_post")

    @validates('post_url')
    def validate_post_url(self, key, post_url):
        parsed_url = urlparse(post_url)
        cleaned_url = parsed_url._replace(query=None)
        return cleaned_url.geturl()

    def estimate_posted_date(self) -> datetime.date:
        if relative_date := self.parse_relative_date():
            return relative_date.date()
        return self.parsed_date.date()

    def parse_relative_date(self) -> datetime.datetime | None:
        if self.relative_post_date is None:
            return

        # add $ symbol because otherwise minutes (m) matches months (mo)
        p_minutes = r'(\d{1,2})m$'
        p_hours = r'(\d{1,2})h$'
        p_days = r'(\d{1,3})d$'
        p_weeks = r'(\d{1,2})w$'
        p_month = r'(\d{1,2})mo$'
        p_years = r'(\d{1,2})y$'

        delta = None
        if re.match(p_minutes, self.relative_post_date)\
            and (count := re.match(p_minutes, self.relative_post_date).group(1)):
            delta = datetime.timedelta(minutes=int(count))

        elif re.match(p_hours, self.relative_post_date)\
            and (count := re.match(p_hours, self.relative_post_date)):
            value = count.group(1)
            delta = datetime.timedelta(hours=int(value))

        elif re.match(p_days, self.relative_post_date)\
            and (count := re.match(p_days, self.relative_post_date)):
            value = count.group(1)
            delta = datetime.timedelta(days=int(value))

        elif re.match(p_weeks, self.relative_post_date)\
            and (count := re.match(p_weeks, self.relative_post_date)):
            value = count.group(1)
            delta = datetime.timedelta(weeks=int(value))

        elif re.match(p_month, self.relative_post_date)\
            and (count := re.match(p_month, self.relative_post_date)):
            value = count.group(1)
            delta = datetime.timedelta(weeks=int(value) * 4)

        elif re.match(p_years, self.relative_post_date)\
            and (count := re.match(p_years, self.relative_post_date)):
            value = count.group(1)
            delta = datetime.timedelta(weeks=int(value) * 52)

        approx_date = (self.parsed_date - delta) if delta else None
        return approx_date

    def likers_parsed(self):
        return len(self.likers)

    def to_dict(self):
        return {
            'id': self.id,
            'url': self.post_url,
            'parsed_date': self.parsed_date,
            'posted_approximately': self.parse_relative_date(),

            'shared_url': self.shared_url,
            'shared_profile_url': self.shared_profile_url,
            'shared_company_url': self.shared_company_url,

            'text': self.text,

            'like_count': self.like_count,
            'comment_count': self.comment_count,
            'view_count': self.view_count,

            # 'likers_parsed': self.likers_parsed(),
            'relative_date': self.parse_relative_date(),

            'investor_interactions': [i.to_dict() for i in self.investor_interactions]
        }

    def __repr__(self):
        post_repr = f"Post id={self.id}"

        post_repr += f', shared from {self.shared_profile_url}' if self.shared_profile_url and self.shared_profile_url != 'NaN' else ''
        post_repr += f', shared from {self.shared_company_url}' if self.shared_company_url and self.shared_company_url != 'NaN' else ''

        return post_repr


class LinkedinUrl(Base):
    __tablename__ = "linkedin_urls"
    id = Column(Integer, primary_key=True)

    profile_id = Column(Integer, ForeignKey("linkedin_profiles.id", name="linkedin_url_to_profile_fk"))
    profile = relationship("LinkedinProfile", back_populates="urls")

    url = Column(Text, nullable=False, unique=True)

    def __repr__(self):
        return self.url


class LinkedinLike(Base):
    """
    Linkedin Like Signal
    """
    __tablename__ = "linkedin_likes"
    profile_url = Column(String, primary_key=True)
    liker_id = Column(String)

    post_id = Column(Integer, ForeignKey('linkedin_posts.id'), primary_key=True)
    post = relationship("LinkedinPost")

    investor_id = Column(Integer, ForeignKey('investor.id'), nullable=False)
    investor = relationship("Investor")

    activity_type = Column(String, nullable=True)

    date_parsed = Column(DateTime, nullable=False, server_default=func.now())

    liker_name = Column(String, nullable=False)
    liker_keyword = Column(String, nullable=False)
    liker_title = Column(String, nullable=False)
    img_id = Column(String)

    processed = Column(Boolean, nullable=False, default=False)
    date_processed = Column(DateTime, nullable=True)

    def __repr__(self):
        return f'Like by {self.liker_name} ({self.liker_keyword}) on' \
               f' Post {self.post_id}'

    def to_dict(self):
        return {
            'post_id': self.post_id,
        'investor_id': self.investor_id,
        'activity_type': self.activity_type,
        'date_parsed': self.date_parsed,
        'liker_name': self.liker_name,
        'liker_keyword': self.liker_keyword,
        'liker_title': self.liker_title,
        'img_id': self.img_id,
        'processed': self.processed,
        'date_processed': self.date_processed
        }