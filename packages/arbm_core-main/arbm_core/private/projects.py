import datetime
import enum
from collections import defaultdict
from urllib.parse import urlparse
import pytz

from deprecated import deprecated

import uuid
from loguru import logger
from ordered_enum.ordered_enum import OrderedEnum

from psycopg2._range import DateRange
from sqlalchemy import (
    Boolean,
    BigInteger,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    func, text, ForeignKeyConstraint,
    select
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import UUID, DATERANGE, ExcludeConstraint
from sqlalchemy.orm import relationship, validates, object_session, reconstructor

from arbm_core.core import MongoDb
from arbm_core.core.signals import AddSignal, get_signals_for_project, get_unique_signals_for_project, save_signal
from .relationships import LinkedinInvestorActivityAssociation



from . import Base

from .investors import Fund
from .twitter import TwitterProject
from .linkedin import LinkedinCompany, LinkedinPost
from .signals import Signal


ProjectDetail = tuple[str, str]


class CompanyType(str, enum.Enum):
    startup = 'startup'
    competitors = 'competitors'


# constants

class InvestmentStage(str, OrderedEnum):
    pre_seed = 'Pre-Seed'
    seed = 'Seed'
    series_a = 'Series A'
    series_b = 'Series B'
    series_c = 'Series C'
    series_d = 'Series D'
    ipo = 'IPO'


class ProjectStatus(str, enum.Enum):
    discovered = 'Discovered'

    rejected = 'Rejected'
    not_in_scope = 'Not in scope'

    pending = 'Pending'
    review = 'Under review'

    published = 'Published'
    accepted = 'Accepted'
    uploaded = 'Uploaded'


class ProjectSource(enum.IntEnum):
    twitter = 10
    linkedin = 11


class ProjectIndustry(str, enum.Enum):
    mobility = 'mobility'


# relationships

analytics_project_categories = Table(
    "analytics_project_categories",
    Base.metadata,
    Column("project_id", ForeignKey("project_analytics.project_id",
                                    name="analytics_project_categories_project_id_fkey"),
            primary_key=True),
    Column("category_name", ForeignKey("analytics_categories.name"),
            primary_key=True),
)


leaders_in_projects = Table(
    "leaders_in_projects",
    Base.metadata,
    Column("project_id", ForeignKey("project_analytics.project_id",
                                    name="leaders_in_projects_project_id_fkey"),
                        primary_key=True),
    Column("leader_id", ForeignKey("leaders.id",
                                   name="leaders_in_projects_leader_id_fkey"),
                        primary_key=True),
)


class FieldConfig(Base):
    __tablename__ = "analytics_datapoints"
    field_name = Column(String, primary_key=True)

    enabled = Column(Boolean, nullable=False, default=True)


class DetailsEntry(Base):
    __tablename__ = "project_details"

    id = Column(Integer, primary_key=True)

    project = relationship("ProjectAnalytics", back_populates="details")
    project_id = Column(ForeignKey("project_analytics.project_id"), nullable=False)

    data_source = Column(String, nullable=False)

    type = Column(String, nullable=False)
    value = Column(String, nullable=False)

    effective_dates = Column(DATERANGE, nullable=False)

    __table_args__ = (ExcludeConstraint(
        # don't allow overlapping date ranges
        (Column('effective_dates'), '&&'),

        # for project, only one details value is allowed per type within a date range
        (Column('project_id'), '='),
        (Column('type'), '='),

        name='constr_details_type_uq_in_daterange'
    ),)

    def to_dict(self):
        return {
            'project_id': self.project_id,
            'data_source': self.data_source,

            'type': self.type,
            'value': self.value,

            'effective_from': self.effective_dates.lower,
            'effective_to': self.effective_dates.upper,
        }

    def __repr__(self):
        return f"DetailsField '{self.type}' [sourced from {self.data_source}], effective {self.effective_dates.lower} - {self.effective_dates.upper}"


class ProjectLink(Base):
    __tablename__ = "project_links"
    project_id = Column(Integer, ForeignKey("discovered_projects.id", name="fk_project_id"), primary_key=True)
    project = relationship("TrackedProject", back_populates="links")

    name = Column(String, primary_key=True)
    value = Column(String, nullable=False)

    def to_dict(self):
        return {
            'name': self.name,
            'url': self.value,
        }

    def __repr__(self):
        return f'{self.name}: {self.value}'


class ProjectCategory(Base):
    __tablename__ = "analytics_categories"
    name = Column(String, primary_key=True)
    projects = relationship("ProjectAnalytics", secondary=analytics_project_categories, back_populates="categories")

    def to_dict(self):
        return {
            'name': self.name
        }

    def __repr__(self):
        return f'Vertical "{self.name}"'


class Leader(Base):
    __tablename__ = "leaders"
    id = Column(Integer, primary_key=True)

    name = Column(String)
    linkedin = Column(String)
    email = Column(String)

    role = Column(String)
    img = Column(String)

    recommended = Column(Boolean, default=False, nullable=False)

    projects = relationship("ProjectAnalytics", secondary=leaders_in_projects, back_populates="leaders")

    def to_dict(self):
        return {
            'id': self.id,
            'project_ids': [a.project_id for a in self.projects],
            'name': self.name,
            'email': self.email,
            'linkedin': self.linkedin,

            'role': self.role,
            'img': self.img,
            'recommended': self.recommended,
        }

    def __repr__(self):
        return f'Leader id {self.id} name {self.name} (email: {self.email} linkedin: {self.linkedin})'


class ProjectTagsAssociation(Base):
    __tablename__ = "analytics_project_tags"

    id = Column(Integer, primary_key=True)

    project_id = Column(ForeignKey("project_analytics.project_id",
                                   name="analytics_project_tags_project_id_fkey"),
                        nullable=False)
    tag_type = Column(String, nullable=False)
    tag_name = Column(String, nullable=False)

    project = relationship("ProjectAnalytics", back_populates="tags", foreign_keys=project_id)
    tag = relationship("ProjectTag", back_populates="projects", foreign_keys=[tag_type, tag_name])

    data_source = Column(String, nullable=False)
    effective_dates = Column(DATERANGE, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint((tag_type, tag_name), ["project_tags.type", "project_tags.name"]),
        ExcludeConstraint(
            # don't allow overlapping date ranges
            (Column('effective_dates'), '&&'),

            # for each date range, tag name, type and source must be unique
            (Column('project_id'), '='),

            (Column('tag_type'), '='),
            (Column('tag_name'), '='),

            name='constr_tag_type_uq_in_daterange'
        ),
    )

    def to_dict(self):
        return {
            'project_id': self.project_id,
            'data_source': self.data_source,

            'tag_type': self.tag_type,
            'tag_name': self.tag_name,

            'effective_from': self.effective_dates.lower,
            'effective_to': self.effective_dates.upper,
        }

    def __repr__(self):
        return f'{self.project.project.title} -> {self.tag_type}={self.tag_name}' \
               f' (effective {self.effective_dates.lower}-{self.effective_dates.upper})'


class ProjectAnalytics(Base):
    __tablename__ = "project_analytics"
    project_id = Column(Integer, ForeignKey("discovered_projects.id", name="fk_project_id"), primary_key=True)
    project = relationship("TrackedProject", back_populates="analytics")

    # project type
    founded = Column(Integer)
    previous_exit = Column(Boolean, nullable=False, default=False)
    recent_investment = Column(Boolean, nullable=False, default=False)

    # tags
    stage = Column(Enum(InvestmentStage, name="investment_stage"), nullable=True)
    location = Column(String)

    # details
    funding = Column(Integer)
    last_round = Column(Date)
    last_round_amount = Column(BigInteger)
    team_size = Column(Integer)

    leaders = relationship("Leader", secondary=leaders_in_projects, back_populates="projects")

    # project industry
    industry = Column(Enum(ProjectIndustry))
    categories = relationship("ProjectCategory", secondary=analytics_project_categories, back_populates="projects")

    details = relationship("DetailsEntry", back_populates="project",
                                           viewonly=True,
                                           primaryjoin="and_("
                                                "ProjectAnalytics.project_id==DetailsEntry.project_id,"
                                                "DetailsEntry.effective_dates.contains(func.current_date())"
                                            ")"
                           )
    tags = relationship("ProjectTagsAssociation", back_populates="project",
                                                  viewonly=True,
                                                  primaryjoin="and_("
                                                      "ProjectAnalytics.project_id==ProjectTagsAssociation.project_id,"
                                                      "ProjectTagsAssociation.effective_dates.contains(func.current_date())"
                                                  ")",
                                                  foreign_keys=[ProjectTagsAssociation.project_id]
                                            )

    historic_tags = relationship("ProjectTagsAssociation", back_populates="project", foreign_keys=[ProjectTagsAssociation.project_id])
    historic_details = relationship("DetailsEntry", back_populates="project")

    def _get_detail(self, attr_name: str):
        session = object_session(self)

        return session.query(DetailsEntry).filter(
            DetailsEntry.project_id == self.project_id,
            DetailsEntry.type == attr_name,
            DetailsEntry.effective_dates.contains(datetime.datetime.now(pytz.UTC).date())
        ).one_or_none()

    def _get_tag(self, attr_name: str):
        session = object_session(self)
        return session.query(ProjectTagsAssociation).filter(
                                        ProjectTagsAssociation.project_id==self.project_id,
                                        ProjectTagsAssociation.tag_type==attr_name,
                                        ProjectTagsAssociation.effective_dates.contains(datetime.datetime.now(pytz.UTC).date())
        ).all()

    @property
    def verticals(self):
        return self.get_attr('verticals', 'tag')

    @verticals.setter
    def verticals(self, new_values: tuple[list[str], str]):
        if new_values is None:
            return

        if not isinstance(new_values, tuple):
            raise ValueError("new_values must be a tuple of tags and data source")

        if not isinstance(new_values[0], list):
            raise ValueError("new_values[0] must be a list of tags")

        if not isinstance(new_values[1], str):
            raise ValueError("new_values[1] must be a data source")

        self.update_tags('verticals', new_values[0], new_values[1])

    @property
    def industries(self):
        return self.get_attr('industries', 'tag')

    @industries.setter
    def industries(self, new_values: tuple[list[str], str]):
        if new_values is None:
            return

        if not isinstance(new_values, tuple):
            raise ValueError("new_values must be a tuple of tags and data source")

        if not isinstance(new_values[0], list):
            raise ValueError("new_values[0] must be a list of tags")

        if not isinstance(new_values[1], str):
            raise ValueError("new_values[1] must be a data source")

        self.update_tags('industries', new_values[0], new_values[1])

    # select tags with the latest date
    def get_attr(self, attr_name: str, attr_type: str | None):
        """
        :param attr_name: name of the attribute
        :param attr_type: tag ProjectTagsAssociation, detail for DetailsEntry
        :return:
        """
        match attr_type:
            case None:
                # search in any category
                return self._get_detail(attr_name) or self._get_tag(attr_name)
            case 'tag':
                return self._get_tag(attr_name)
            case 'detail':
                return self._get_detail(attr_name)
            case _:
                raise ValueError("attribute not found")

    def add_tag(self, tag_type: str, tag_name: str, data_source: str):
        session = object_session(self)

        existing_tag = session.query(ProjectTagsAssociation).filter(
            ProjectTagsAssociation.project_id==self.project_id,
            ProjectTagsAssociation.tag_type==tag_type,
            ProjectTagsAssociation.tag_name==tag_name,
            ProjectTagsAssociation.effective_dates.contains(datetime.datetime.now(pytz.UTC).date())
        ).one_or_none()

        if existing_tag:
            return

        tag = session.get(ProjectTag, {'type': tag_type, 'name': tag_name}) \
              or ProjectTag(type=tag_type, name=tag_name)

        project_tag = ProjectTagsAssociation(
            project_id=self.project_id,
            tag=tag,
            data_source=data_source,
            effective_dates=DateRange(lower=datetime.datetime.now(pytz.UTC).date(), upper=None)
        )

        session.add(project_tag)
        session.commit()

    def update_tags(self, tag_type: str, new_values: list[str], data_source: str):
        current_values: list[ProjectTagsAssociation] = self.get_attr(tag_type, 'tag')

        session = object_session(self)
        for current_val in current_values:
            current_val.effective_dates = DateRange(lower=current_val.effective_dates.lower,
                                                    upper=datetime.datetime.now(pytz.UTC).date())
            session.add(current_val)

        new_tags = []
        for v in new_values:
            tag = session.get(ProjectTag, {'type': tag_type, 'name': v})\
                  or ProjectTag(type=tag_type, name=v)

            new_tags.append(ProjectTagsAssociation(
                project_id=self.project_id,
                tag=tag,
                data_source=data_source,
                effective_dates=DateRange(lower=datetime.datetime.now(pytz.UTC).date(), upper=None)
            )
        )

        session.add_all(new_tags)
        session.commit()

    def update_detail(self, attr_name: str, new_value: str, data_source: str):
        current_value: DetailsEntry = self.get_attr(attr_name, 'detail')

        session = object_session(self)

        if current_value is not None:
            if current_value.effective_dates.lower == datetime.datetime.now(pytz.UTC).date():
                logger.error("error while updating a project detail: value with DateRange starting today already exists!"
                             "old value will be deleted...")
                session.delete(current_value)
                session.commit()
            else:
                current_value.effective_dates = DateRange(lower=current_value.effective_dates.lower,
                                                          upper=datetime.datetime.now(pytz.UTC).date())
                session.add(current_value)

        new_detail = DetailsEntry(
            project_id=self.project_id,
            data_source=data_source,
            type=attr_name,
            value=new_value,
            effective_dates=DateRange(lower=datetime.datetime.now(pytz.UTC).date(), upper=None)
        )

        session.add(new_detail)
        session.commit()

    def enriched(self):
        return (self.stage is not None and self.stage != '')

    def __repr__(self):
        return f"Project {self.project_id} analytics" \
               f" enriched={self.enriched()}" \
               f" recent_investment={self.recent_investment}" \
               f" stage={(self.stage.value if self.stage else None)}" \
               f" funding={(self.funding if self.funding else None)}" \
               f" categories={','.join([str(c) for c in self.categories])}"

    def to_dict(self):
        return {
            'project_id': self.project_id,

            'enriched': self.enriched(),

            'recent_investment': self.recent_investment,
            'previous_exit': self.previous_exit,

            'stage': self.stage.value if self.stage else '',
            'funding': self.funding if self.funding else '',

            'last_round': self.last_round.strftime("%Y-%m-%d") if self.last_round else None,
            'last_round_amount': self.last_round_amount,

            'team_size': self.team_size,
            'founded': self.founded,
            'location': self.location,

            'leaders': [l.to_dict() for l in self.leaders],

            'industry': self.industry,
            'categories': [c.to_dict() for c in self.categories],

            'tags': [t.to_dict() for t in self.tags],
            'details': [d.to_dict() for d in self.details],
        }


class ProjectTag(Base):
    __tablename__ = "project_tags"

    # id = Column(Integer, primary_key=True)
    type = Column(String, primary_key=True)
    name = Column(String, primary_key=True)
    # type = Column(String, primary_key=True)
    # name = Column(String, primary_key=True)

    projects = relationship("ProjectTagsAssociation", back_populates="tag", foreign_keys=('[ProjectTagsAssociation.tag_type, ProjectTagsAssociation.tag_name]'))
                                                                            #, foreign_keys=('[ProjectTagsAssociation.project_id,'
                                                                            #              'ProjectTagsAssociation.tag_type,'
                                                                            #               'ProjectTagsAssociation.tag_name]'),)

    Index("tag_name_index", text("lower(name)"), unique=True)
    UniqueConstraint("type", "name", name="uq_tag")


class TrackedProject(Base):
    __tablename__ = "discovered_projects"

    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), unique=True, default=uuid.uuid4, nullable=False)
    title = Column(String, nullable=False)

    project_type = Column(Enum(CompanyType), nullable=False)
    is_startup = Column(Boolean, nullable=False)
    is_b2b = Column(Boolean, nullable=False)

    website = Column(Text)

    logo = Column(String)

    source = Column(Enum(ProjectSource))

    status = Column(Enum(ProjectStatus), default=ProjectStatus.discovered)
    status_changed = Column(DateTime)

    analytics = relationship("ProjectAnalytics", back_populates="project", uselist=False)

    twitter = relationship("TwitterProject", back_populates="tracked_project", uselist=False)
    linkedin_profile = relationship("LinkedinCompany", back_populates="tracked_project", uselist=False)

    links = relationship("ProjectLink", back_populates="project", lazy='immediate')

    # todo: gather all project interactions with funds, investors, etc
    # fund_signals = relationship("ProjectFundSignal", back_populates="project")
    # signals = relationship("ProjectSignal", back_populates="project")

    UniqueConstraint(title, website, name="uq_project")
    UniqueConstraint(website, name="uq_project_website")


    @validates("status")
    def validate_status(self, key, status):
        self.status_changed = datetime.datetime.now(pytz.UTC)
        return status

    @validates("website")
    def validate_website(self, key, website):
        if website is None or not website:
            return None

        website = website.strip().lower()
        urlparse(website, allow_fragments=True)

        return website

    def get_link(self, link_name) -> str | None:
        for l in self.links:
            if l.name == link_name:
                return l.value
        return None

    @hybrid_property
    def discovered_date(self):
        discovered_dates = []

        if self.twitter:
            discovered_dates.append(self.twitter.extracted_date or self.twitter.discovered_date)

        if self.linkedin_profile:
            discovered_dates.append(self.linkedin_profile.date_discovered)

        return min(discovered_dates) if discovered_dates else None

    @discovered_date.expression
    def discovered_date(cls):
        # postgres has a Least function providing alternative to python min
        return func.least(LinkedinCompany.date_discovered, TwitterProject.extracted_date)

    def get_website(self):
        if self.website:
            return self.website

    @property
    def signals(self):
        return get_signals_for_project(MongoDb, self.uuid)

    @property
    def funds(self):
        fund_uuids = list(get_unique_signals_for_project(MongoDb, self.uuid))
        s = object_session(self)
        funds = [s.scalars(select(Fund).where(Fund.uuid == uuid['_id'])).unique().one() for uuid in fund_uuids]
        return funds


    def add_signal(self, signal: AddSignal):
        return save_signal(MongoDb, signal)

    @property
    def description(self):
        analytics: ProjectAnalytics = self.analytics
        if analytics:
            attr = analytics.get_attr('description', 'detail')
            if attr:
                return attr.value

    @description.setter
    def description(self, value: ProjectDetail):
        if not value:
            return
        data_value, data_source = value
        self.analytics.update_detail(attr_name='Description',
                                    new_value=data_value,
                                    data_source=data_source)

    @deprecated(reason="use project.signals instead")
    def compute_linkedin_signals(self) -> list[Signal]:
        if not self.linkedin_profile:
            return []

        signals = []
        investor_interactions: list[LinkedinInvestorActivityAssociation] = self.linkedin_profile.get_investor_interactions()
        for interaction in investor_interactions:
            post: LinkedinPost = interaction.post
            funds: list[Fund] = interaction.investor.funds
            for fund in funds:
                signals.append(
                    Signal(
                        date=post.estimate_posted_date() or interaction.discovered_date,
                        fund_uuid=fund.uuid,
                        project_uuid=self.uuid
                    )
                )

        return signals

    @deprecated(reason="use project.signals instead")
    def compute_twitter_signals(self) -> list[Signal]:
        if not self.twitter:
            return []

        twitter_discovered_date = self.twitter.extracted_date.replace(tzinfo=None).date()
        twitter_funds: list[Fund] = [f for i in self.twitter.investors for f in i.funds]

        signals = []
        for fund in twitter_funds:
            signals.append(
                Signal(
                    date=twitter_discovered_date,
                    fund_uuid=fund.uuid,
                    project_uuid=self.uuid
                )
            )

        return signals

    @deprecated(reason="use project.signals instead")
    def compute_timeline(self) -> list[Signal]:
        linkedin_signals: list[Signal] = self.compute_linkedin_signals()
        twitter_signals: list[Signal] = self.compute_twitter_signals()

        # fund_signals: list[Signal] = [
        #                                 Signal(
        #                                     date=s.discovered_date.date(),
        #                                     fund_uuid=s.fund.uuid,
        #                                     project_uuid=self.uuid
        #                                 )
        #                                 for s in self.fund_signals
        #                               ]

        signals_by_date = defaultdict(list)

        signals_repeating = linkedin_signals + twitter_signals #+ fund_signals
        for s in signals_repeating:
            signals_by_date[s.date].append(s)

        signals_by_date = {k: list(set(v)) for k, v in signals_by_date.items()}

        signals = []
        for v in signals_by_date.values():
            signals.extend(v)

        return sorted(signals, key=lambda s: s.date)

    @property
    @deprecated(reason="use project.funds instead")
    def interested_funds(self):
        funds = set()
        if self.twitter:
            for i in self.twitter.investors:
                for f in i.funds:
                    funds.add(f)

        if self.linkedin_profile:
            for i in self.linkedin_profile.get_investors():
                for f in i.funds:
                    funds.add(f)

        return funds

    @property
    def investor_list(self):
        investors = set()

        if self.twitter:
            investors.union(set(self.twitter.investor_list))

        if self.linkedin_profile:
            investors.union(set(self.linkedin_profile.get_investors()))

        return list(investors)

    def to_dict(self):
        return {
            'id': self.id,
            'uuid': str(self.uuid),

            'analytics': self.analytics.to_dict() if self.analytics else {},

            'discovered_date': self.discovered_date,
            'last_parsed': self.linkedin_profile.last_parsed if self.linkedin_profile else None,

            'title': self.title,
            'website': self.website,
            'logo': self.logo,

            'description': self.description,

            'twitter': self.twitter.to_dict() if self.twitter else None,
            'linkedin': self.linkedin_profile.to_dict() if self.linkedin_profile else None,
            'links': [link.to_dict() for link in self.links] if self.links else [],

            # 'source': self.source.name,
            'status': self.status.name,
            'status_changed': self.status_changed,

            'signals': self.signals,

            'interested_funds': [f.to_dict() for f in self.interested_funds],
            # 'fund_signals': [s.to_dict() for s in self.fund_signals],

        }

    def __repr__(self):
        return (
            f"{self.status.name} project (id={self.id!r}): '{self.title!r}', website={self.website!r}"
        )
