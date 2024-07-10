from datetime import datetime, timedelta

import uuid

from sqlalchemy import ARRAY, Boolean, Column, Date, DateTime, Table, ForeignKey, String, Text, Integer, Index, and_
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, validates, object_session
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.hybrid import hybrid_property

import pytz

from arbm_core.core import MongoDb
from arbm_core.core.signals import get_unique_signals_for_project, get_unique_signals_for_fund
from arbm_core.private.investors import Fund
from . import Base, _TAG_ATTRS
from .schemas.project import ProjectSchema


project_to_fund_table = Table(
    "rel_projects_to_funds",
    Base.metadata,
    Column("project_uuid", ForeignKey("all_clients.projects_published.uuid", name="fk_fund_project_uuid"), primary_key=True),
    Column("fund_uuid", ForeignKey("all_clients.funds.uuid", name="fk_project_fund_uuid"), primary_key=True),
    schema="all_clients"
)


class UserProjectAssociation(Base):
    __tablename__ = "project_user_association"
    __table_args__ = {"schema": "all_clients"}

    username = Column(ForeignKey("all_clients.users.username", name="fk_project_user_id"), primary_key=True)
    project_id = Column(ForeignKey("all_clients.projects_published.uuid", name="fk_user_project_id"), primary_key=True)

    user = relationship("ClientUser", back_populates="feed_projects", lazy="immediate")
    project = relationship("Project", back_populates="client_users", lazy="immediate")

    time_recommended = Column(DateTime(timezone=True), nullable=False,
                              default=datetime.now(tz=pytz.UTC))
    revoked = Column(Boolean, default=False)

    favourite = Column(Boolean, nullable=False, default=False)
    contacted = Column(Boolean, nullable=False, default=False)

    rating = Column(Integer)
    feedback = Column(String)
    feedback_posted = Column(DateTime(timezone=True))

    @hybrid_property
    def live(self):
        return (not self.revoked) and self.time_recommended < datetime.now(tz=pytz.UTC)

    @live.expression
    def live(cls):
        return and_(cls.revoked != True, cls.time_recommended < datetime.now(tz=pytz.UTC))

    @hybrid_property
    def archived(self):
        return self.time_recommended < datetime.now(tz=pytz.UTC) - timedelta(hours=24)

    @validates('feedback')
    def validate_feedback(self, key, feedback):
        self.feedback_posted = datetime.now(tz=pytz.UTC)
        return feedback

    def to_dict(self):
        return {
            'username': self.username,
            'project_uuid': self.project_id,

            'time_recommended': self.time_recommended.isoformat(timespec='seconds'),
            'revoked': self.revoked,

            'favourite': self.favourite,
            'rating': self.rating,
            'feedback': self.feedback,
        }

    def __repr__(self):
        return f'Recommended for {self.username}: "{self.project.title}"'


class FundProfile(Base):
    __tablename__ = "funds"
    __table_args__ = {"schema": "all_clients"}

    uuid = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, unique=True, nullable=False)

    logo = Column(Text)

    @property
    def fund_details(self):
        fund_data = MongoDb.funds.find_one({'_id': self.uuid}) or {}
        return {key: fund_data.get(key) for key in Fund._FUND_ATTRS}

    # projects = relationship("Project", secondary=project_to_fund_table, back_populates="funds")
    @property
    def projects(self):
        project_uuids = get_unique_signals_for_fund(MongoDb, self.uuid)
        s = object_session(self)
        projects = [s.get(Project, uuid['_id']) for uuid in project_uuids]
        return projects

    Index("fund_name_unique_lowercased", func.lower("name"), unique=True)


class Contact(Base):
    __tablename__ = "contacts"
    __table_args__ = {"schema": "all_clients"}

    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, primary_key=True)
    project_uuid = Column(ForeignKey("all_clients.projects_published.uuid"), nullable=False)

    project = relationship("Project", back_populates="contacts")

    name = Column(String)
    linkedin = Column(String)
    email = Column(String)

    img = Column(String)
    role = Column(String)

    recommended = Column(Boolean)


class MiscEntry(Base):
    __tablename__ = "project_tags"
    __table_args__ = {"schema": "all_clients"}

    project = relationship("Project", back_populates="tags")

    project_uuid = Column(ForeignKey("all_clients.projects_published.uuid"), primary_key=True)
    title = Column(String, primary_key=True)
    content = Column(String, primary_key=True)

    icon = Column(String)
    category = Column(String)


class SocialEntry(Base):
    __tablename__ = "project_socials"
    __table_args__ = {"schema": "all_clients"}

    project = relationship("Project", back_populates="socials")

    project_uuid = Column(ForeignKey("all_clients.projects_published.uuid"), primary_key=True)
    title = Column(String, primary_key=True)
    url = Column(String, primary_key=True)

    icon = Column(String)


class Project(Base):
    __tablename__ = "projects_published"
    __table_args__ = {"schema": "all_clients"}

    uuid = Column(UUID(as_uuid=True), primary_key=True)
    discovered_date = Column(Date, nullable=True)
    time_published = Column(DateTime(timezone=True), nullable=False, default=datetime.now(tz=pytz.UTC))

    title = Column(String, nullable=False)
    about = Column(String, nullable=False)

    website = Column(String, nullable=False)
    logo = Column(Text)

    industry = Column(String)
    verticals = Column(ARRAY(String), nullable=False)

    markdown_description = Column(String)

    tags = relationship("MiscEntry", back_populates="project", cascade="all, delete-orphan", lazy="immediate")
    socials = relationship("SocialEntry", back_populates="project", cascade="all, delete-orphan", lazy="immediate")
    contacts = relationship("Contact", back_populates="project", cascade="all, delete-orphan", lazy="immediate")

    client_users = relationship("UserProjectAssociation", back_populates="project")

    def __getattr__(self, attr):
        if attr in _TAG_ATTRS:
            for t in self.tags:
                if t.title == attr:
                    return t.content
            return None

        raise AttributeError

    # funds = relationship("FundProfile", secondary=project_to_fund_table, back_populates="projects", lazy='immediate')
    @property
    def funds(self):
        fund_uuids = get_unique_signals_for_project(MongoDb, self.uuid)
        s = object_session(self)
        funds = [s.get(FundProfile, uuid['_id']) for uuid in fund_uuids]
        funds = [f for f in funds if f]
        return funds

    def users_recommended(self, organization_id):
        published_users: list[UserProjectAssociation] = self.client_users
        return [u for u in published_users if (u.revoked == False
                                              and u.user.organization_id == organization_id
                                              and u.live)]

    def to_dict(self):
        return ProjectSchema.from_orm(self).dict()

    def __repr__(self):
        return f'Project "{self.title}"'
