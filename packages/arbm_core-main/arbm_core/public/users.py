import datetime
import enum
import uuid

import pytz
from sqlalchemy import ARRAY, Boolean, Column, Enum, ForeignKey, Integer, String, DateTime, Date, func, Text, Table
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from . import Base

ORG_ID_LENGTH = 64


user_funds_preference = Table(
    "user_funds_preference",
    Base.metadata,
    Column("fund_uuid", ForeignKey("all_clients.funds.uuid", name="fk_user_prefers_funds"), primary_key=True),
    Column("username", ForeignKey("all_clients.users.username", name="fk_fund_prefered_by_users"), primary_key=True),
    schema="all_clients"
)


client_funds_portfolio = Table(
    "client_funds_portfolio",
    Base.metadata,
    Column("organization_id", ForeignKey("all_clients.organizations.name", name="fk_portfolio_organization_id"), primary_key=True),
    Column("fund_uuid", ForeignKey("all_clients.funds.uuid", name="fk_portfolio_fund_uuid"), primary_key=True),
    schema="all_clients"
)


auto_lists_to_projects = Table(
    "auto_lists_to_projects",
    Base.metadata,
    Column("auto_list_id", ForeignKey("all_clients.auto_project_lists.id", name="fk_auto_lists_to_projects_auto_list_id"), primary_key=True),
    Column("project_id", ForeignKey("all_clients.projects_published.uuid", name="fk_auto_lists_to_projects_project"), primary_key=True),
    Column("matched_on", DateTime),
    schema="all_clients"
)


class MembershipPlan(str, enum.Enum):
    free = 'free'
    trial = 'trial'
    premium = 'premium'


class Email(Base):
    __tablename__ = "emails"
    __table_args__ = {"schema": "all_clients"}

    id = Column(Integer, primary_key=True)

    username = Column(String(64), ForeignKey("all_clients.users.username"), nullable=False)
    user = relationship("ClientUser", back_populates="emails")

    type = Column(String, nullable=False)
    approved = Column(Boolean, nullable=False, default=True)

    plaintext = Column(Text, nullable=False)
    html = Column(Text, nullable=False)

    time_scheduled = Column(DateTime, nullable=False)
    sent = Column(Boolean, nullable=False, default=False)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'approved': self.approved,

            'type': self.type,

            'plaintext': self.plaintext,
            'html': self.html,

            'time_scheduled': self.time_scheduled,
            'sent': self.sent,
        }

    def __repr__(self):
        return f"Email for {self.username} " \
               f"({self.type})"


class ClientOrganization(Base):
    __tablename__ = "organizations"
    __table_args__ = {"schema": "all_clients"}

    name = Column(String(ORG_ID_LENGTH), primary_key=True)
    membership = Column(Enum(MembershipPlan), nullable=False, default=MembershipPlan.premium)

    signup_date = Column(DateTime, nullable=False, server_default=func.now())

    timezone = Column(String)
    summary_day = Column(String)  # when should the summary be sent out

    users = relationship("ClientUser", back_populates="organization",
                                       viewonly=True,
                                       primaryjoin="and_("
                                            "ClientOrganization.name==ClientUser.organization_id,"
                                            "ClientUser.active == True"
                                       ")"
                         )

    funds_portfolio = relationship("FundProfile", secondary=client_funds_portfolio)
    max_funds = Column(Integer, nullable=False, default=0)

    auto_project_lists = relationship("AutoProjectList", back_populates="organization")
    max_auto_lists = Column(Integer, nullable=False, default=3)

    allowed_pages = Column(ARRAY(String), nullable=False, default=['curated_list',
                                                                   'connected_ventures',
                                                                   'team_reports',
                                                                   'signals_search',
                                                                   'deal_sources'])

    reports = relationship("OrganizationReport", back_populates="organization")
    all_users = relationship("ClientUser", back_populates="organization")

    def to_dict(self):
        return {
            'name': self.name,
            'timezone': self.timezone,
            'membership': self.membership,
            'users': [u.username for u in self.users]
        }

    def get_timezone(self, default_tz: datetime.tzinfo = pytz.UTC):
        return pytz.timezone(self.timezone) if self.timezone else default_tz

    def local_time(self) -> datetime.datetime:
        return datetime.datetime.now(pytz.UTC).astimezone(tz=self.get_timezone())

    def __repr__(self):
        return f"{self.name} ({self.membership})"


class OrganizationReport(Base):
    __tablename__ = "reports"
    __table_args__ = {"schema": "all_clients"}

    uuid = Column(UUID(as_uuid=True), unique=True, default=uuid.uuid4, nullable=False, primary_key=True)
    organization_id = Column(String(ORG_ID_LENGTH), ForeignKey("all_clients.organizations.name"), primary_key=True)
    organization = relationship("ClientOrganization", back_populates="reports")

    report_type = Column(String, nullable=False)

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    time_generated = Column(DateTime, nullable=False, server_default=func.now())

    revoked = Column(Boolean, nullable=False, default=False)

    contents = Column(JSONB, nullable=False)

    def __repr__(self):
        return f"Report for {self.organization_id} " \
               f"{self.start_date.strftime('%a, %-d %b')}" \
               f"-{self.end_date.strftime('%a, %-d %b')}"


class ClientUser(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "all_clients"}

    organization_id = Column(String(ORG_ID_LENGTH), ForeignKey("all_clients.organizations.name"), nullable=False)
    organization = relationship("ClientOrganization", back_populates="users")

    username = Column(String(64), primary_key=True)
    email = Column(String, unique=True)
    hashed_password = Column(String)

    firstname = Column(String(64))
    lastname = Column(String(64))

    active = Column(Boolean, nullable=False)

    timezone = Column(String)

    feed_projects = relationship("UserProjectAssociation", back_populates="user")

    created_auto_lists = relationship("AutoProjectList", back_populates="author")
    pipeline_funds = relationship("FundProfile", secondary=user_funds_preference)

    emails = relationship("Email", back_populates="user")
    reachout_template = Column(Text)

    def summary_scheduled(self):
        emails_today = [e for e in self.emails if e.time_scheduled.date() == datetime.datetime.now().date()]
        return emails_today[0].to_dict() if len(emails_today) > 0 else None

    def to_dict(self):
        return {
            'username': self.username,
            'email': self.email,
            'organization': self.organization.to_dict(),

            'feed_project_uuids': [p.project_id for p in sorted(self.feed_projects,
                                                                key=lambda x: x.time_recommended,
                                                                reverse=True)],

            'summary_today': self.summary_scheduled(),
        }

    def __repr__(self):
        return f"{self.username} ({'active' if self.active else 'inactive'})"


class AutoProjectList(Base):
    __tablename__ = "auto_project_lists"
    __table_args__ = {"schema": "all_clients"}

    id = Column(Integer, primary_key=True, autoincrement=True)

    organization_id = Column(String(ORG_ID_LENGTH), ForeignKey("all_clients.organizations.name"), nullable=False)
    organization = relationship("ClientOrganization", back_populates="auto_project_lists")

    active = Column(Boolean, nullable=False, default=False)

    name = Column(String(64), nullable=False)
    prompt = Column(String, nullable=False)

    projects = relationship("Project",
                            #back_populates="auto_lists",
                            secondary=auto_lists_to_projects,
                            order_by=auto_lists_to_projects.c.matched_on.desc())

    created_by = Column(String(64), ForeignKey("all_clients.users.username"), nullable=False)
    author = relationship("ClientUser", back_populates="created_auto_lists")

    created_on = Column(DateTime, nullable=False, server_default=func.now())
    last_edited = Column(DateTime, nullable=True, onupdate=func.now())

    last_run = Column(DateTime, nullable=True)
