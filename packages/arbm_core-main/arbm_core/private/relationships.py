from sqlalchemy import Table, ForeignKey, Column, String, DateTime
from sqlalchemy.orm import relationship

from . import Base

linkedin_post_likers_table = Table(
    "linkedin_post_likers",
    Base.metadata,
    Column("liker_linkedin_id", ForeignKey("linkedin_profiles.id", name="fk_linkedin_post_liker_id"), primary_key=True),
    Column("linkedin_post_id", ForeignKey("linkedin_posts.id", name="fk_linkedin_liked_post_id"), primary_key=True),
)

linkedin_profile_projects_table = Table(
    "linkedin_profile_projects",
    Base.metadata,
    Column(
        "source_profile_id",
        ForeignKey("linkedin_profiles.id", name="fk_linkedin_source_to_company_id"),
        primary_key=True,
    ),
    Column(
        "company_profile_id",
        ForeignKey("linkedin_companies.id", name="fk_linkedin_company_to_source_id"),
        primary_key=True,
    ),
)

investor_fund_table = Table(
    "investor_fund_association",
    Base.metadata,
    Column("investor_id", ForeignKey("investor.id", name="fk_fund_investor_id"), primary_key=True),
    Column("fund_id", ForeignKey("fund.id", name="fk_investor_fund_id"), primary_key=True),
)


class LinkedinInvestorActivityAssociation(Base):
    __tablename__ = "linkedin_investor_activity_association"

    investor_id = Column(ForeignKey("investor.id", name="fk_linkedin_activity_investor_id"), primary_key=True)
    linkedin_post_id = Column(
        ForeignKey("linkedin_posts.id", name="fk_linkedin_investor_activity_id"), primary_key=True
    )

    investor = relationship("Investor", back_populates="linkedin_activity")
    post = relationship("LinkedinPost", back_populates="investor_interactions", lazy='joined')

    activity_type = Column(String(100))
    discovered_date = Column(DateTime)

    def to_dict(self):
        return {
            'investor_id': self.investor_id,
            'linkedin_post_id': self.linkedin_post_id,
            'activity_type': self.activity_type,
            'discovered_date': self.discovered_date,
        }

    def __repr__(self):
        return f'{self.investor.name}, action="{self.activity_type}", on {self.post}' \
               f' (discovered on {self.discovered_date})'
