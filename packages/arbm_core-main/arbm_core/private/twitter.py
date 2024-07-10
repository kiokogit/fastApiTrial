import json

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    func, select,
)
from sqlalchemy.orm import Session, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

from . import Base

twitter_investor_table = Table(
    "twitter_investor_association",
    Base.metadata,
    Column("investor_id", ForeignKey("investor.id", name="fk_twitter_investor_id"), primary_key=True),
    Column(
        "twitter_project_id", ForeignKey("projects_twitter.user_id", name="fk_investor_twitter_id"), primary_key=True
    ),
)


class TwitterParsingResult(Base):
    __tablename__ = "twitter_parsed"

    id = Column(Integer, primary_key=True)

    # userId,
    user_id = Column(BigInteger, nullable=False, index=True)
    username = Column(String(64), nullable=False)

    query_id = Column(BigInteger, nullable=False)
    query_username = Column(String(64), nullable=False, index=True)

    time_parsed = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    name = Column(String(128))
    created_date = Column(DateTime)

    # website_shortened
    website = Column(Text)
    bio = Column(Text)
    img_url = Column(Text)

    # followers, following, tweets count
    followers = Column(Integer)
    following = Column(Integer)
    tweets_count = Column(Integer)

    # verified, withheld
    verified = Column(Boolean)
    withheld = Column(Text)

    twitter_project_id = Column(BigInteger, ForeignKey("projects_twitter.user_id"), index=True)
    twitter_project = relationship("TwitterProject", back_populates="parsed_data")

    UniqueConstraint(user_id, query_id, time_parsed, name="uq_unique_twitter_parsing_result")

    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'username': self.username,
            'query_id': self.query_id,
            'query_username': self.query_username,
            'time_parsed': self.time_parsed,
            'name': self.name,
            'created_date': self.created_date,
            'website': self.website,
            'bio': self.bio,
            'img_url': self.img_url,
            'followers': self.followers,
            'following': self.following,
            'tweets_count': self.tweets_count,
            'verified': self.verified,
            'withheld': self.withheld,
        }

    def __repr__(self):
        return (
            f"{self.time_parsed} parsed: (id={self.id!r}, project_id={self.twitter_project_id!r}): {self.name!r}"
            f", twitter.com/{self.username!r}"
        )


class TwitterProject(Base):
    __tablename__ = "projects_twitter"

    user_id = Column(BigInteger, primary_key=True)
    discovered_date = Column(DateTime, index=True)
    extracted_date = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    # link to analytics project entry
    tracked_project_id = Column(Integer, ForeignKey("discovered_projects.id", name="fk_twitter_project_id"))
    tracked_project = relationship("TrackedProject", back_populates="twitter")

    UniqueConstraint(tracked_project_id, name="uq_twitter_links_one_project")

    # Twitter profile details
    investors = relationship("Investor", secondary=twitter_investor_table, back_populates="twitter_subscriptions")
    parsed_data = relationship(
        "TwitterParsingResult", back_populates="twitter_project", order_by="TwitterParsingResult.time_parsed.desc()"
    )

    founders = Column(ARRAY(String))
    founders_ids = Column(ARRAY(BigInteger))

    # average_likes
    # average_likes_timespan_days

    # @hybrid_property
    # def book_count(self):
    #     return object_session(self).query(book_writer_association_table).filter(
    #         book_writer_association_table.c.writer_id == self.id).count()
    #
    # @book_count.expression
    # def book_count(cls):
    #     return select([func.count(book_writer_association_table.c.book_id)]).where(
    #         book_writer_association_table.c.writer_id == cls.id).label('book_count')

    @hybrid_property
    def investor_count(self):
        return len(self.investors)

    @investor_count.expression
    def investor_count(cls):
        return select([func.count(twitter_investor_table.c.investor_id)]).where(
            twitter_investor_table.c.twitter_project_id == cls.user_id).label('investor_count')

    @property
    def username(self):
        if len(self.parsed_data) > 0:
            return self.parsed_data[0].username
        return None

    @property
    def parsed_records(self):
        return len(self.parsed_data)

    @property
    def parsing_result(self):
        if len(self.parsed_data) > 0:
            i = 0
            # return latest parsing record for this project id, otherwise return latest parsing record for any
            # of its founders
            while i < len(self.parsed_data) - 1 and self.parsed_data[i].user_id != self.user_id:
                i += 1
            return self.parsed_data[i] if self.parsed_data[i].user_id == self.user_id else self.parsed_data[0]
        return None

    @property
    def first_parsed(self):
        if len(self.parsed_data) > 0:
            return self.parsed_data[-1]
        return None

    @property
    def investor_list(self):
        return set([p.query_username for p in self.parsed_data])

    def get_username(self):
        if len(self.parsed_data) > 0:
            return self.parsed_data[0].username
        return None

    def to_dict(self, shallow=True):
        return {
            'username': self.username,
            'parsing_result': self.parsing_result.to_dict() if self.parsing_result else None,

            'founders': self.founders,
            'founders_ids': self.founders_ids,


            'investor_count': self.investor_count,
            'extracted_date': self.extracted_date.strftime("%-d %b %Y, %H:%M"),
            'first_parsed': self.first_parsed.time_parsed.strftime("%-d %b %Y, %H:%M") if self.first_parsed else None,
            'updated_on': self.parsing_result.time_parsed.strftime("%-d %b %Y, %H:%M") if self.parsing_result else None,

            'profile_data': self.parsing_result.to_dict() if self.parsing_result else {},

            'investors': [i.to_dict() for i in self.investors] if not shallow else [i.id for i in self.investors]
        }

    def __repr__(self):
        return f"Twitter Project {(self.username or 'parsing data not found')}" f"(discovered: {self.discovered_date})"


def insert_parsing_records(s, row, twitter_project_id=None):
    if row['twitter_user_id'] == '':
        return

    stmt = insert(TwitterParsingResult).values(
        user_id=row['twitter_user_id'],
        username=row['twitter_url'],
        query_id=row["query_id"],
        query_username=row["query_username"],
        time_parsed=row["timestamp"],
        name=row["Name"],
        created_date=row["createdAt"] if not pd.isna(row["createdAt"]) else None,
        website=str(row["project_website"]),
        bio=row["bio"],
        img_url=row["imgUrl"],
        followers=row["twitter_followers"],
        following=row["followingCount"] if "followingCount" in row else None,
        tweets_count=row["tweetsCount"],
        verified=row["verified"] if "verified" in row else None,
        withheld=json.dumps(row["withheld"]) if "withheld" in row else None,
        twitter_project_id=row["twitter_user_id"]
        if s.query(TwitterProject).filter_by(user_id=row["twitter_user_id"]).one_or_none() else None,
    )

    stmt = stmt.on_conflict_do_nothing(constraint="uq_unique_twitter_parsing_result")
    res = s.execute(stmt)
    s.commit()
