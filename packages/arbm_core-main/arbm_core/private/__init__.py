from dataclasses import dataclass
import datetime
import os
from uuid import UUID

from sqlalchemy import create_engine, Column, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

from .. import BooleanModel

Base = declarative_base()

class Deletable:
    removed = Column(Boolean, nullable=False, default=False)

db_host = os.environ['DB_HOST']
db_name = os.environ['DB_NAME']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASS']
db_port = os.environ.get('DB_PORT', 5432)

echo = BooleanModel(enable_echo=os.environ.get('ECHO', False)).enable_echo

engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require", echo=echo, future=True)
Session = sessionmaker(engine, future=True)


@dataclass(eq=True, frozen=True)
class SignalSchema:
    date: datetime.date

    fund_uuid: UUID
    project_uuid: UUID


from . import projects, relationships, twitter, users
