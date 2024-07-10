import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.schema import MetaData

from .. import BooleanModel

from ..private import Base
# Base = declarative_base(metadata=MetaData(schema="all_clients"))

_TAG_ATTRS = ['founded', 'location', 'team_size', 'stage', 'funding', 'last_round', 'last_round_amount']

from . import projects, users
from . import schemas
