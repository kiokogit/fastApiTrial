from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY

from . import Base


class User(Base):
    __tablename__ = "web_users"

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)

    role = Column(String)

    email = Column(String, unique=True)
    telegram = Column(String, unique=True)

    notifications = Column(ARRAY(String))