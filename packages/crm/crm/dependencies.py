from enum import Enum
from typing import Annotated
from dotenv import dotenv_values
from pymongo import MongoClient
from pymongo.database import Database

from sqlalchemy.orm import Session
from fastapi import Depends

from arbm_core import private


config = dotenv_values(".env")


def get_full_session():
    public_session = private.Session()
    try:
        yield public_session
    finally:
        public_session.close()


def get_mongodb_client():
    mongodb_client = MongoClient(config["MONGO_URI"], uuidRepresentation="standard")
    database = mongodb_client[config["MONGO_DB_NAME"]]
    try:
        yield database
    finally:
        mongodb_client.close()


MongoDb = Annotated[Database, Depends(get_mongodb_client)]


class RouterTags(Enum):
    clients = "clients"
    funds = "funds"
    investors = "investors"
    logs = "logs"
    projects = "projects"


class PaginationParams:
    # don't allow unbounded limits
    MAX_LIMIT = 1000

    def __init__(self, offset: int = 0, limit: int = 100) -> None:
        self.offset = offset
        self.limit = min(limit, self.MAX_LIMIT)


class QueryParams(PaginationParams):
    def __init__(self, offset: int = 0, limit: int = 100, q: str | None = None) -> None:
        self.q = q
        super().__init__(offset=offset, limit=limit)


DbSession = Annotated[Session, Depends(get_full_session)]

PaginationParams = Annotated[PaginationParams, Depends(PaginationParams)]
QueryParams = Annotated[QueryParams, Depends(QueryParams)]