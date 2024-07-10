import abc

from pydantic_mongo import ObjectIdField

from src import PyMongoBase


class GraphEntity(PyMongoBase, abc.ABC):
    pass


class InvestingEntity(GraphEntity):
    id: ObjectIdField = None


class FundedEntity(GraphEntity):
    id: ObjectIdField = None
