from pydantic import BaseModel

from crm.schemas.projects import ProjectSchema


class FeedSchema(BaseModel):
    results_count: int
    projects: list[ProjectSchema]

    const: dict

    # todo: implement query caching
    # - query_uuid - unique identifier for a query
    # - offset / page - pagination