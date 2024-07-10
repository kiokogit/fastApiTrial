from typing import Any

from pydantic import BaseModel
from pydantic.color import Color


class Node(BaseModel):
    id: str
    group: int
    color: Color

    object_data: Any

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash((str(self.id)))


class Link(BaseModel):
    source: str
    target: str

    def __eq__(self, other):
        return self.source == other.source and self.target == other.target

    def __hash__(self):
        return hash((str(self.source) + str(self.target)))


class Graph(BaseModel):
    nodes: list[Node]
    links: list[Link]
