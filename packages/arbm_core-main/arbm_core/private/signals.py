import datetime
from dataclasses import dataclass
from uuid import UUID


@dataclass(eq=True, frozen=True)
class Signal:
    date: datetime.date

    fund_uuid: UUID
    project_uuid: UUID
