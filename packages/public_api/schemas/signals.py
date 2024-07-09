from typing import Any
from uuid import UUID

from pydantic import BaseModel


class FundSchema(BaseModel):
    uuid: UUID

    name: str
    logo: str | None

    fund_details: dict[str, Any] | None

    class Config:
        orm_mode = True
