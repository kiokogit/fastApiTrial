from uuid import UUID

from arbm_core.private.investors import Fund


from pydantic import BaseModel, HttpUrl, create_model


class FundSchema(BaseModel):
    uuid: UUID

    name: str
    website: HttpUrl | None
    logo: str | None

    total_signals: int | None
    signals_quarter: int | None
    signals_month: int | None

    class Config:
        orm_mode = True


# init the schema manually so that the id/uuid fields are displayed first
FundFullSchema = create_model(
    'FundFullSchema',
    __base__=FundSchema,
    **{k: (str, None) for k in Fund._FUND_ATTRS},
) # type: ignore
