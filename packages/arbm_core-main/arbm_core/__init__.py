from pydantic import BaseModel


class BooleanModel(BaseModel):
    enable_echo: bool