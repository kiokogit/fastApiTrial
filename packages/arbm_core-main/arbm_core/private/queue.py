from sqlalchemy import Boolean, Column, String, Integer, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB

from . import Base


class QueuedItem(Base):
    __tablename__ = "parsing_queue"

    object_type = Column(String, primary_key=True)
    object_key = Column(String, primary_key=True)
    data = Column(JSONB, nullable=True)

    time_queued = Column(DateTime(timezone=True), server_default=func.now())

    priority = Column(Integer, nullable=False)

    popped = Column(Boolean, nullable=False, default=False)
    time_popped = Column(DateTime(timezone=True), nullable=True)