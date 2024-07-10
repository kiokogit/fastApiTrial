import enum

from sqlalchemy import Column, String, Integer, DateTime, func, Enum
from sqlalchemy.dialects.postgresql import JSONB

from . import Base


class EventType(str, enum.Enum):
    info = 'info'
    error = 'error'


class LogEntry(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    type = Column(Enum(EventType), nullable=False)
    module = Column(String, nullable=False)
    event = Column(String, nullable=False)
    message = Column(JSONB, nullable=False)

    def __repr__(self):
        return f"Log entry #{self.id} at {self.timestamp}; event {self.event} inside {self.module}"

    def to_dict(self):
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'type': self.type,
            'module': self.module,
            'event': self.event,
            'message': self.message,
        }
