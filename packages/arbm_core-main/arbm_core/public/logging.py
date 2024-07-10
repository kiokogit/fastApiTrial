from sqlalchemy import Column, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB

from . import Base


class LogItem(Base):
    __tablename__ = "logs"
    __table_args__ = {"schema": "all_clients"}

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    event = Column(String, nullable=False)
    details = Column(JSONB, nullable=False)

    def __repr__(self):
        return f"Log entry #{self.id} at {self.timestamp}; event {self.event}"

    def to_dict(self):
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'event': self.event,
            'details': self.message,
        }
