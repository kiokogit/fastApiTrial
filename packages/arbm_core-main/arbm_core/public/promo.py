from sqlalchemy import Column, String, DateTime, func

from . import Base


class NewsletterSubscriber(Base):
    __tablename__ = "subscribers"
    __table_args__ = {"schema": "all_clients"}

    email = Column(String, primary_key=True)
    company = Column(String)

    subscribed_on = Column(DateTime, server_default=func.now())


class TerminalRequest(Base):
    __tablename__ = "terminal_requests"
    __table_args__ = {"schema": "all_clients"}

    purpose = Column(String, primary_key=True)
    email = Column(String, primary_key=True)

    inquiry = Column(String, nullable=True)

    phone = Column(String, nullable=True)
    role = Column(String, nullable=True)
    how_found = Column(String, nullable=True)

    full_name = Column(String, nullable=True)
    company = Column(String, nullable=True)

    requested_on = Column(DateTime, server_default=func.now())