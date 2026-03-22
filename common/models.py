"""SQLAlchemy ORM models for the application."""

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class IngestionAudit(Base):
    """Model for tracking successfully ingested files."""

    __tablename__ = "ingestion_audit"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False, unique=True)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())