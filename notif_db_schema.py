from sqlalchemy import Column, String, Numeric, DateTime, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
import uuid
from enum import Enum
from datetime import datetime


Base = declarative_base()


class Notification(Base):
    __tablename__ = 'notifications'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)

    username = Column(String(100), nullable=False, index=True)

    order_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # Текст уведомления 
    text = Column(String(500), nullable=False)

    # Назначение (куда "полетело")
    email = Column(String(255), nullable=False)

    sent_at = Column(DateTime, nullable=False, default=datetime.utcnow)