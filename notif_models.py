from pydantic import BaseModel, Field, EmailStr
from decimal import Decimal
from uuid import UUID
from datetime import datetime
from typing import Optional


class UserName(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)

class OrderID(BaseModel):
    order_id: UUID

class Event(BaseModel):
    event: str

class NotificationID(BaseModel):
    id: UUID


class OrderUpdateMessage(OrderID, UserName, Event):
    updated_at: datetime = Field() 


class NotificationReturn(NotificationID, UserName):
    email: EmailStr
    sent_at: datetime
    text: str
    class Config:
        from_attributes = True