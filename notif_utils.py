from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
import uuid
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status
from notif_config import settings
from kafka_consumer import KafkaCons
from notif_db import AsyncSessionLocal
from notif_models import OrderUpdateMessage
from notif_db_schema import Notification as NotificationSchema
from services import ProfileService


consumer = None


async def handle_order_message(
    message: OrderUpdateMessage
):
    email = await get_profile_email(message.username)
    try:
        notification = NotificationSchema(
            id = uuid.uuid4(),
            username = message.username,
            order_id = message.order_id,
            text = get_notif_text_from_event(message),
            email = email,
            sent_at = datetime.utcnow()
        )

        async with AsyncSessionLocal() as db:
            db.add(notification)
            await db.commit()
            await db.refresh(notification)
        
        print(f"✅ Saved notification for order {message.order_id}")
    except Exception as e:
        print(f"❌ Failed to process order {message.order_id}: {e}")
        raise


def get_notif_text_from_event(
    message: OrderUpdateMessage
):
    if message.event == 'payment_confirmed':
        text = 'Order ' + str(message.order_id) + ' paid!'
    elif message.event == 'payment_failed':
        text = 'Unable to pay for order ' + str(message.order_id) + '. Order cancelled!'
    return text    


async def handle_startup():
    global consumer 
    consumer = KafkaCons()
    await consumer.init_connection(handle_order_message)


async def handle_shutdown():
    await consumer.close()


async def get_profile_email(
    req_uname: str
):
    try:    
        profile_service = ProfileService()

        response = await profile_service.get_profile(req_uname)
    except Exception as e:
        print(f'Failed to get email from Profile service')
        raise

    return response.json().get('email')
    