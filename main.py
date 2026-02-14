from fastapi import Depends, FastAPI, HTTPException, status
from notif_models import OrderUpdateMessage, NotificationReturn
from notif_db import _get_db
import notif_utils as utils
import uuid
from sqlalchemy.future import select
from typing import List
from notif_db_schema import Notification as NotificationSchema


app = FastAPI(title="Notification Service", version="1.0.0")


@app.on_event("startup")
async def on_startup():
    await utils.handle_startup()


@app.on_event("shutdown")
async def on_shutdown():
    await utils.handle_shutdown()


@app.get('/health', summary='HealthCheck EndPoint', tags=['Health Check'])
def healthcheck():
    return {'status': 'OK'}


@app.get("/user/{req_uname}", summary = 'Get notifications for user', tags=['Notofications'], response_model=List[NotificationReturn])
async def get_notifications_for_user(
    req_uname: str,
    db = Depends(_get_db)
):
    result = await db.execute(select(NotificationSchema).filter(NotificationSchema.username == req_uname))
    notifications = result.scalars().all()
    return notifications

@app.get("/order/{order_id}", summary = 'Get notifications for user', tags=['Notofications'], response_model=List[NotificationReturn])
async def get_notifications_for_order(
    order_id: uuid.UUID,
    db = Depends(_get_db)
):
    result = await db.execute(select(NotificationSchema).filter(NotificationSchema.order_id == order_id))
    notifications = result.scalars().all()
    return notifications