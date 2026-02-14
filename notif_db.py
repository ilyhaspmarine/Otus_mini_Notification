from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import os
from urllib.parse import urlunparse
from notif_config import settings


if not settings.db.driver:
    raise ValueError("The DB_DRIVER_SYNC environment variable is not set.")

comp = (
    settings.db.driver,  # scheme
    f"{settings.db.username}:{settings.db.password}@{settings.db.host}:{settings.db.port}",  # netloc
    settings.db.database,  # path
    "",  # params
    "",  # query
    "",  # fragment
)

url = urlunparse(comp)

engine = create_async_engine(url)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def _get_db():
    async with AsyncSessionLocal() as session:
        yield session