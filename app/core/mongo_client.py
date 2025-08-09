from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
from app.core.config import settings

MONGO_URI = str(settings.MONGO_URI)

client: Optional[AsyncIOMotorClient] = None


async def get_client() -> AsyncIOMotorClient:
    global client
    if client is None:
        client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=30000)
    return client
