from redis import asyncio as aioredis
from ..config import Config


_connection = None


def get():
    global _connection
    if not _connection:
        _connection = aioredis.from_url(
            Config.REDIS_CONNECTION_STRING, decode_responses=True
        )
    return _connection
