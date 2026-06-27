from helpers.database.redis import get as get_redis
from helpers.constants import TTL_VIEWER


def _key_viewers(ndcId: int, chatId: str) -> str:
    return f"x{ndcId}:chat:{chatId}:viewers"

def _key_user_chats(uid: str) -> str:
    return f"user:{uid}:viewing"


async def _viewer_add(uid: str, ndcId: int, chatId: str):
    redis = get_redis()
    await redis.sadd(_key_viewers(ndcId, chatId), uid)
    await redis.expire(_key_viewers(ndcId, chatId), TTL_VIEWER)
    await redis.sadd(_key_user_chats(uid), f"{ndcId}:{chatId}")
    await redis.expire(_key_user_chats(uid), TTL_VIEWER)


async def _viewer_remove(uid: str, ndcId: int, chatId: str):
    redis = get_redis()
    await redis.srem(_key_viewers(ndcId, chatId), uid)
    await redis.srem(_key_user_chats(uid), f"{ndcId}:{chatId}")


async def refresh_user_viewing(uid: str):
    redis = get_redis()
    index_key = _key_user_chats(uid)
    members = await redis.smembers(index_key)
    if not members:
        return

    await redis.expire(index_key, TTL_VIEWER)
    for entry in members:
        raw = entry.decode() if isinstance(entry, bytes) else entry
        ndcId, chatId = raw.split(":", 1)
        await redis.expire(_key_viewers(ndcId, chatId), TTL_VIEWER)