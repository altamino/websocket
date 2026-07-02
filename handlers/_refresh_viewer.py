import time
from datetime import datetime, UTC
from helpers.database.redis import get as get_redis
from helpers.constants import TTL_VIEWER


def _key_viewers(ndcId: int, chatId: str) -> str:
    return f"x{ndcId}:chat:{chatId}:viewers"

def _key_user_chats(uid: str) -> str:
    return f"user:{uid}:viewing"


async def record_user_interaction_time(uid: str):
    redis = get_redis()
    now = time.time()
    last_ts_key = f"user:{uid}:last_active_ts"
    last_ts = await redis.get(last_ts_key)

    today_str = datetime.now(UTC).strftime("%Y-%m-%d")
    sec_key = f"user:{uid}:active_sec:{today_str}"

    if last_ts:
        try:
            elapsed = now - float(last_ts)
            if 0 < elapsed <= 120:
                await redis.incrbyfloat(sec_key, elapsed)
                await redis.expire(sec_key, 172800)  # TTL 2 days
        except (ValueError, TypeError):
            pass

    await redis.set(last_ts_key, str(now), ex=180)


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
    await record_user_interaction_time(uid)
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