from helpers.database.redis import get as get_redis
from helpers.constants import TTL_COMMUNITY_ONLINE

def _key_online(ndcId: str) -> str:
    return f"x{ndcId}:online"


async def on_ws_ndc_event(uid: str, ndcId: str):
    redis = get_redis()
    key = _key_online(ndcId)
    await redis.sadd(key, uid)
    await redis.expire(key, TTL_COMMUNITY_ONLINE)


async def go_offline_ndc(uid: str, ndcId: str):
    redis = get_redis()
    await redis.srem(_key_online(ndcId), uid)


async def get_online_uids(ndcId: str) -> list[str]:
    redis = get_redis()
    members = await redis.smembers(_key_online(ndcId))
    return [m.decode() if isinstance(m, bytes) else m for m in members]


async def is_online(uid: str, ndcId: str) -> bool:
    redis = get_redis()
    return bool(await redis.sismember(_key_online(ndcId), uid))