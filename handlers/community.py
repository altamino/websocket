from helpers.database.redis import get as get_redis
from helpers.constants import TTL_COMMUNITY_ONLINE


def _key_online(ndcId: str, uid: str) -> str:
    return f"x{ndcId}:online:{uid}"


def _pattern_online(ndcId: str) -> str:
    return f"x{ndcId}:online:*"


async def on_ws_ndc_event(uid: str, ndcId: str):
    redis = get_redis()
    key = _key_online(ndcId, uid)
    await redis.set(key, "1", ex=TTL_COMMUNITY_ONLINE)


async def go_offline_ndc(uid: str, ndcId: str):
    redis = get_redis()
    await redis.delete(_key_online(ndcId, uid))


async def get_online_uids(ndcId: str) -> list[str]:
    redis = get_redis()
    pattern = _pattern_online(ndcId)
    uids = []
    async for key in redis.scan_iter(pattern):
        # key format is x{ndcId}:online:{uid}
        uids.append(key.split(":")[-1])
    return uids


async def is_online(uid: str, ndcId: str) -> bool:
    redis = get_redis()
    return bool(await redis.exists(_key_online(ndcId, uid)))
