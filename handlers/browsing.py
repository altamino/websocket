from helpers.database.redis import get as get_redis
from helpers.constants import TTL_COMMUNITY_ONLINE



def parse_browsing_target(target: str) -> tuple[str, str | None]:
    # ndc://x3/featured -> ("featured", None)
    # ndc://x3/blog/{uuid} -> ("blog", uuid)
    # ndc://x3/public-chats -> ("public-chats", None)
    # ndc://x3/user-profile/{uuid} -> ("user-profile", uuid)
    path = target.split("/", 3)[-1]  # after ndc://x{ndcId}/
    parts = path.split("/", 1)
    kind = parts[0]
    target_id = parts[1] if len(parts) > 1 else None
    return kind, target_id




def _key_browsing(ndcId: str, kind: str, target_id: str, uid: str) -> str:
    return f"x{ndcId}:browsing:{kind}:{target_id}:{uid}"


def _pattern_browsing(ndcId: str, kind: str, target_id: str) -> str:
    return f"x{ndcId}:browsing:{kind}:{target_id}:*"


async def on_browsing_start(uid: str, ndcId: str, kind: str, target_id: str | None):
    redis = get_redis()
    key = _key_browsing(ndcId, kind, target_id or "-", uid)
    await redis.set(key, "1", ex=TTL_COMMUNITY_ONLINE)


async def on_browsing_end(uid: str, ndcId: str, kind: str, target_id: str | None):
    redis = get_redis()
    await redis.delete(_key_browsing(ndcId, kind, target_id or "-", uid))


async def get_browsing_uids(ndcId: str, kind: str, target_id: str | None) -> list[str]:
    redis = get_redis()
    pattern = _pattern_browsing(ndcId, kind, target_id or "-")
    uids = []
    async for key in redis.scan_iter(pattern):
        if isinstance(key, bytes):
            key = key.decode()
        uids.append(key.split(":")[-1])
    return uids