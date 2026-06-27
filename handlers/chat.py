from helpers.database.mongo import Database
from helpers.constants import (
    TTL_CHATTING, TTL_RECORDING, TTL_TYPING, TTL_VIEWER
)
from helpers.connection_manager import ConnectionManager
from objects.ws_events import ChatEvents
from objects.user import User
from datetime import datetime
from helpers.connection_manager import broadcast_ws_message
from helpers.database.redis import get as get_redis


def _key_typing(ndcId: int, chatId: str)    -> str: return f"x{ndcId}:chat:{chatId}:typing"
def _key_recording(ndcId: int, chatId: str) -> str: return f"x{ndcId}:chat:{chatId}:recording"
def _key_viewers(ndcId: int, chatId: str)   -> str: return f"x{ndcId}:chat:{chatId}:viewers"
def _key_chatting(ndcId: int, chatId: str)  -> str: return f"x{ndcId}:chat:{chatId}:chatting"
 

async def _redis_set_add_with_ttl(key: str, uid: str, ttl: int):
    redis = get_redis()
    await redis.sadd(key, uid)
    await redis.expire(key, ttl)
 


async def _redis_set_remove(key: str, uid: str):
    redis = get_redis()
    await redis.srem(key, uid)
 
 
async def _redis_set_members(key: str) -> list[str]:
    redis = get_redis()
    members = await redis.smembers(key)
    return [m.decode() if isinstance(m, bytes) else m for m in members]
 
 
async def _refresh_chatting(uid: str, chatId: str, ndcId: int):
    redis = get_redis()
    key = _key_chatting(ndcId, chatId)
    await redis.sadd(key, uid)
    await redis.expire(key, TTL_CHATTING)


async def _get_profiles(ndcId: int, uids: list[str]) -> list[dict]:
    if not uids:
        return []
    db = await Database().init()
    try:
        table = await db.get(f"x{ndcId}", "Users")
        profiles = []
        async for row in table.find({"id": {"$in": uids}}):
            profiles.append(User.OwnNonSensetiveProfile(row, ndcId=ndcId, membershipStatus=1))
        return profiles
    finally:
        await db.close()



async def _broadcast_state(ndcId: int, chatId: str):
    viewers   = await _redis_set_members(_key_viewers(ndcId, chatId))
    typing    = await _redis_set_members(_key_typing(ndcId, chatId))
    recording = await _redis_set_members(_key_recording(ndcId, chatId))
 
    if not viewers:
        return
 
    if typing:
        profiles = await _get_profiles(ndcId, typing)
        await broadcast_ws_message(
            ChatEvents.typing_start(chatId, ndcId, profiles),
            uids=viewers,
        )
 
    if recording:
        profiles = await _get_profiles(ndcId, recording)
        await broadcast_ws_message(
            ChatEvents.recording_start(chatId, ndcId, profiles),
            uids=viewers,
        )





 
async def on_chat_voice_recording(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_add_with_ttl(_key_recording(ndcId, chatId), uid, TTL_RECORDING)
    await _refresh_chatting(uid, chatId, ndcId)
 
    uids     = await _redis_set_members(_key_recording(ndcId, chatId))
    profiles  = await _get_profiles(ndcId, uids)
    viewers   = await _redis_set_members(_key_viewers(ndcId, chatId))
 
    await broadcast_ws_message(
        ChatEvents.recording_start(chatId, ndcId, profiles),
        uids=viewers or None,
    )
 
 
async def on_chat_voice_recording_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_remove(_key_recording(ndcId, chatId), uid)
    await _refresh_chatting(uid, chatId, ndcId)
 
    uids     = await _redis_set_members(_key_recording(ndcId, chatId))
    profiles  = await _get_profiles(ndcId, uids)
    viewers   = await _redis_set_members(_key_viewers(ndcId, chatId))
 
    await broadcast_ws_message(
        ChatEvents.recording_end(chatId, ndcId, profiles),
        uids=viewers or None,
    )
 
 
async def on_chat_message_typing(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_add_with_ttl(_key_typing(ndcId, chatId), uid, TTL_TYPING)
    await _refresh_chatting(uid, chatId, ndcId)
 
    uids     = await _redis_set_members(_key_typing(ndcId, chatId))
    profiles  = await _get_profiles(ndcId, uids)
    viewers   = await _redis_set_members(_key_viewers(ndcId, chatId))
 
    await broadcast_ws_message(
        ChatEvents.typing_start(chatId, ndcId, profiles),
        uids=viewers or None,
    )
 
 
async def on_chat_message_typing_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_remove(_key_typing(ndcId, chatId), uid)
    await _refresh_chatting(uid, chatId, ndcId)
 
    uids     = await _redis_set_members(_key_typing(ndcId, chatId))
    profiles  = await _get_profiles(ndcId, uids)
    viewers   = await _redis_set_members(_key_viewers(ndcId, chatId))
 
    await broadcast_ws_message(
        ChatEvents.typing_end(chatId, ndcId, profiles),
        uids=viewers or None,
    )
 
 

async def on_chat_screen_open(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_add_with_ttl(_key_viewers(ndcId, chatId), uid, TTL_VIEWER)
    await _refresh_chatting(uid, chatId, ndcId)

    await _broadcast_state(ndcId, chatId)
 
 
async def on_chat_screen_close(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    key_v = _key_viewers(ndcId, chatId)
    await _redis_set_remove(key_v, uid)
 
    await _redis_set_remove(_key_typing(ndcId, chatId), uid)
    await _redis_set_remove(_key_recording(ndcId, chatId), uid)
    await _redis_set_remove(_key_chatting(ndcId, chatId), uid)

    await _broadcast_state(ndcId, chatId)
 
 

 
async def on_chatting(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    """Экран снова активен — продлеваем присутствие."""
    await _refresh_chatting(uid, chatId, ndcId)
    redis = get_redis()
    if await redis.sismember(_key_viewers(ndcId, chatId), uid):
        await redis.expire(_key_viewers(ndcId, chatId), TTL_VIEWER)
 
 
async def on_chatting_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    await _redis_set_remove(_key_chatting(ndcId, chatId), uid)
 
async def mark_read(data: dict, uid: str):
    if data["o"]["markHasRead"] is True:
        ndcId   = data["o"]["ndcId"]
        chatId  = data["o"]["threadId"]
        readTimestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        db = await Database().init()
        table = await db.get(f"x{ndcId}", "Chats")
        await table.update_one(
            {"id": chatId},
            {"$set": {f"lastReadedList.{uid}": readTimestamp}},
        )
        await db.close()