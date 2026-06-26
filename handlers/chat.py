from helpers.database.mongo import Database
from helpers.constants import CHAT_VIEWER_TTL_SECONDS as VIEWER_TTL_SECONDS
from helpers.connection_manager import ConnectionManager
from objects.ws_events import ChatEvents
from objects.user import User
from datetime import datetime
import time


active_chat_viewers: dict[tuple[int, str], dict[str, float]] = {}


active_typing: dict[tuple[int, str], set[str]] = {}
active_recording: dict[tuple[int, str], set[str]] = {}


def _touch(ndcId: int, chatId: str, uid: str):
    key = (ndcId, chatId)
    active_chat_viewers.setdefault(key, {})[uid] = time.monotonic()


def _close_chat(ndcId: int, chatId: str, uid: str):
    key = (ndcId, chatId)
    if key in active_chat_viewers:
        active_chat_viewers[key].pop(uid, None)
        if not active_chat_viewers[key]:
            del active_chat_viewers[key]

    _stop_typing(ndcId, chatId, uid)
    _stop_recording(ndcId, chatId, uid)


def refresh_chat_viewer(uid: str):
    now = time.monotonic()
    for viewers in active_chat_viewers.values():
        if uid in viewers:
            viewers[uid] = now


def _get_viewers(ndcId: int, chatId: str, exclude_uid: str) -> list[str]:
    key = (ndcId, chatId)
    viewers = active_chat_viewers.get(key)
    if not viewers:
        return []

    now = time.monotonic()
    stale = []
    result = []
    for uid, last_seen in viewers.items():
        if now - last_seen > VIEWER_TTL_SECONDS:
            stale.append(uid)
            continue
        if uid != exclude_uid:
            result.append(uid)

    for uid in stale:
        viewers.pop(uid, None)
    if not viewers:
        active_chat_viewers.pop(key, None)

    return result


def _start_typing(ndcId: int, chatId: str, uid: str) -> set[str]:
    key = (ndcId, chatId)
    active_typing.setdefault(key, set()).add(uid)
    return active_typing[key]


def _stop_typing(ndcId: int, chatId: str, uid: str):
    key = (ndcId, chatId)
    if key in active_typing:
        active_typing[key].discard(uid)
        if not active_typing[key]:
            del active_typing[key]


def _start_recording(ndcId: int, chatId: str, uid: str) -> set[str]:
    key = (ndcId, chatId)
    active_recording.setdefault(key, set()).add(uid)
    return active_recording[key]


def _stop_recording(ndcId: int, chatId: str, uid: str):
    key = (ndcId, chatId)
    if key in active_recording:
        active_recording[key].discard(uid)
        if not active_recording[key]:
            del active_recording[key]


async def _get_profiles(ndcId: int, uids: list[str]) -> list[dict]:
    if not uids:
        return []
    db = await Database().init()
    try:
        table = db.get(f"x{ndcId}", "Users")
        profiles = []
        async for row in table.find({"id": {"$in": uids}}):
            profiles.append(User.OwnNonSensetiveProfile(row, ndcId=ndcId, membershipStatus=1))
        return profiles
    finally:
        db.close()


async def _broadcast_chat_action(uid: str, chatId: str, ndcId: int, manager: ConnectionManager,
                                  event_builder, uids_for_profiles: list[str]):
    targets = _get_viewers(ndcId, chatId, uid)
    if not targets:
        return

    profiles = await _get_profiles(ndcId, uids_for_profiles)
    if not profiles:
        return

    event = event_builder(chatId, ndcId, profiles)
    await manager.selective_broadcast(event, targets)


async def on_chat_voice_recording(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    current = _start_recording(ndcId, chatId, uid)
    await _broadcast_chat_action(uid, chatId, ndcId, manager, ChatEvents.recording_start, list(current))


async def on_chat_voice_recording_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _stop_recording(ndcId, chatId, uid)
    await _broadcast_chat_action(uid, chatId, ndcId, manager, ChatEvents.recording_end, [uid])


async def on_chat_message_typing(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    current = _start_typing(ndcId, chatId, uid)
    await _broadcast_chat_action(uid, chatId, ndcId, manager, ChatEvents.typing_start, list(current))


async def on_chat_message_typing_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _stop_typing(ndcId, chatId, uid)
    await _broadcast_chat_action(uid, chatId, ndcId, manager, ChatEvents.typing_end, [uid])


#---------
async def on_chat_screen_open(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _touch(ndcId, chatId, uid)


async def on_chat_screen_close(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _close_chat(ndcId, chatId, uid)


async def on_chatting(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _touch(ndcId, chatId, uid)


async def on_chatting_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    _close_chat(ndcId, chatId, uid)


async def mark_read(data: dict, uid: str):
    if data["o"]["markHasRead"] is True:
        ndcId = data["o"]["ndcId"]
        chatId = data["o"]["threadId"]
        readTimestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        db = await Database().init()
        table = await db.get(f"x{ndcId}", "Chats")
        await table.update_one(
            {"id": chatId},
            {"$set": {f"lastReadedList.{uid}": readTimestamp}},
        )
        db.close()