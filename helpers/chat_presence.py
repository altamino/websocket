import asyncio
from typing import Dict, Set
from helpers.connection_manager import ConnectionManager
from helpers.constants import CHAT_PRESENCE_TIMEOUT as PRESENCE_TIMEOUT


#TODO Remake for redis (we can take info from redis for taking online users ect), also remake ws messages from api server for using redis pub/sub


# uid -> set of chatIds where he now
_user_chats: Dict[str, Set[str]] = {}

# chatId -> set of uids who now in chat
_chat_users: Dict[str, Set[str]] = {}

# (uid, chatId) -> asyncio.TimerHandle
_timeouts: Dict[tuple, asyncio.TimerHandle] = {}


def _remove_user_from_chat(uid: str, chatId: str):
    if uid in _user_chats:
        _user_chats[uid].discard(chatId)
        if not _user_chats[uid]:
            del _user_chats[uid]
    if chatId in _chat_users:
        _chat_users[chatId].discard(uid)
        if not _chat_users[chatId]:
            del _chat_users[chatId]
    _timeouts.pop((uid, chatId), None)


def _reset_timeout(uid: str, chatId: str, manager: ConnectionManager, ndcId: int):
    key = (uid, chatId)
    if key in _timeouts:
        _timeouts[key].cancel()

    loop = asyncio.get_event_loop()
    handle = loop.call_later(
        PRESENCE_TIMEOUT,
        lambda: asyncio.ensure_future(_on_timeout(uid, chatId, manager, ndcId))
    )
    _timeouts[key] = handle


async def _on_timeout(uid: str, chatId: str, manager: ConnectionManager, ndcId: int):
    _remove_user_from_chat(uid, chatId)


def enter_chat(uid: str, chatId: str, manager: ConnectionManager, ndcId: int):
    if uid not in _user_chats:
        _user_chats[uid] = set()
    _user_chats[uid].add(chatId)

    if chatId not in _chat_users:
        _chat_users[chatId] = set()
    _chat_users[chatId].add(uid)

    _reset_timeout(uid, chatId, manager, ndcId)


def leave_chat(uid: str, chatId: str):
    key = (uid, chatId)
    if key in _timeouts:
        _timeouts[key].cancel()
    _remove_user_from_chat(uid, chatId)


def extend_presence(uid: str, chatId: str, manager: ConnectionManager, ndcId: int):
    if chatId in _chat_users and uid in _chat_users[chatId]:
        _reset_timeout(uid, chatId, manager, ndcId)
    else:
        enter_chat(uid, chatId, manager, ndcId)


def get_chat_users(chatId: str) -> Set[str]:
    return _chat_users.get(chatId, set())