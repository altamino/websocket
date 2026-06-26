from helpers.database.mongo import Database
from helpers.connection_manager import ConnectionManager
from helpers.chat_presence import (
    enter_chat, leave_chat, extend_presence, get_chat_users
)
from objects.ws_events import ChatEvents
from datetime import datetime


async def on_chat_screen_open(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    enter_chat(uid, chatId, manager, ndcId)


async def on_chat_screen_close(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    leave_chat(uid, chatId)


async def on_chat_voice_recording(uid: str, chatId: str, ndcId: int, reqid: str, threadType: int, manager: ConnectionManager):
    extend_presence(uid, chatId, manager, ndcId)
    users = get_chat_users(chatId) - {uid}
    if users:
        await manager.selective_broadcast(ChatEvents.recording_start(uid, chatId, ndcId, reqid, threadType), list(users))


async def on_chat_voice_recording_end(uid: str, chatId: str, ndcId: int, reqid: str, threadType: int, duration: int, manager: ConnectionManager):
    users = get_chat_users(chatId) - {uid}
    if users:
        await manager.selective_broadcast(ChatEvents.recording_end(uid, chatId, ndcId, reqid, duration, threadType), list(users))


async def on_chat_message_typing(uid: str, chatId: str, ndcId: int, reqid: str, threadType: int, manager: ConnectionManager):
    extend_presence(uid, chatId, manager, ndcId)
    users = get_chat_users(chatId) - {uid}
    if users:
        await manager.selective_broadcast(ChatEvents.typing_start(uid, chatId, ndcId, reqid, threadType), list(users))


async def on_chat_message_typing_end(uid: str, chatId: str, ndcId: int, reqid: str, threadType: int, duration: int, manager: ConnectionManager):
    users = get_chat_users(chatId) - {uid}
    if users:
        await manager.selective_broadcast(ChatEvents.typing_end(uid, chatId, ndcId, reqid, duration, threadType), list(users))


async def on_chatting(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    extend_presence(uid, chatId, manager, ndcId)


async def on_chatting_end(uid: str, chatId: str, ndcId: int, manager: ConnectionManager):
    leave_chat(uid, chatId)


async def mark_read(data: dict, uid: str):
    if data["o"]["markHasRead"] is True:
        ndcId = data["o"]["ndcId"]
        chatId = data["o"]["threadId"]
        readTimestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        db = await Database().init()
        table = db.get(f"x{ndcId}", "Chats")
        await table.update_one(
            {"id": chatId},
            {"$set": {f"lastReadedList.{uid}": readTimestamp}},
        )
        db.close()