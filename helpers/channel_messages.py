from helpers.connection_manager import broadcast_ws_message
from helpers.database.models import Community, ModelFabric, dttmn
from helpers.database.mongo import Database
from objects.user import User
from objects.ws_events import ChatEvents

# Signalling channel type -> chat system message type
START_MESSAGE_TYPES = {
    1: 107,  # voice
    3: 109,  # avatar
    4: 108,  # video
    5: 114,  # screen
}

END_MESSAGE_TYPES = {
    1: 110,
    3: 112,
    4: 111,
    5: 115,
}


def _message_obj(message: dict, thread_id: str, ndc_id: int, author: dict) -> dict:
    return {
        "includedInSummary": True,
        "uid": message["authorId"],
        "author": author,
        "isHidden": False,
        "messageId": message["messageId"],
        "mediaType": message.get("mediaType", 0),
        "content": message.get("content"),
        "clientRefId": message.get("clientRefId", 0),
        "threadId": thread_id,
        "ndcId": ndc_id,
        "createdTime": message["createdTime"],
        "extensions": message.get("extensions") or {},
        "type": message["messageType"],
        "mediaValue": message.get("mediaValue"),
    }


async def _chat_targets(ndc_id: int, thread_id: str) -> tuple[dict | None, list[str]]:
    db = await Database().init()
    try:
        chats = await db.get(f"x{ndc_id}", "Chats")
        chat_info = await chats.find_one({"id": thread_id}) or {}
        targets = chat_info.get("memberList", []) + chat_info.get("invitedList", [])
        return chat_info, targets
    finally:
        await db.close()


async def _persist_and_broadcast(
    ndc_id: int,
    thread_id: str,
    author_uid: str,
    message_type: int,
    channel_type: int,
):
    chat_info, targets = await _chat_targets(ndc_id, thread_id)
    if not chat_info or not targets:
        return

    db = await Database().init()
    try:
        users = await db.get(f"x{ndc_id}", "Users")
        author_row = await users.find_one({"id": author_uid}) or {}
        author_row.setdefault("following", [])
        author_row.setdefault("whoFollows", [])
        author = User.OwnNonSensetiveProfile(author_row, ndcId=ndc_id)

        message = ModelFabric.Construct(
            Community.Message,
            authorId=author_uid,
            messageType=message_type,
            content=None,
            mediaType=0,
        )

        msg_table = await db.get(f"x{ndc_id}", f"_Chat:{thread_id}")
        await msg_table.insert_one(message)

        chat_update: dict = {
            "lastMessageId": message["messageId"],
            "lastMessageTimestamp": message["timestamp"],
            "channelType": channel_type,
            "modifiedTime": dttmn(),
        }
        if channel_type:
            chat_update["channelTypeLastCreatedTime"] = message["createdTime"]

        chats = await db.get(f"x{ndc_id}", "Chats")
        await chats.update_one({"id": thread_id}, {"$set": chat_update})
    finally:
        await db.close()

    message_obj = _message_obj(message, thread_id, ndc_id, author)
    await broadcast_ws_message(
        ChatEvents.new_message(ndc_id, message_obj),
        targets,
    )


async def emit_channel_started(
    ndc_id: int,
    thread_id: str,
    author_uid: str,
    channel_type: int,
):
    message_type = START_MESSAGE_TYPES.get(channel_type)
    if not message_type:
        return
    await _persist_and_broadcast(
        ndc_id, thread_id, author_uid, message_type, channel_type
    )


async def emit_channel_ended(
    ndc_id: int,
    thread_id: str,
    author_uid: str,
    channel_type: int,
):
    if not channel_type:
        return
    message_type = END_MESSAGE_TYPES.get(channel_type)
    if not message_type:
        return
    await _persist_and_broadcast(ndc_id, thread_id, author_uid, message_type, 0)
