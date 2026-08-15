from helpers.connection_manager import ConnectionManager
from helpers.validator import is_valid_id
from helpers.wsobjs import WSObjects
from helpers.constants import (
    WS_TYPE_PING,
    WS_TYPE_MARK_READ,
    WS_CHAT_SCREEN_OPEN,
    WS_CHAT_SCREEN_CLOSE,
    WS_FETCH_CHANNEL_USERS,
    WS_TYPE_UPDATE_CHANNEL_TYPE,
    WS_TYPE_JOIN_LIVE,
    WS_TYPE_GET_AGORA,
    WS_ACTION_START,
    WS_ACTION_END,
    ACTION_BROWSING,
    ACTION_CHATTING,
    ACTION_RECORDING,
    ACTION_TYPING,
)

from .chat import (
    mark_read,
    on_chat_screen_close,
    on_chat_screen_open,
    on_chat_message_typing,
    on_chat_voice_recording,
    on_chatting,
    on_chat_message_typing_end,
    on_chat_voice_recording_end,
    on_chatting_end,
)
from .call import (
    on_fetch_channel_users,
    on_get_agora,
    on_join_thread,
    on_leave_thread,
    on_update_channel_type,
    on_update_role,
)

from ._refresh_viewer import refresh_user_viewing

from .community import on_ws_ndc_event


async def handle_message(
    data: dict, manager: ConnectionManager, uid: str, isAdmin: bool, ws
):
    t: dict = data.get("t")
    o: dict = data.get("o")
    print(data)
    if t is None or o is None:
        return

    if data["t"] == WS_TYPE_PING:
        channel_infos = manager.get_user_channel_infos(uid)
        await manager.answer(
            WSObjects.Pong(o.get("id"), channel_infos),
            ws,
        )
        await refresh_user_viewing(uid)
        return

    if not data["o"].get("id") or not is_valid_id(data["o"].get("id")):
        await manager.answer(WSObjects.WSError(1, "No ID of request"), ws)
        return

    ndcId: int = o.get("ndcId")
    chatId: str = o.get("threadId")
    ws_req_id = o.get("id")
    targetChatId: str = o.get("target", "").split("/")[-1]
    actions: list = o.get("actions")

    if ndcId:
        await on_ws_ndc_event(uid, ndcId)

    if (
        t == WS_TYPE_MARK_READ
        and o.get("markHasRead", None) is not None
        and ndcId is not None
        and chatId is not None
    ):
        await mark_read(data, uid)

    elif t == WS_CHAT_SCREEN_OPEN and ndcId is not None and chatId is not None:
        await on_chat_screen_open(uid, chatId, ndcId, manager)
        await on_join_thread(uid, ndcId, chatId, ws_req_id, manager, ws)

    elif t == WS_CHAT_SCREEN_CLOSE and ndcId is not None and chatId is not None:
        await on_chat_screen_close(uid, chatId, ndcId, manager)
        await on_leave_thread(uid, ndcId, chatId, ws_req_id, manager, ws)

    elif t == WS_TYPE_JOIN_LIVE and ndcId is not None and chatId is not None:
        join_role = o.get("joinRole", 1)
        await on_update_role(uid, ndcId, chatId, join_role, ws_req_id, manager, ws)

    elif t == WS_FETCH_CHANNEL_USERS and ndcId is not None and chatId is not None:
        await on_fetch_channel_users(uid, ndcId, chatId, ws_req_id, manager, ws)

    elif t == WS_TYPE_UPDATE_CHANNEL_TYPE and ndcId is not None and chatId is not None:
        channel_type = o.get("channelType", 0)
        await on_update_channel_type(
            uid, ndcId, chatId, channel_type, ws_req_id, manager, ws
        )

    elif t == WS_TYPE_GET_AGORA and ndcId is not None and chatId is not None:
        await on_get_agora(uid, ndcId, chatId, ws_req_id, manager, ws)

    elif t == WS_ACTION_START and actions:
        if actions == [ACTION_RECORDING] and targetChatId:
            await on_chat_voice_recording(uid, targetChatId, ndcId, manager)
        elif actions == [ACTION_TYPING] and targetChatId:
            await on_chat_message_typing(uid, targetChatId, ndcId, manager)
        elif actions == [ACTION_CHATTING] and targetChatId:
            await on_chatting(uid, targetChatId, ndcId, manager)

        elif actions == [ACTION_BROWSING]:  # TODO
            pass

    elif t == WS_ACTION_END and actions:
        if actions == [ACTION_RECORDING] and targetChatId:
            await on_chat_voice_recording_end(uid, targetChatId, ndcId, manager)
        elif actions == [ACTION_TYPING] and targetChatId:
            await on_chat_message_typing_end(uid, targetChatId, ndcId, manager)
        elif actions == [ACTION_CHATTING] and targetChatId:
            await on_chatting_end(uid, targetChatId, ndcId, manager)

        elif actions == [ACTION_BROWSING]:  # TODO
            pass
