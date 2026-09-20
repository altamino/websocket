from helpers.agora import (
    DEFAULT_TOKEN_EXPIRE_SECONDS,
    ROLE_PUBLISHER,
    build_rtc_token,
    channel_name,
    uid_from_uuid,
)
from helpers.channel_messages import emit_channel_ended, emit_channel_started
from helpers.config import Config
from helpers.connection_manager import ConnectionManager
from helpers.database.mongo import Database
from helpers.wsobjs import WSObjects


def _member_payload(member: dict) -> dict:
    return {
        "channelUid": member["channelUid"],
        "joinRole": member["joinRole"],
        "isHost": member["joinRole"] == 1,
        "isOffline": False,
        "userProfile": {"uid": member["uid"]},
    }


def _user_list_payload(thread_id: str, members: list[dict]) -> dict:
    return {
        "t": 102,
        "o": {
            "threadId": thread_id,
            "userList": [_member_payload(m) for m in members],
        },
    }


def _has_presenter(members: list[dict]) -> bool:
    return any(m["joinRole"] == 1 for m in members)


def _should_finish_channel(members: list[dict], channel_type: int) -> bool:
    if not channel_type:
        return False
    return not members or not _has_presenter(members)


async def _chat_channel_type_from_db(ndc_id: int, thread_id: str) -> int:
    try:
        db = await Database().init()
        try:
            chats = await db.get(f"x{ndc_id}", "Chats")
            chat_info = (
                await chats.find_one({"id": thread_id}, {"channelType": 1}) or {}
            )
            return chat_info.get("channelType", 0)
        finally:
            await db.close()
    except Exception:
        return 0


async def _finish_channel(
    uid: str,
    ndc_id: int,
    thread_id: str,
    manager: ConnectionManager,
    ended_channel_type: int | None = None,
):
    if not ended_channel_type:
        ended_channel_type = manager.get_channel_type(thread_id)
    if not ended_channel_type:
        ended_channel_type = await _chat_channel_type_from_db(ndc_id, thread_id)

    manager.clear_channel_members(thread_id)

    channel_end = WSObjects.ChannelTypeUpdate(thread_id, 0, status=0)
    force_quit = WSObjects.ChannelForceQuit(thread_id)
    await manager.broadcast_to_thread(thread_id, _user_list_payload(thread_id, []))
    await manager.broadcast_to_thread(thread_id, channel_end)
    await manager.broadcast_to_thread(thread_id, force_quit)

    if ended_channel_type:
        await emit_channel_ended(ndc_id, thread_id, uid, ended_channel_type)


async def _other_chat_members(ndc_id: int, thread_id: str, uid: str) -> list[str]:
    try:
        db = await Database().init()
        try:
            chats = await db.get(f"x{ndc_id}", "Chats")
            chat_info = await chats.find_one({"id": thread_id}) or {}
        finally:
            await db.close()
    except Exception:
        return []
    members = chat_info.get("memberList", []) + chat_info.get("invitedList", [])
    return [m for m in members if m != uid]


async def on_join_thread(
    uid: str,
    ndc_id: int,
    thread_id: str,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    manager.set_thread_ndc(thread_id, ndc_id)
    manager.subscribe_thread(uid, thread_id)
    await manager.answer(
        {"t": 101, "o": {"id": ws_req_id, "ndcId": ndc_id, "threadId": thread_id}},
        ws,
    )

    members = manager.get_channel_members(thread_id)
    if members:
        await manager.answer(_user_list_payload(thread_id, members), ws)

    active_channel_type = manager.get_channel_type(thread_id)
    if not active_channel_type and members:
        try:
            channel_type = await _chat_channel_type_from_db(ndc_id, thread_id)
            if channel_type:
                manager.set_channel_type(thread_id, channel_type)
                active_channel_type = channel_type
        except Exception:
            pass

    if active_channel_type:
        await manager.answer(
            WSObjects.ChannelTypeUpdate(thread_id, active_channel_type, status=1),
            ws,
        )


async def on_leave_thread(
    uid: str,
    ndc_id: int,
    thread_id: str,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    manager.unsubscribe_thread(uid, thread_id)
    manager.remove_channel_member(thread_id, uid)
    manager.clear_user_busy(uid)

    await manager.broadcast_to_thread(
        thread_id,
        WSObjects.ChannelUserLeave(thread_id, uid),
        exclude_uid=uid,
    )

    remaining = manager.get_channel_members(thread_id)
    channel_type = manager.get_channel_type(thread_id)
    if not channel_type:
        channel_type = await _chat_channel_type_from_db(ndc_id, thread_id)
    if _should_finish_channel(remaining, channel_type):
        await _finish_channel(
            uid, ndc_id, thread_id, manager, ended_channel_type=channel_type
        )
    elif remaining:
        await manager.broadcast_to_thread(
            thread_id, _user_list_payload(thread_id, remaining)
        )

    await manager.answer({"t": 104, "o": {"id": ws_req_id, "ndcId": ndc_id}}, ws)


async def on_update_role(
    uid: str,
    ndc_id: int,
    thread_id: str,
    join_role: int,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    agora_uid = uid_from_uuid(uid)
    channel_type = manager.get_channel_type(thread_id)
    if not channel_type:
        channel_type = await _chat_channel_type_from_db(ndc_id, thread_id)

    if join_role == 0:
        manager.remove_channel_member(thread_id, uid)
        manager.clear_user_busy(uid)

        await manager.answer(
            WSObjects.LiveChatJoin(ws_req_id, ndc_id, thread_id, 0, uid, 0),
            ws,
        )

        await manager.broadcast_to_thread(
            thread_id,
            WSObjects.ChannelUserLeave(thread_id, uid),
            exclude_uid=uid,
        )

        remaining = manager.get_channel_members(thread_id)
        if _should_finish_channel(remaining, channel_type):
            await _finish_channel(
                uid,
                ndc_id,
                thread_id,
                manager,
                ended_channel_type=channel_type,
            )
        elif remaining:
            await manager.broadcast_to_thread(
                thread_id, _user_list_payload(thread_id, remaining)
            )
        return

    manager.set_thread_ndc(thread_id, ndc_id)
    manager.subscribe_thread(uid, thread_id)
    manager.add_channel_member(
        thread_id, uid, join_role, channel_uid=agora_uid, ndc_id=ndc_id
    )

    if join_role == 1:
        manager.mark_user_busy(uid, thread_id)
    else:
        manager.clear_user_busy(uid)

    await manager.answer(
        WSObjects.LiveChatJoin(ws_req_id, ndc_id, thread_id, join_role, uid, agora_uid),
        ws,
    )

    members = manager.get_channel_members(thread_id)
    if _should_finish_channel(members, channel_type):
        await _finish_channel(
            uid,
            ndc_id,
            thread_id,
            manager,
            ended_channel_type=channel_type,
        )
        return

    if join_role == 1:
        await manager.broadcast_to_thread(
            thread_id,
            WSObjects.ChannelUserJoin(thread_id, uid, join_role, agora_uid),
            exclude_uid=uid,
        )
        if len(members) > 1:
            await manager.broadcast_to_thread(thread_id, _user_list_payload(thread_id, members))
    else:
        await manager.broadcast_to_thread(thread_id, _user_list_payload(thread_id, members))


async def on_fetch_channel_users(
    uid: str,
    ndc_id: int,
    thread_id: str,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    members = manager.get_channel_members(thread_id)
    await manager.answer(
        {
            "t": 102,
            "o": {
                "id": ws_req_id,
                "ndcId": ndc_id,
                "threadId": thread_id,
                "userList": [_member_payload(m) for m in members],
            },
        },
        ws,
    )


async def on_update_channel_type(
    uid: str,
    ndc_id: int,
    thread_id: str,
    channel_type: int,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    if channel_type in (1, 3, 4, 5):
        other_members = await _other_chat_members(ndc_id, thread_id, uid)
        for member_uid in other_members:
            if manager.is_user_busy(member_uid):
                await manager.answer(
                    {
                        "t": 109,
                        "o": {
                            "id": ws_req_id,
                            "ndcId": ndc_id,
                            "exception": {
                                "code": 111,
                                "message": "receiver busy",
                            },
                        },
                    },
                    ws,
                )
                return

    previous_type = manager.get_channel_type(thread_id)
    channel_finished = False

    if channel_type:
        manager.set_channel_type(thread_id, channel_type)
        manager.mark_user_busy(uid, thread_id)
    else:
        ended_type = previous_type or await _chat_channel_type_from_db(ndc_id, thread_id)
        manager.set_channel_type(thread_id, 0)
        manager.clear_user_busy(uid)
        remaining = manager.get_channel_members(thread_id)
        if _should_finish_channel(remaining, ended_type):
            await _finish_channel(
                uid, ndc_id, thread_id, manager, ended_channel_type=ended_type
            )
            channel_finished = True
        elif ended_type:
            await emit_channel_ended(ndc_id, thread_id, uid, ended_type)

    if not channel_finished:
        await manager.broadcast_to_thread(
            thread_id,
            WSObjects.ChannelTypeUpdate(
                thread_id, channel_type, status=1 if channel_type else 0
            ),
            exclude_uid=uid,
        )
    await manager.answer(
        WSObjects.ChannelTypeResponse(ws_req_id, ndc_id, channel_type),
        ws,
    )

    if channel_type and previous_type != channel_type:
        await emit_channel_started(ndc_id, thread_id, uid, channel_type)


async def on_get_agora(
    uid: str,
    ndc_id: int,
    thread_id: str,
    ws_req_id,
    manager: ConnectionManager,
    ws,
):
    agora_uid = uid_from_uuid(uid)
    channel = channel_name(ndc_id, thread_id)

    if not Config.AGORA_APP_ID or not Config.AGORA_APP_CERTIFICATE:
        await manager.answer(
            WSObjects.WSError(500, "Agora not configured", ws_req_id, ndc_id),
            ws,
        )
        return

    token = build_rtc_token(
        Config.AGORA_APP_ID,
        Config.AGORA_APP_CERTIFICATE,
        channel,
        agora_uid,
        ROLE_PUBLISHER,
        DEFAULT_TOKEN_EXPIRE_SECONDS,
    )

    await manager.answer(
        WSObjects.AgoraChannel(
            ws_req_id,
            ndc_id,
            token,
            channel,
            agora_uid,
            DEFAULT_TOKEN_EXPIRE_SECONDS,
        ),
        ws,
    )
