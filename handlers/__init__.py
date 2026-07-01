from helpers.connection_manager import ConnectionManager
from helpers.validator import is_valid_uuid4
from helpers.validator import is_valid_id
from helpers.wsobjs import WSObjects
from helpers.config import Config
from helpers.agora import build_rtc_token, ROLE_PUBLISHER, uid_from_uuid
from helpers.database.mongo import Database
from helpers.constants import (
		WS_TYPE_PING,
		WS_TYPE_MARK_READ,
		WS_CHAT_SCREEN_OPEN,
		WS_CHAT_SCREEN_CLOSE,
		WS_FETCH_CHANNEL_USERS,
		WS_ACTION_START,
		WS_ACTION_END,
		WS_TYPE_JOIN_LIVE,
		WS_TYPE_GET_AGORA,
		WS_TYPE_TOPIC_SUBSCRIBE,
		WS_TYPE_UPDATE_CHANNEL_TYPE,

		ACTION_BROWSING,
		ACTION_CHATTING,
		ACTION_RECORDING,
		ACTION_TYPING

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

from ._refresh_viewer import refresh_user_viewing

from .community import on_ws_ndc_event

async def handle_message(data: dict, manager: ConnectionManager, uid: str, isAdmin: bool, ws):
	t: dict = data.get("t")
	o: dict = data.get("o") or {}
	if t is None:
		return

	print(f"[WS MSG] uid={uid} t={t} o={o}")

	if t == WS_TYPE_PING:
		await manager.answer(WSObjects.Pong(), ws)
		await refresh_user_viewing(uid)
		return

	if not data["o"].get("id") or not is_valid_id(data["o"].get("id")):
		print(f"[WS MSG] No/invalid ID for t={t}, o.id={data['o'].get('id')!r}")


	ndcId: int = o.get("ndcId")
	chatId: str = o.get("threadId")
	ws_req_id = o.get("id")
	targetChatId: str = o.get("target", "").split("/")[-1]
	actions: list = o.get("actions")


	if ndcId:
		await on_ws_ndc_event(uid, ndcId)

	if (
		t == WS_TYPE_MARK_READ and
		o.get("markHasRead", None) is not None
		and ndcId is not None and chatId is not None
	):
		await mark_read(data, uid)
	

	elif ( 
		t == WS_CHAT_SCREEN_OPEN
		and ndcId is not None
		and chatId is not None
	):
		manager.subscribe_thread(uid, chatId)
		await on_chat_screen_open(uid, chatId, ndcId, manager)
		await manager.answer({"t": 101, "o": {"id": ws_req_id, "ndcId": ndcId, "threadId": chatId}}, ws)
		channel_members_now = manager.get_channel_members(chatId)
		if channel_members_now:
			channel_user_info = [{"channelUid": m["channelUid"], "joinRole": m["joinRole"], "isOffline": False, "userProfile": {"uid": m["uid"]}} for m in channel_members_now]
			await manager.answer({"t": 102, "o": {"threadId": chatId, "userList": channel_user_info}}, ws)
		active_channel_type = manager.get_channel_type(chatId)
		if not active_channel_type and channel_members_now:
			try:
				_db = await Database().init()
				_chat_info = await _db.get(f"x{ndcId}", "Chats").find_one({"id": chatId}, {"channelType": 1}) or {}
				await _db.close()
				_ct = _chat_info.get("channelType", 0)
				if _ct:
					manager.set_channel_type(chatId, _ct)
					active_channel_type = _ct
			except Exception:
				pass
		if active_channel_type:
			await manager.answer({"t": 111, "o": {"threadId": chatId, "channelType": active_channel_type, "status": 0}}, ws)


	elif ( 
		t == WS_CHAT_SCREEN_CLOSE
		and ndcId is not None
		and chatId is not None
	):
		manager.unsubscribe_thread(uid, chatId)
		await on_chat_screen_close(uid, chatId, ndcId, manager)
		await manager.answer({"t": 104, "o": {"id": ws_req_id, "ndcId": ndcId}}, ws)
	




	elif t == WS_ACTION_START and actions:
		if actions == [ACTION_RECORDING] and targetChatId:
			await on_chat_voice_recording(uid, targetChatId, ndcId, manager)
		elif actions == [ACTION_TYPING] and targetChatId:
			await on_chat_message_typing(uid, targetChatId, ndcId, manager)
		elif actions == [ACTION_CHATTING] and targetChatId:
			await on_chatting(uid, targetChatId, ndcId, manager)


		elif actions == [ACTION_BROWSING]:
			pass


	elif t == WS_ACTION_END and actions:
		if actions == [ACTION_RECORDING] and targetChatId:
			await on_chat_voice_recording_end(uid, targetChatId, ndcId, manager)
		elif actions == [ACTION_TYPING] and targetChatId:
			await on_chat_message_typing_end(uid, targetChatId, ndcId, manager)
		elif actions == [ACTION_CHATTING] and targetChatId:
			await on_chatting_end(uid, targetChatId, ndcId, manager)
		elif actions == [ACTION_BROWSING]:
			pass


	elif t == WS_TYPE_JOIN_LIVE and ndcId is not None and chatId is not None:
		join_role = o.get("joinRole", 1)
		agora_uid = uid_from_uuid(uid)

		if join_role == 0:
			members_before = manager.get_channel_members(chatId)
			was_host = any(m["uid"] == uid and m["joinRole"] == 1 for m in members_before)
			manager.remove_channel_member(chatId, uid)
			remaining = manager.get_channel_members(chatId)

			await manager.broadcast_to_thread(chatId, WSObjects.ChannelUserLeave(chatId, uid))

			if was_host or not remaining:
				for m in remaining:
					await manager.broadcast_to_thread(chatId, WSObjects.ChannelUserLeave(chatId, m["uid"]))
				await manager.broadcast_to_thread(chatId, {"t": 102, "o": {"threadId": chatId, "userList": []}})
				manager.clear_channel_members(chatId)
				import time as _time
				from datetime import datetime, UTC
				from uuid import uuid4 as _uuid4
				try:
					db = await Database().init()
					chat_table = await db.get(f"x{ndcId}", "Chats")
					msg_collection = await db.get(f"x{ndcId}", f"_Chat:{chatId}")
					msg_id = str(_uuid4())
					now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
					now_ms = int(_time.time() * 1000)
					await msg_collection.insert_one({
						"messageId": msg_id,
						"authorId": uid,
					"messageType": 110,
						"clientRefId": 0,
						"content": None,
						"mediaType": 0,
						"mediaValue": None,
						"timestamp": now_ms,
						"extensions": {},
						"createdTime": now_iso,
					})
					await chat_table.update_one(
						{"id": chatId},
						{"$set": {"channelType": 0, "lastMessageId": msg_id, "lastMessageTimestamp": now_ms}}
					)
					chat_info = await chat_table.find_one({"id": chatId}) or {}
					await db.close()
					members_list = chat_info.get("memberList", []) + chat_info.get("invitedList", [])
					await manager.selective_broadcast({
						"t": 1000,
						"o": {
							"ndcId": ndcId,
							"chatMessage": {
								"messageId": msg_id,
								"threadId": chatId,
								"ndcId": ndcId,
								"uid": uid,
								"type": 110,
								"content": None,
								"mediaType": 0,
								"mediaValue": None,
								"createdTime": now_iso,
								"clientRefId": 0,
								"extensions": {},
								"includedInSummary": True,
								"isHidden": False,
							},
							"alertOption": 1,
							"membershipStatus": 1,
						},
					}, members_list)
				except Exception as e:
					print(f"[WS] Failed to end channel on host leave: {e}")
				await manager.broadcast_to_thread(chatId, WSObjects.ChannelTypeUpdate(chatId, 0))
		else:
			manager.subscribe_thread(uid, chatId)
			manager.add_channel_member(chatId, uid, join_role, channel_uid=agora_uid)
			await manager.answer(WSObjects.LiveChatJoin(ws_req_id, ndcId, chatId, join_role, uid, agora_uid), ws)
			await manager.broadcast_to_thread(chatId, WSObjects.ChannelUserJoin(chatId, uid, join_role), exclude_uid=uid)


	elif t == WS_FETCH_CHANNEL_USERS and ndcId is not None and chatId is not None:
		members = manager.get_channel_members(chatId)
		user_info_list = [{"channelUid": m["channelUid"], "joinRole": m["joinRole"], "isOffline": False, "userProfile": {"uid": m["uid"]}} for m in members]
		await manager.answer({"t": 102, "o": {"id": ws_req_id, "ndcId": ndcId, "threadId": chatId, "userList": user_info_list}}, ws)


	elif t == WS_TYPE_UPDATE_CHANNEL_TYPE and ndcId is not None and chatId is not None:
		import time as _time
		from datetime import datetime, UTC
		from uuid import uuid4 as _uuid4
		from helpers.processors.cache import CacheProcessor
		channel_type = o.get("channelType", 0)
		MSG_TYPE = 107 if channel_type == 1 else 110

		try:
			db = await Database().init()
			chat_table = await db.get(f"x{ndcId}", "Chats")
			msg_collection = await db.get(f"x{ndcId}", f"_Chat:{chatId}")
			msg_id = str(_uuid4())
			now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
			now_ms = int(_time.time() * 1000)
			system_msg = {
				"messageId": msg_id,
				"authorId": uid,
				"messageType": MSG_TYPE,
				"clientRefId": 0,
				"content": None,
				"mediaType": 0,
				"mediaValue": None,
				"timestamp": now_ms,
				"extensions": {},
				"createdTime": now_iso,
			}
			await msg_collection.insert_one(system_msg)

			await chat_table.update_one(
				{"id": chatId},
				{"$set": {
					"channelType": channel_type,
					"lastMessageId": msg_id,
					"lastMessageTimestamp": now_ms,
				}}
			)

			ws_msg_payload = {
				"t": 1000,
				"o": {
					"ndcId": ndcId,
					"chatMessage": {
						"messageId": msg_id,
						"threadId": chatId,
						"ndcId": ndcId,
						"uid": uid,
						"type": MSG_TYPE,
						"content": None,
						"mediaType": 0,
						"mediaValue": None,
						"createdTime": now_iso,
						"clientRefId": 0,
						"extensions": {},
						"includedInSummary": True,
						"isHidden": False,
					},
					"alertOption": 1,
					"membershipStatus": 1,
				},
			}
			chat_info = await chat_table.find_one({"id": chatId}) or {}
			members = chat_info.get("memberList", []) + chat_info.get("invitedList", [])
			await db.close()
			await manager.selective_broadcast(ws_msg_payload, members)
		except Exception as e:
			print(f"[WS] Failed to insert system message for channelType change: {e}")

		if channel_type:
			manager.set_channel_type(chatId, channel_type)
		else:
			manager.clear_channel_members(chatId)

		await manager.broadcast_to_thread(chatId, WSObjects.ChannelTypeUpdate(chatId, channel_type), exclude_uid=uid)
		await manager.answer(WSObjects.ChannelTypeResponse(ws_req_id, ndcId, channel_type), ws)


	elif t == WS_TYPE_GET_AGORA and ndcId is not None and chatId is not None:
		import time as _time
		expire_seconds = 3600
		agora_uid = uid_from_uuid(uid)
		token = build_rtc_token(
			Config.AGORA_APP_ID,
			Config.AGORA_APP_CERTIFICATE,
			chatId,
			agora_uid,
			ROLE_PUBLISHER,
			expire_seconds,
		)
		expired_time = int(_time.time()) + expire_seconds
		await manager.answer(WSObjects.AgoraChannel(ws_req_id, ndcId, token, chatId, agora_uid, expired_time), ws)


	elif t == WS_TYPE_TOPIC_SUBSCRIBE:
		pass

