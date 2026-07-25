from objects.api_broadcast_types import ApiBroadcastType
from objects.ws_events import PushEvents, ChatEvents

def handle_api_message(
	t: int,
	message: dict,
	) -> dict | None:


	if t == ApiBroadcastType.InviteChatPush:
		return PushEvents.push_chat_invite(
			message["ndcId"],
			message["threadId"],
			message["inviter"],
			message["threadType"]
		)