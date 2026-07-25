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
	elif t == ApiBroadcastType.ChatMessagePush:
		return PushEvents.push_chat_message(
			message["ndcId"],
			message["threadId"],
			message["author"],
			message["content"],
			message["messageType"]
		)

	elif t == ApiBroadcastType.NewFollowerPush:
		return PushEvents.push_user_membership(
			message["ndcId"],
			message["user"]
		)

	elif t == ApiBroadcastType.NewCommunityMemberPush:
		return PushEvents.push_new_community_user(
			message["ndcId"],
			message["user"]
		)