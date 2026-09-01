import uuid
import time
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


from random import randint


from helpers.constants import (
    WS_TYPE_CHAT_MESSAGE,
    WS_SERV_ACTION_END as WS_ACTION_END,
    WS_SERV_ACTION_START as WS_ACTION_START,
    WS_NOTIFICATION_MESSAGE,
    ACTION_RECORDING,
    ACTION_TYPING,
    NOTIFICATION_TYPE_NEW_MESSAGE,
)



class ChatEvents:
    @staticmethod
    def new_message(
        ndcId: int, message: dict, alertOption: int = 1, membershipStatus: int = 1
    ) -> dict:
        return {
            "t": WS_TYPE_CHAT_MESSAGE,
            "o": {
                "ndcId": ndcId,
                "chatMessage": message,
                "alertOption": alertOption,
                "membershipStatus": membershipStatus,
            },
        }

    @staticmethod
    def new_message_notification(
        chatId: str, ndcId: int, notifType: int = NOTIFICATION_TYPE_NEW_MESSAGE
    ) -> dict:
        # IDK
        return {
            "t": WS_NOTIFICATION_MESSAGE,
            "o": {
                "payload": {"notifType": notifType},
                "ndcId": ndcId,
                "threadId": chatId,
            },
        }

    @staticmethod
    def _ndtopic(ndcId: int, topic: str) -> str:
        prefix = "ndtopic:g:" if ndcId == 0 else f"ndtopic:x{ndcId}:"
        return prefix + topic

    @staticmethod
    def _send_topic(ndcId: int, topic: str, userProfileList: list) -> dict:
        return {
            "ndcId": ndcId,
            "topic": ChatEvents._ndtopic(ndcId, topic),
            "userProfileCount": len(userProfileList),
            "userProfileList": userProfileList,
        }

    @staticmethod
    def typing_start(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": ChatEvents._send_topic(
                ndcId, f"users-start-typing-at:{chatId}", userProfileList
            ),
        }

    @staticmethod
    def typing_end(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": ChatEvents._send_topic(
                ndcId, f"users-end-typing-at:{chatId}", userProfileList
            ),
        }

    @staticmethod
    def recording_start(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": ChatEvents._send_topic(
                ndcId, f"users-start-recording-at:{chatId}", userProfileList
            ),
        }

    @staticmethod
    def recording_end(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": ChatEvents._send_topic(
                ndcId, f"users-end-recording-at:{chatId}", userProfileList
            ),
        }


class BrowsingEvents:
    pass


class PushEvents:
    @staticmethod
    def _base(notif_type, ndc_id, message=None, title=None, badge=0, **fields):
        payload = {
            "notifType": notif_type,
            "id": str(uuid.uuid4()),
            "ndcId": ndc_id,
            "ts": _now_iso(),
            "aps": {
                "badge": badge,
                "message": message or "",
                "sound": "default",
                "title": title or "",
            },
        }
        payload.update({k: v for k, v in fields.items() if v is not None})
        return {"t": 10, "o": {"payload": payload}}

    # ---------- ЧАТ (подтверждено по коду для 21) ----------
    @staticmethod
    def push_chat_invite(ndc_id, thread_id, inviter, thread_type=1):
        return PushEvents._base(
            21,
            ndc_id,
            message=f"{inviter.get('nickname', 'User')} invited you to a chat",
            title="Chat invite",
            tid=thread_id,
            uid=inviter.get("uid"),
            userProfile=inviter,
            nickname=inviter.get("nickname"),
            picUrl=inviter.get("icon"),
            picType=1,
            ttype=thread_type,
            exp=int(time.time() * 1000) + 86_400_000,
        )

    @staticmethod
    def push_chat_message(ndc_id, thread_id, author: dict, content: str, msg_type=0):
        nick = author.get("nickname")
        return PushEvents._base(
            18,
            ndc_id,
            message=content,
            title=nick,
            tid=thread_id,
            uid=author.get("uid"),
            userProfile=author,
            nickname=nick,
            threadTime=_now_iso(),
            msgType=msg_type,
        )

    @staticmethod
    def push_chat_typing(ndc_id, thread_id, user: dict):
        """19 — печатает."""
        return PushEvents._base(
            19, ndc_id, tid=thread_id, uid=user.get("uid"), userProfile=user
        )

    @staticmethod
    def push_chat_user_observing(ndc_id, thread_id, user: dict):
        """20 — юзер смотрит тред."""
        return PushEvents._base(
            20, ndc_id, tid=thread_id, uid=user.get("uid"), userProfile=user
        )

    @staticmethod
    def push_join_request_received(ndc_id, thread_id, requester: dict):
        """22 — хосту: запрос на вступление."""
        return PushEvents._base(
            22,
            ndc_id,
            tid=thread_id,
            uid=requester.get("uid"),
            userProfile=requester,
            nickname=requester.get("nickname"),
        )

    @staticmethod
    def push_join_request_approved(ndc_id, thread_id, approver: dict):
        """23 — запрос вступить одобрен."""
        return PushEvents._base(
            23,
            ndc_id,
            tid=thread_id,
            uid=approver.get("uid"),
            userProfile=approver,
        )

    @staticmethod
    def push_chat_add_cohost(ndc_id, thread_id, actor: dict):
        """67 — назначен ко-хостом."""
        return PushEvents._base(
            67, ndc_id, tid=thread_id, uid=actor.get("uid"), userProfile=actor
        )

    @staticmethod
    def push_chat_remove_cohost(ndc_id, thread_id, actor: dict):
        """68 — снят с ко-хоста."""
        return PushEvents._base(
            68, ndc_id, tid=thread_id, uid=actor.get("uid"), userProfile=actor
        )

    # ---------- КОНТЕНТ / СОЦИАЛ ----------
    @staticmethod
    def push_comment(ndc_id, from_user: dict, url: str, msg_type=0):
        """3 — коммент."""
        return PushEvents._base(
            3,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
            u=url,
            msgType=msg_type,
        )

    @staticmethod
    def push_reply(ndc_id, from_user: dict, url: str):
        """7 — ответ на коммент."""
        return PushEvents._base(
            7,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
            u=url,
        )

    @staticmethod
    def push_vote_up(ndc_id, from_user: dict, url: str):
        """9 — лайк."""
        return PushEvents._base(
            9,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
            u=url,
        )

    @staticmethod
    def push_repost(ndc_id, from_user: dict, url: str):
        """11 — репост."""
        return PushEvents._base(
            11,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
            u=url,
        )

    # ---------- МЕМБЕРШИП ----------
    @staticmethod
    def push_user_membership(ndc_id, from_user: dict):
        """1 — фолловер."""
        return PushEvents._base(
            1,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
        )

    @staticmethod
    def push_new_community_user(ndc_id, from_user: dict):
        """custom for bots."""
        return PushEvents._base(
            30002,
            ndc_id,
            uid=from_user.get("uid"),
            userProfile=from_user,
            nickname=from_user.get("nickname"),
        )
