from helpers.connection_manager import ConnectionManager

from random import randint


from helpers.constants import (
    WS_TYPE_CHAT_MESSAGE,
    WS_ACTION_END, WS_ACTION_START,
    WS_NOTIFICATION_MESSAGE,

    ACTION_RECORDING, ACTION_TYPING,

    NOTIFICATION_TYPE_NEW_MESSAGE
)

class ChatEvents:
    @staticmethod
    def new_message(ndcId: int, message: dict, alertOption: int = 1, membershipStatus: int = 1) -> dict:
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
    def new_message_notification(chatId: str, ndcId: int, notifType: int = NOTIFICATION_TYPE_NEW_MESSAGE) -> dict:
        #IDK
        return {
            "t": WS_NOTIFICATION_MESSAGE,
            "o": {
                "payload": {
                    "notifType": notifType
                },
                "ndcId": ndcId,
                "threadId": chatId,
            }
        }






    @staticmethod
    def typing_start(uid: str, chatId: str, ndcId: int, reqid: int | str, threadType: int = 2) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": {
                "actions": [ACTION_TYPING],
                "target": f"ndc://x{ndcId}/chat-thread/{chatId}",
                "ndcId": ndcId,
                "params": {
                    "threadType": threadType,
                },
                "id": str(reqid),
            }
        }

    @staticmethod
    def typing_end(uid: str, chatId: str, ndcId: int, reqid: str | int, duration: int, threadType: int = 2) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": {
                "actions": [ACTION_TYPING],
                "target": f"ndc://x{ndcId}/chat-thread/{chatId}",
                "ndcId": ndcId,
                "params": {
                    "duration": duration,
                    "threadType": threadType,
                },
                "id": str(reqid),
            }
        }

    @staticmethod
    def recording_start(uid: str, chatId: str, ndcId: int, reqid: str | int, threadType: int = 2) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": {
                "actions": [ACTION_RECORDING],
                "target": f"ndc://x{ndcId}/chat-thread/{chatId}",
                "ndcId": ndcId,
                "params": {
                    "topicIds": [],
                    "threadType": threadType,
                },
                "id": str(reqid),
            }
        }

    @staticmethod
    def recording_end(uid: str, chatId: str, ndcId: int, reqid: str | int, duration: int, threadType: int = 2) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": {
                "actions": [ACTION_RECORDING],
                "target": f"ndc://x{ndcId}/chat-thread/{chatId}",
                "ndcId": ndcId,
                "params": {
                    "duration": duration,
                    "topicIds": [],
                    "threadType": threadType,
                },
                "id": str(reqid),
            }
        }



class BrowsingEvents:
    pass