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
    def _send_topic(ndcId: int, topic: str, userProfileList: list) -> dict:
        return {
            "ndcId": ndcId,
            "topic": topic,
            "userProfileCount": len(userProfileList),
            "userProfileList": userProfileList
        }



    @staticmethod
    def typing_start(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": ChatEvents._send_topic(ndcId, f"users-start-typing-at:{chatId}", userProfileList)
        }


    @staticmethod
    def typing_end(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": ChatEvents._send_topic(ndcId, f"users-end-typing-at:{chatId}", userProfileList)
        }



    @staticmethod
    def recording_start(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_START,
            "o": ChatEvents._send_topic(ndcId, f"users-start-recording-at:{chatId}", userProfileList)
        }


    @staticmethod
    def recording_end(chatId: str, ndcId: int, userProfileList: list) -> dict:
        return {
            "t": WS_ACTION_END,
            "o": ChatEvents._send_topic(ndcId, f"users-end-recording-at:{chatId}", userProfileList)
        }





class BrowsingEvents:
    pass