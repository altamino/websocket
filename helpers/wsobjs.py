from datetime import datetime, UTC
from orjson import dumps
from helpers.constants import WS_TYPE_ERROR


class WSObjects:
    @staticmethod
    def HttpError(ws_message: str, ws_statuscode: int) -> bytes:
        return dumps(
            {
                # ws info
                "ws:statuscode": ws_statuscode,
                "ws:message": ws_message,
                # api info
                "api:statuscode": 104,
                "api:duration": "0.001s",
                "api:message": "Invalid request. Check all data that you sended or try again later.",
                "api:timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    @staticmethod
    def WSError(
        exception_code: int,
        exception_message: str,
        ws_req_id: str | int | None = 0,
        ndcId: int | None = 0,
    ) -> dict:
        ws_req_id = ws_req_id or 0
        ndcId = ndcId or 0
        return {
            "t": WS_TYPE_ERROR,
            "o": {
                "id": ws_req_id,
                "ndcId": ndcId,
                "exception": {
                    "id": ws_req_id,
                    "code": exception_code,
                    "message": exception_message,
                },
            },
        }

    @staticmethod
    def NewLogin() -> dict:
        return {"t": 118, "o": {}}

    @staticmethod
    def UniversalMessage(message: str):
        return {"t": 1, "o": {"code": 1, "message": message}}

    @staticmethod
    def InternalWSError():
        return WSObjects.UniversalMessage("Internal socket error")

    @staticmethod
    def Pong(
        ws_req_id: str | None = None,
        thread_channel_user_info_list: list | None = None,
    ) -> dict:
        return {
            "t": 117,
            "o": {
                "id": ws_req_id,
                "threadChannelUserInfoList": thread_channel_user_info_list or [],
            },
        }

    @staticmethod
    def LiveChatJoin(
        ws_req_id,
        ndcId: int,
        thread_id: str,
        join_role: int,
        uid: str,
        channel_uid: int,
    ) -> dict:
        return {
            "t": 113,
            "o": {
                "id": ws_req_id,
                "ndcId": ndcId,
                "threadId": thread_id,
                "user": {
                    "joinRole": join_role,
                    "channelUid": channel_uid if join_role else 0,
                    "isHost": join_role == 1,
                    "isOffline": join_role == 0,
                    "userProfile": {"uid": uid},
                },
            },
        }

    @staticmethod
    def ChannelUserJoin(
        thread_id: str, user_uid: str, join_role: int, channel_uid: int
    ) -> dict:
        return {
            "t": 106,
            "o": {
                "threadId": thread_id,
                "user": {
                    "channelUid": channel_uid,
                    "isHost": join_role == 1,
                    "isOffline": False,
                    "joinRole": join_role,
                    "userProfile": {"uid": user_uid},
                },
            },
        }

    @staticmethod
    def ChannelUserLeave(thread_id: str, user_uid: str) -> dict:
        return {
            "t": 107,
            "o": {
                "threadId": thread_id,
                "user": {
                    "channelUid": 0,
                    "isOffline": True,
                    "joinRole": 0,
                    "userProfile": {"uid": user_uid},
                },
            },
        }

    @staticmethod
    def ChannelTypeUpdate(thread_id: str, channel_type: int, status: int = 0) -> dict:
        return {
            "t": 111,
            "o": {
                "threadId": thread_id,
                "channelType": channel_type,
                "status": status,
            },
        }

    @staticmethod
    def ChannelTypeResponse(ws_req_id, ndcId: int, channel_type: int) -> dict:
        return {
            "t": 109,
            "o": {
                "id": ws_req_id,
                "ndcId": ndcId,
                "channelType": channel_type,
            },
        }

    @staticmethod
    def ChannelForceQuit(thread_id: str, reason: int = 99) -> dict:
        return {
            "t": 115,
            "o": {
                "threadId": thread_id,
                "reason": reason,
            },
        }

    @staticmethod
    def AgoraChannel(
        ws_req_id,
        ndcId: int,
        channel_key: str,
        channel_name: str,
        channel_uid: int,
        expired_time: int,
    ) -> dict:
        return {
            "t": 201,
            "o": {
                "id": ws_req_id,
                "ndcId": ndcId,
                "channelKey": channel_key,
                "channelName": channel_name,
                "channelUid": channel_uid,
                "expiredTime": expired_time,
            },
        }
