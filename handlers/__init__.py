from helpers.connection_manager import ConnectionManager
from helpers.validator import is_valid_uuid4
from helpers.constants import (
    WS_TYPE_MARK_READ,
    WS_CHAT_SCREEN_OPEN,
    WS_CHAT_SCREEN_CLOSE,
    WS_ACTION_START,
    WS_ACTION_END,


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
    on_chatting_end
)

from .community import on_ws_ndc_event

async def handle_message(data: dict, manager: ConnectionManager, uid: str, isAdmin: bool):
    t: dict = data.get("t")
    o: dict = data.get("o")
    if t is None or o is None:
        return
    
    ndcId: int = o.get("ndcId")
    chatId: str = o.get("threadId")
    targetChatId: str = o.get("target", "").split("/")[-1]
    actions: list = o.get("actions")
    params: dict = o.get("params")
    reqId: str = o.get("id")

    threadType = params.get("threadType", 2)
    duration = params.get("duration", 34563)

    if ndcId:
        await on_ws_ndc_event(uid, ndcId)


    if chatId:
        if not is_valid_uuid4(chatId): return
    if targetChatId:
        if not is_valid_uuid4(targetChatId): return


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
        await on_chat_screen_open(uid, chatId)


    elif ( 
        t == WS_CHAT_SCREEN_CLOSE
        and ndcId is not None
        and chatId is not None
    ):
        await on_chat_screen_close(uid, chatId)
    




    elif t == WS_ACTION_START and actions:
        if actions == [ACTION_RECORDING] and targetChatId:
            await on_chat_voice_recording(uid, targetChatId, ndcId, reqId, threadType, manager)
        elif actions == [ACTION_TYPING] and targetChatId:
            await on_chat_message_typing(uid, targetChatId, ndcId, reqId, threadType, manager)
        elif actions == [ACTION_CHATTING] and targetChatId:
            await on_chatting(uid, targetChatId, ndcId, manager)


        elif actions == [ACTION_BROWSING]: #TODO
            pass


    elif t == WS_ACTION_END and actions:
        if actions == [ACTION_RECORDING] and targetChatId:
            await on_chat_voice_recording_end(uid, targetChatId, ndcId, reqId, threadType, duration, manager)
        elif actions == [ACTION_TYPING] and targetChatId:
            await on_chat_message_typing_end(uid, targetChatId, ndcId, reqId, threadType, duration, manager)
        elif actions == [ACTION_CHATTING] and targetChatId:
            await on_chatting_end(uid, targetChatId, ndcId, manager)


        elif actions == [ACTION_BROWSING]: #TODO
            pass


"""



{
  "o": {
    "actions": [
      "Chatting"
    ],
    "target": "ndc://g/chat-thread/0f668f3a-c5f5-42e0-b552-58b270e7841c",
    "ndcId": 0,
    "params": {
      "duration": 34563,
      "threadType": 2,
      "membershipStatus": 1
    },
    "id": "661424288"
  },
  "t": 306
}









{
  "o": {
    "actions": [
      "Typing"
    ],
    "target": "ndc://g/chat-thread/0f668f3a-c5f5-42e0-b552-58b270e7841c",
    "ndcId": 0,
    "params": {
      "duration": 10340,
      "threadType": 2
    },
    "id": "661422075"
  },
  "t": 306
}











{
  "o": {
    "actions": [
      "Browsing"
    ],
    "target": "ndc://x1/public-chats",
    "ndcId": 1,
    "params": {
      "duration": 10770,
      "topicIds": []
    },
    "id": "658206538"
  },
  "t": 306
}



{
  "o": {
    "actions": [
      "Recording"
    ],
    "target": "ndc://x1/chat-thread/5462d834-8a55-4610-90a0-fafc5d7cc63e",
    "ndcId": 1,
    "params": {
      "duration": 1446,
      "topicIds": [],
      "threadType": 2
    },
    "id": "663340677"
  },
  "t": 306
}


#t: 306 = end, 304 = start (start & end same, but start don't have duration)

"""