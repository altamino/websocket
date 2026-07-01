from fastapi import WebSocket
from typing import List, Dict, Optional, Set
import asyncio
import json

from helpers.database.redis import get as get_redis



class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # thread_id -> set of uids subscribed to that thread
        self.thread_subscribers: Dict[str, Set[str]] = {}
        # thread_id -> list of active voice-channel members {uid, joinRole, channelUid}
        self.channel_members: Dict[str, List[dict]] = {}
        # thread_id -> active channelType (1=voice, 4=video, 5=screenroom); absent means no live
        self.channel_types: Dict[str, int] = {}

    async def connect(self, websocket: WebSocket, uid: str):
        await websocket.accept()
        if uid not in self.active_connections:
            self.active_connections[uid] = []
        self.active_connections[uid].append(websocket)

    def disconnect(self, websocket: WebSocket, uid: str):
        self.active_connections[uid].remove(websocket)
        if not self.active_connections[uid]:
            del self.active_connections[uid]
        # Remove uid from all thread subscriptions if no more connections
        if uid not in self.active_connections:
            for subs in self.thread_subscribers.values():
                subs.discard(uid)
            # Remove from voice channel members too
            for members in self.channel_members.values():
                members[:] = [m for m in members if m["uid"] != uid]

    def subscribe_thread(self, uid: str, thread_id: str):
        if thread_id not in self.thread_subscribers:
            self.thread_subscribers[thread_id] = set()
        self.thread_subscribers[thread_id].add(uid)

    def unsubscribe_thread(self, uid: str, thread_id: str):
        if thread_id in self.thread_subscribers:
            self.thread_subscribers[thread_id].discard(uid)

    def add_channel_member(self, thread_id: str, uid: str, join_role: int = 1, channel_uid: int = 0):
        if thread_id not in self.channel_members:
            self.channel_members[thread_id] = []
        # Remove any existing entry for this uid first
        self.channel_members[thread_id] = [m for m in self.channel_members[thread_id] if m["uid"] != uid]
        self.channel_members[thread_id].append({"uid": uid, "joinRole": join_role, "channelUid": channel_uid})

    def remove_channel_member(self, thread_id: str, uid: str):
        if thread_id in self.channel_members:
            self.channel_members[thread_id] = [m for m in self.channel_members[thread_id] if m["uid"] != uid]
            if not self.channel_members[thread_id]:
                del self.channel_members[thread_id]

    def clear_channel_members(self, thread_id: str):
        self.channel_members.pop(thread_id, None)
        self.channel_types.pop(thread_id, None)

    def get_channel_members(self, thread_id: str) -> List[dict]:
        return list(self.channel_members.get(thread_id, []))

    def set_channel_type(self, thread_id: str, channel_type: int):
        if channel_type:
            self.channel_types[thread_id] = channel_type
        else:
            self.channel_types.pop(thread_id, None)

    def get_channel_type(self, thread_id: str) -> int:
        return self.channel_types.get(thread_id, 0)

    async def answer(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def start(self):
        pass  # no-op; kept for compatibility with main.py lifespan

    async def stop(self):
        pass  # no-op

    async def broadcast_to_thread(self, thread_id: str, message: dict, exclude_uid: str = None):
        uids = self.thread_subscribers.get(thread_id, set())
        tasks = []
        for uid in uids:
            if uid == exclude_uid:
                continue
            for conn in self.active_connections.get(uid, []):
                tasks.append(conn.send_json(message))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def selective_broadcast(self, message: dict, uids: List[str]):
        tasks = []
        for uid in uids:
            for conn in self.active_connections.get(uid, []):
                tasks.append(conn.send_json(message))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# Module-level helper used by handlers/chat.py (mirrors the Redis-based version
# in the upstream but falls back to a direct broadcast via the singleton manager).
_manager_ref: Optional["ConnectionManager"] = None

async def broadcast_ws_message(message: dict, uids: Optional[List[str]] = None):
    """Send a WS message to specific uids (or all if uids is None)."""
    if _manager_ref is None:
        return
    if uids is not None:
        await _manager_ref.selective_broadcast(message, uids)
    else:
        for connections in _manager_ref.active_connections.values():
            for conn in connections:
                try:
                    await conn.send_json(message)
                except Exception:
                    pass
