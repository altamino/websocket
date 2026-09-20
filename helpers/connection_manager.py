import asyncio
import json
from typing import Dict, List, Optional, Set

from handlers.handle_api import handle_api_message
from helpers.database.redis import get as get_redis
from objects.api_broadcast_types import ApiBroadcastType
from helpers.wsobjs import WSObjects
from fastapi import WebSocket
from redis import asyncio as aioredis
from starlette.websockets import WebSocketState

from .config import Config

PING_INTERVAL = 30
PING_TIMEOUT = 10
BROADCAST_ALL = "to-everyone"


class ConnectionManager:
    CHANNEL_CMD = "ws:commands"

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self._ping_tasks: Dict[WebSocket, asyncio.Task] = {}
        self.thread_subscribers: Dict[str, Set[str]] = {}
        self.channel_members: Dict[str, List[dict]] = {}
        self.channel_types: Dict[str, int] = {}
        self.thread_ndc_ids: Dict[str, int] = {}
        self.user_busy_threads: Dict[str, str] = {}

        self.pubsub_redis = None
        self.pubsub = None
        self._listener_task: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self):
        self._stopping = False
        self._listener_task = asyncio.create_task(self._listen_forever())

    async def stop(self):
        self._stopping = True
        if self._listener_task:
            self._listener_task.cancel()
        if self.pubsub:
            await self.pubsub.unsubscribe()
            await self.pubsub.close()
        if self.pubsub_redis:
            await self.pubsub_redis.close()

    async def connect(self, websocket: WebSocket, uid: str):
        await websocket.accept()
        if uid not in self.active_connections:
            self.active_connections[uid] = []
        self.active_connections[uid].append(websocket)
        task = asyncio.create_task(self._ping_loop(websocket, uid))
        self._ping_tasks[websocket] = task

    def disconnect(self, websocket: WebSocket, uid: str):
        task = self._ping_tasks.pop(websocket, None)
        if task:
            task.cancel()

        conns = self.active_connections.get(uid)
        if conns is None:
            return
        try:
            conns.remove(websocket)
        except ValueError:
            pass
        if not conns:
            del self.active_connections[uid]
        if uid not in self.active_connections:
            for subs in self.thread_subscribers.values():
                subs.discard(uid)
            for members in self.channel_members.values():
                members[:] = [m for m in members if m["uid"] != uid]
            self.user_busy_threads.pop(uid, None)

    def subscribe_thread(self, uid: str, thread_id: str):
        if thread_id not in self.thread_subscribers:
            self.thread_subscribers[thread_id] = set()
        self.thread_subscribers[thread_id].add(uid)

    def unsubscribe_thread(self, uid: str, thread_id: str):
        if thread_id in self.thread_subscribers:
            self.thread_subscribers[thread_id].discard(uid)

    def set_thread_ndc(self, thread_id: str, ndc_id: int):
        if ndc_id:
            self.thread_ndc_ids[thread_id] = ndc_id

    def add_channel_member(
        self,
        thread_id: str,
        uid: str,
        join_role: int = 1,
        channel_uid: int = 0,
        ndc_id: int | None = None,
    ):
        if ndc_id:
            self.set_thread_ndc(thread_id, ndc_id)
        if thread_id not in self.channel_members:
            self.channel_members[thread_id] = []
        self.channel_members[thread_id] = [
            m for m in self.channel_members[thread_id] if m["uid"] != uid
        ]
        self.channel_members[thread_id].append(
            {"uid": uid, "joinRole": join_role, "channelUid": channel_uid}
        )

    def remove_channel_member(self, thread_id: str, uid: str):
        if thread_id in self.channel_members:
            self.channel_members[thread_id] = [
                m for m in self.channel_members[thread_id] if m["uid"] != uid
            ]
            if not self.channel_members[thread_id]:
                del self.channel_members[thread_id]

    def clear_channel_members(self, thread_id: str):
        self.channel_members.pop(thread_id, None)
        self.channel_types.pop(thread_id, None)
        self.thread_ndc_ids.pop(thread_id, None)

    def get_user_channel_infos(self, uid: str) -> list[dict]:
        infos: list[dict] = []
        for thread_id, members in self.channel_members.items():
            for member in members:
                if member["uid"] != uid:
                    continue
                infos.append(
                    {
                        "ndcId": self.thread_ndc_ids.get(thread_id, 0),
                        "threadId": thread_id,
                        "uid": uid,
                        "joinRole": member["joinRole"],
                    }
                )
                break
        return infos

    def get_channel_members(self, thread_id: str) -> List[dict]:
        return list(self.channel_members.get(thread_id, []))

    def set_channel_type(self, thread_id: str, channel_type: int):
        if channel_type:
            self.channel_types[thread_id] = channel_type
        else:
            self.channel_types.pop(thread_id, None)

    def get_channel_type(self, thread_id: str) -> int:
        return self.channel_types.get(thread_id, 0)

    def mark_user_busy(self, uid: str, thread_id: str):
        self.user_busy_threads[uid] = thread_id

    def clear_user_busy(self, uid: str):
        self.user_busy_threads.pop(uid, None)

    def is_user_busy(self, uid: str) -> bool:
        return uid in self.user_busy_threads

    async def broadcast_to_thread(
        self, thread_id: str, message: dict, exclude_uid: str | None = None
    ):
        uids = self.thread_subscribers.get(thread_id, set())
        targets = [uid for uid in uids if uid != exclude_uid]
        if targets:
            await self._local_selective_broadcast(message, targets)

    async def selective_broadcast(self, message: dict, uids: List[str]):
        await self._local_selective_broadcast(message, uids)

    async def _ping_loop(self, websocket: WebSocket, uid: str):
        # Keep the task alive for lifecycle symmetry only.
        # Do NOT send t:117 here — an empty threadChannelUserInfoList makes
        # the client call exitLiveChannel() for every active call channel.
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
        except asyncio.CancelledError:
            pass

    async def answer(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def _local_broadcast(self, message: dict):
        for connections in self.active_connections.values():
            for connection in connections:
                try:
                    await connection.send_json(message)
                except Exception:
                    pass

    async def _local_selective_broadcast(self, message: dict, uids: List[str]):
        tasks = []
        for uid in uids:
            if uid in self.active_connections:
                for connection in self.active_connections[uid]:
                    tasks.append(connection.send_json(message))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _connect_pubsub(self):
        self.pubsub_redis = aioredis.from_url(
            Config.REDIS_CONNECTION_STRING,
            decode_responses=True,
            socket_timeout=None,
            socket_connect_timeout=5,
            health_check_interval=25,
            retry_on_timeout=True,
        )
        self.pubsub = self.pubsub_redis.pubsub()
        await self.pubsub.subscribe(self.CHANNEL_CMD)

    async def _listen_forever(self):
        backoff = 1
        while not self._stopping:
            try:
                await self._connect_pubsub()
                print("redis pub/sub connected")
                backoff = 1
                await self._listen()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[WS listener error] {e!r}, reconnecting in {backoff}s")
                try:
                    if self.pubsub:
                        await self.pubsub.close()
                    if self.pubsub_redis:
                        await self.pubsub_redis.close()
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _listen(self):
        async for raw in self.pubsub.listen():
            if raw["type"] != "message":
                continue
            try:
                cmd = json.loads(raw["data"])
            except (TypeError, ValueError):
                print("decode error")
                continue

            message = cmd.get("message")
            uids = cmd.get("uids")
            t = cmd.get("type", 0)

            payload = (
                message
                if t == ApiBroadcastType.RawSend
                else handle_api_message(t, message)
            )

            if payload:
                if uids == BROADCAST_ALL:
                    await self._local_broadcast(payload)
                elif isinstance(uids, list) and uids:
                    await self._local_selective_broadcast(payload, uids)
                else:
                    print("No broadcast targets")


async def broadcast_ws_message(message: dict, uids: Optional[List[str]] = None):
    redis = get_redis()
    payload = {"message": message}
    if uids is not None:
        payload["uids"] = uids
    await redis.publish(ConnectionManager.CHANNEL_CMD, json.dumps(payload))
