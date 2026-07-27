import asyncio
import json
from typing import Dict, List, Optional

from handlers.handle_api import handle_api_message
from helpers.database.redis import get as get_redis
from objects.api_broadcast_types import ApiBroadcastType
from helpers.wsobjs import WSObjects
from fastapi import WebSocket
from redis import asyncio as aioredis
from starlette.websockets import WebSocketState

from .config import Config

PING_INTERVAL = 30
PING_TIMEOUT  = 10 
BROADCAST_ALL = "to-everyone"

class ConnectionManager:
    CHANNEL_CMD = "ws:commands"

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self._ping_tasks: Dict[WebSocket, asyncio.Task] = {}

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

    async def _ping_loop(self, websocket: WebSocket, uid: str):
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
                try:
                    await asyncio.wait_for(
                        websocket.send_json(WSObjects.Pong()),
                        timeout=PING_TIMEOUT,
                    )
                except (asyncio.TimeoutError, Exception):
                    try:
                        await websocket.close(code=1001)
                    except Exception:
                        pass
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


            payload = message if t == ApiBroadcastType.RawSend else handle_api_message(t, message)

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