import asyncio
import json
from typing import Dict, List, Optional

from fastapi import WebSocket
from redis import asyncio as aioredis

from .config import Config


class ConnectionManager:
    CHANNEL_CMD = "ws:commands"

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

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

    # --- локальные WS-соединения этого процесса ---

    async def connect(self, websocket: WebSocket, uid: str):
        await websocket.accept()
        if uid not in self.active_connections:
            self.active_connections[uid] = []
        self.active_connections[uid].append(websocket)

    def disconnect(self, websocket: WebSocket, uid: str):
        self.active_connections[uid].remove(websocket)
        if not self.active_connections[uid]:
            del self.active_connections[uid]

    async def answer(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    # --- доставка локальным клиентам этого процесса ---

    async def _local_broadcast(self, message: dict):
        for connections in self.active_connections.values():
            for connection in connections:
                await connection.send_json(message)

    async def _local_selective_broadcast(self, message: dict, uids: List[str]):
        tasks = []
        for uid in uids:
            if uid in self.active_connections.keys():
                for connection in self.active_connections[uid]:
                    tasks.append(connection.send_json(message))
        if tasks:
            await asyncio.gather(*tasks)



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
            print(f"got ws command, uids={uids}")

            if uids:
                await self._local_selective_broadcast(message, uids)
            else:
                await self._local_broadcast(message)