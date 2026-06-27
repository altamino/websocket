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

        # отдельный клиент только для pub/sub, не шарим с общим кэш-клиентом
        self.pubsub_redis = None
        self.pubsub = None
        self._listener_task: Optional[asyncio.Task] = None

    async def start(self):
        print("starting redis pub/sub")
        self.pubsub_redis = aioredis.from_url(
            Config.REDIS_CONNECTION_STRING, decode_responses=True
        )
        self.pubsub = self.pubsub_redis.pubsub()
        await self.pubsub.subscribe(self.CHANNEL_CMD)
        self._listener_task = asyncio.create_task(self._listen_with_guard())

    async def stop(self):
        print("closing redis pub/sub")
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

    # --- слушатель Redis pub/sub ---

    async def _listen_with_guard(self):
        try:
            await self._listen()
        except Exception as e:
            print(f"[FATAL] WS listener died: {e}")

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

            if uids:
                await self._local_selective_broadcast(message, uids)
            else:
                await self._local_broadcast(message)