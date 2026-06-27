import asyncio
import json
from fastapi import WebSocket
from typing import List, Dict, Optional
from helpers.database.redis import get as get_redis


class ConnectionManager:
    CHANNEL_CMD = "ws:commands"

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self.redis = None
        self.pubsub = None
        self._listener_task: Optional[asyncio.Task] = None

    async def start(self):
        self.redis = get_redis()
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(self.CHANNEL_CMD)
        self._listener_task = asyncio.create_task(self._listen_with_guard())

    async def _listen_with_guard(self):
        try:
            await self._listen()
        except Exception as e:
            print(f"[FATAL] WS listener died: {e}")

    async def stop(self):
        if self._listener_task:
            self._listener_task.cancel()
        if self.pubsub:
            await self.pubsub.unsubscribe()
            await self.pubsub.close()

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


    async def _local_selective_broadcast(self, message: dict, uids: List[str]):
        tasks = []
        for uid in uids:
            if uid in self.active_connections.keys():
                for connection in self.active_connections[uid]:
                    tasks.append(connection.send_json(message))
        if tasks:
            await asyncio.gather(*tasks)

    async def _local_broadcast(self, message: dict):
        for connections in self.active_connections.values():
            for connection in connections:
                await connection.send_json(message)

    async def _listen(self):
        async for raw in self.pubsub.listen():
            if raw["type"] != "message":
                continue  # пропускаем subscribe/unsubscribe подтверждения

            try:
                cmd = json.loads(raw["data"])
            except (TypeError, ValueError):
                print("decode error")
                continue  # пропускаем, если не смогли распарсить

            message = cmd.get("message")
            uids = cmd.get("uids")

            if uids:
                await self._local_selective_broadcast(message, uids)
            else:
                await self._local_broadcast(message)

    async def selective_broadcast(self, message: dict, uids: List[str]):
        await self.redis.publish(
            self.CHANNEL_CMD, json.dumps({"message": message, "uids": uids})
        )

    async def broadcast(self, message: dict):
        await self.redis.publish(
            self.CHANNEL_CMD, json.dumps({"message": message})
        )