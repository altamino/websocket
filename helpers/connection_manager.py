from fastapi import WebSocket
from typing import List, Dict
import asyncio



class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

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

    async def selective_broadcast(self, message: dict, uids: List[str]):
        tasks = []
        got_counter = 0
        for uid in uids:
            if uid in self.active_connections.keys():
                for connection in self.active_connections[uid]:
                    tasks.append(connection.send_json(message))
                    got_counter += 1
        if tasks:
            await asyncio.gather(*tasks)

        return got_counter

    async def broadcast(self, message: dict):
        got_counter = 0
        for connections in self.active_connections.values():
            for connection in connections:
                await connection.send_json(message)
                got_counter += 1

        return got_counter
