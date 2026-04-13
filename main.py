from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from helpers.middleware import CheckRequest
from helpers.wsobjs import WSObjects
from datetime import datetime
from typing import List, Dict
import asyncio

# copycat from altamino/api, maybe we should shrink them
from objects.errors import Errors
from helpers.database.mongo import Database
from helpers.constants import WS_TYPE_PING, WS_TYPE_MARK_READ


app = FastAPI()


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


manager = ConnectionManager()


@app.get("/")
async def index():
    return Errors.InvalidRequest()


@app.get("/health")
async def health():
    return {"alive": True}


@app.websocket("/")
async def websocket_endpoint(ws: WebSocket):
    admin, uid, error = await CheckRequest(ws)
    if error:
        print(f"WebSocket connection rejected: {error['message']}")
        await ws.close(
            code=error.get("code", 1008), reason=error.get("message", "Unauthorized")
        )
        return

    if admin:
        await ws.accept()
    if not admin:
        await manager.connect(ws, uid)

    try:
        while True:
            data = await ws.receive_json()

            if data.get("t") and data.get("o"):
                # ping
                if data["t"] == WS_TYPE_PING:
                    await manager.answer(WSObjects.Pong(), ws)
                    continue

                # check for id
                if not data["o"].get("id"):
                    await manager.answer(WSObjects.WSError(1, "No ID of request"), ws)
                    continue

                ws_req_id = data["o"].get("id")
                if (
                    data["t"] == WS_TYPE_MARK_READ
                    and data["o"].get("markHasRead", None) is not None
                ):
                    if data["o"]["markHasRead"] is True:
                        ndcId = data["o"]["ndcId"]
                        chatId = data["o"]["threadId"]
                        readTimestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

                        db_instance = Database()
                        db = await db_instance.get(f"x{ndcId}")
                        await db["Chats"].update_one(
                            {"id": chatId},
                            {"$set": {f"lastReadedList.{uid}": readTimestamp}},
                        )
                    continue

            if data.get("ADMIN-SAYS") and admin:
                try:
                    js = data["ADMIN-SAYS"]
                    users = js["VICTIMS"]
                    payload = js["WEAPON"]

                    if users == "ALL":
                        f = await manager.broadcast(payload)
                    else:
                        f = await manager.selective_broadcast(payload, users)

                    await manager.answer(
                        {
                            "status": "ok",
                            "clients": len(users) if users != "ALL" else f,
                            "probably_got": f,
                        },
                        ws,
                    )
                except Exception as e:
                    await manager.answer({"status": "error", "reason": str(e)}, ws)
                continue

            continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
        await ws.close(code=1011, reason="Internal Server Error")
    finally:
        if not admin:
            manager.disconnect(ws, uid)
