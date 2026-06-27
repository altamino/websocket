from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from helpers.middleware import CheckRequest
from objects.errors import Errors
from handlers import handle_message
from helpers.connection_manager import ConnectionManager

manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    yield
    await manager.stop()

app = FastAPI(lifespan=lifespan)

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
        await ws.close(code=error.get("code", 1008), reason=error.get("message", "Unauthorized"))
        return

    await manager.connect(ws, uid)

    try:
        while True:
            data = await ws.receive_json()
            if data.get("t") and data.get("o"):
                try:
                    await handle_message(data, manager, uid, admin, ws)
                except Exception as e:
                    print(e)
            continue
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
        await ws.close(code=1011, reason="Internal Server Error")
    finally:
        manager.disconnect(ws, uid)