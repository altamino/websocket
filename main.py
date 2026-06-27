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
            try:
                data = await ws.receive_json()
            except Exception as e:
                print(f"failed receive json: {e}")
                break  # соединение мертво — выходим, finally сделает disconnect

            if data.get("t") and data.get("o"):
                try:
                    await handle_message(data, manager, uid, admin, ws)
                except Exception as e:
                    print(f"handle_message error: {e}")
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        manager.disconnect(ws, uid)