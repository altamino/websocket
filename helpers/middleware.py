from fastapi import WebSocket

from .config import Config
from .processors.device import DeviceProcessor
from .processors.session import SessionProcessor
from .processors.signature import SignatureProcessor


async def CheckRequest(
    websocket: WebSocket,
) -> list:
    admin = False
    uid = None

    try:
        auid = websocket.headers["AUID"]
        auth = websocket.headers["NDCAUTH"]
        device = websocket.headers["NDCDEVICEID"]
        signature = websocket.headers["NDC-MSG-SIG"]
        body = websocket.query_params["signbody"].split("|")

        if len(body) != 2 or body[0] != device:
            raise Exception()

        data_time = body[1]
    except Exception:
        return [
            None,
            None,
            {"code": 1003, "message": "Unsupported or invalid data."},
        ]

    sid = await SessionProcessor.Get(auth)
    if not sid or sid["uid"] != auid:
        return [
            None,
            None,
            {"code": 1003, "message": "Unsupported or invalid session."},
        ]

    did_valid = DeviceProcessor.Validate(device)
    sig_valid = SignatureProcessor.Validate(signature, f"{device}|{data_time}")
    if not did_valid or not sig_valid:
        return [
            None,
            None,
            {"code": 1003, "message": "Unsupported or invalid device."},
        ]

    uid = sid["uid"]

    return [admin, uid, None]
