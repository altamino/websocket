"""Bound pending application frames; failed sends never acknowledge durable events."""

import asyncio, json


async def answer(manager, message, ws):
    raw = json.dumps(
        message, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    size = len(raw.encode("utf-8"))
    pending = manager._pending_sends.setdefault(ws, [0, 0])
    try:
        if pending[0] >= 32 or pending[1] + size > 262144:
            raise RuntimeError("Slow consumer: pending frame limit exceeded")
        pending[0] += 1
        pending[1] += size
        try:
            lock = manager._send_locks.setdefault(ws, asyncio.Lock())
            async with lock:
                await asyncio.wait_for(ws.send_text(raw), timeout=10)
        finally:
            pending[0] -= 1
            pending[1] -= size
    except Exception:
        for uid, connections in list(manager.active_connections.items()):
            if ws in connections:
                manager.disconnect(ws, uid)
                break
        try:
            await asyncio.wait_for(
                ws.close(code=1013, reason="Transport unavailable or slow consumer"),
                timeout=2,
            )
        except Exception:
            pass
        raise
