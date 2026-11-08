"""Reconnection delivery without adding a new client protocol or replaying RTC."""

import asyncio
import hashlib

from helpers.database.mongo import Database
from helpers.ws_outbox import acknowledge, load_after
from handlers.handle_api import handle_api_message


class DurableDelivery:
    def __init__(self, manager):
        self.manager = manager
        self.db = None
        self.sessions = {}
        self.locks = {}
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self.loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.db:
            await self.db.close()

    async def store(self):
        if self.db is None:
            self.db = await Database().init()
        return await self.db.get()

    def connect(self, ws, uid):
        # CheckRequest already authenticated NDCAUTH. Never persist the token itself.
        token = ws.headers.get("NDCAUTH")
        if not token:
            return
        key = hashlib.sha256((uid + "\0" + token).encode()).hexdigest()
        self.sessions[ws] = [uid, key, None, None]
        self.locks[ws] = asyncio.Lock()

    def disconnect(self, ws):
        self.sessions.pop(ws, None)
        self.locks.pop(ws, None)

    async def allowed(self, uid, command):
        kind, message = command.get("type", 0), command["message"]
        if kind == 0:
            obj = message.get("o", {})
            chat = obj.get("chatMessage", {})
            ndc, thread = obj.get("ndcId"), chat.get("threadId")
        elif kind == 101:
            ndc, thread = message.get("ndcId"), message.get("threadId")
            chat = None
        else:
            ndc, thread, chat = message.get("ndcId", 0), None, None
        if not isinstance(ndc, int) or ndc < 0:
            return False
        if kind in (0, 101) and not thread:
            return False
        users = await self.db.get(f"x{ndc}", "Users")
        user = await users.find_one({"id": uid}, {"status": 1, "following": 1})
        global_users = await self.db.get(table="Users")
        account = await global_users.find_one({"id": uid}, {"status": 1})
        if (
            not user
            or user.get("status", 0) != 0
            or not account
            or account.get("status", 0) != 0
        ):
            return False
        if kind == 120:
            actor_uid = message.get("user", {}).get("uid")
            source = await users.find_one(
                {"id": actor_uid, "status": {"$nin": [5, 9, 10]}}
            )
            if not source or uid not in (source.get("following") or []):
                return False
            # Recompute reader-relative bits at delivery time, never replay an
            # obsolete follower relationship from a queued profile snapshot.
            outgoing = set(user.get("following") or [])
            bits = int(actor_uid in outgoing) + 2
            profile = dict(
                message["user"],
                followingStatus=bits,
                membershipStatus=bits,
                joinedCount=len(set(source.get("following") or [])),
            )
            command["message"] = dict(message, user=profile)
        if thread:
            chats = await self.db.get(f"x{ndc}", "Chats")
            member = await chats.find_one(
                {"id": thread, "$or": [{"memberList": uid}, {"invitedList": uid}]},
                {"id": 1},
            )
            if not member:
                return False
            if chat:
                history = await self.db.get(f"x{ndc}", f"_Chat:{thread}")
                current = await history.find_one({"messageId": chat.get("messageId")})
                # Skip withdrawn/deleted/replaced content; a later event or REST supplies current state.
                if not current or current.get("messageType") != chat.get("type"):
                    return False
                if current.get("content") != chat.get("content"):
                    return False
        return True

    async def flush(self, ws):
        lock = self.locks.get(ws)
        if lock is None:
            return
        async with lock:
            state = self.sessions.get(ws)
            if state is None:
                return
            uid, key, cursor, epoch = state
            if not await self.manager.extensions.authenticated(ws, uid):
                return
            database = await self.store()
            if cursor is None:
                saved = await database["WsDeliveryCursors"].find_one({"_id": key}) or {}
                cursor = state[2] = saved.get("sequence", 0)
                epoch = state[3] = saved.get("epoch")
            row = await load_after(database, uid, cursor, epoch)
            current_epoch = row.get("epoch")
            for event in row.get("events", []):
                if ws not in self.sessions:
                    return
                command = event["command"]
                if await self.allowed(uid, command):
                    payload = (
                        command["message"]
                        if command.get("type", 0) == 0
                        else handle_api_message(command["type"], command["message"])
                    )
                    if payload:
                        # If send raises, no acknowledgment: reconnect/retry retains this event.
                        await self.manager.answer(payload, ws)
                await acknowledge(database, key, event["seq"], current_epoch)
                state[2] = event["seq"]
                state[3] = current_epoch
            if (
                row
                and not row.get("events")
                and (row["sequence"] != state[2] or current_epoch != state[3])
            ):
                await acknowledge(database, key, row["sequence"], current_epoch)
                state[2] = row["sequence"]
                state[3] = current_epoch

    async def flush_users(self, uids):
        sockets = [ws for ws, s in list(self.sessions.items()) if s[0] in uids]
        if sockets:
            results = await asyncio.gather(
                *(self.flush(ws) for ws in sockets), return_exceptions=True
            )
            for result in results:
                if isinstance(result, Exception):
                    print(
                        "[WS durable] delivery will retry:", result.__class__.__name__
                    )

    async def loop(self):
        while True:
            try:
                await self.flush_users({s[0] for s in list(self.sessions.values())})
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print("[WS durable] storage retry:", error.__class__.__name__)
            await asyncio.sleep(3)
