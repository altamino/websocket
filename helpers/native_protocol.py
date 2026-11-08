"""Kioraa native signalling features adapted to AltAmino's existing Python stack."""

import asyncio, json, re, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from helpers.config import Config
from helpers.database.redis import get as get_redis
from helpers.processors.session import SessionProcessor
from helpers.native_access import NativeAccess, NativeError, native_id
from helpers.native_activities import NativeActivities
from helpers.channel_store import ChannelStore, LEASE

INCOMING = {
    20,
    100,
    103,
    105,
    108,
    112,
    116,
    120,
    122,
    126,
    132,
    134,
    136,
    138,
    200,
    300,
    302,
    304,
    306,
    1001,
}
ROOM_OPS = {100, 103, 105, 108, 112, 120, 122, 126, 132, 134, 136, 138, 200, 1001}
MEDIA_OPS = {120, 122, 126, 132, 134, 136, 138, 200}


class NativeProtocol:
    def __init__(self, manager):
        self.manager = manager
        self.access = NativeAccess()
        self.activities = NativeActivities(self.access, manager)
        self.channels = ChannelStore(self.access, manager)
        self.frames = {}
        self.rates = {}
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self.maintenance())

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        for ws in list(self.activities.sockets):
            await self.activities.disconnect(ws)
        await self.access.close()

    def connect(self, ws, uid):
        self.activities.connect(ws, uid)
        self.rates[ws] = deque()

    def disconnect(self, ws):
        self.rates.pop(ws, None)
        self.frames.pop(ws, None)
        self.manager._spawn(self.activities.disconnect(ws))

    async def authenticated(self, ws, uid):
        session = await SessionProcessor.Get(ws.headers.get("NDCAUTH"))
        if not session or session.get("uid") != uid:
            await ws.close(code=1008, reason="Session expired or revoked")
            self.manager.disconnect(ws, uid)
            return False
        account = await (await self.access.global_collection("Users")).find_one(
            {"id": uid}, {"status": 1}
        )
        if not account or account.get("status", 0) in (5, 9, 10):
            await ws.close(code=1008, reason="Account unavailable")
            self.manager.disconnect(ws, uid)
            return False
        return True

    async def receive(self, ws):
        raw = await ws.receive_text()
        if len(raw.encode()) > 65536:
            raise NativeError("WebSocket payload is too large.", 101)
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            raise NativeError("Malformed JSON.", 101)
        return data

    @asynccontextmanager
    async def frame(self, data, uid, ws):
        if not await self.authenticated(ws, uid):
            raise NativeError("Session expired.", 105)
        if (
            not isinstance(data, dict)
            or type(data.get("t")) is not int
            or not isinstance(data.get("o"), dict)
        ):
            raise NativeError("Malformed native frame.", 101)
        t, o = data["t"], data["o"]
        if t not in INCOMING:
            raise NativeError("Unknown or server-only operation.", 109)
        queue = self.rates.setdefault(ws, deque())
        now = time.monotonic()
        while queue and queue[0] < now - 60:
            queue.popleft()
        if len(queue) >= 120:
            await ws.close(code=1008, reason="Message rate exceeded")
            raise NativeError("Message rate exceeded.", 109)
        queue.append(now)
        if t not in (20, 116) and (
            not isinstance(o.get("id"), str) or not 1 <= len(o["id"]) <= 128
        ):
            raise NativeError("Request correlation ID required.", 101)
        if t in ROOM_OPS:
            ndc, tid = o.get("ndcId"), o.get("threadId")
            room = await self.access.room(
                uid, ndc, tid, organizer=t in (108, 126, 132, 134)
            )
            media = (
                t in MEDIA_OPS
                or (t == 108 and o.get("channelType") != 0)
                or (t == 112 and o.get("joinRole") != 0)
            )
            if media and (not Config.AGORA_APP_ID or not Config.AGORA_APP_CERTIFICATE):
                raise NativeError(
                    "Agora is not configured; no media session was created.", 117
                )
            async with self.channels.locked(ndc, tid, uid) as state:
                self.frames[ws] = (ndc, tid, room, state)
                try:
                    if t in MEDIA_OPS and not self.manager.get_channel_type(tid):
                        raise NativeError("No active live channel.", 117)
                    if t == 108 and o.get("channelType") not in (0, 1, 3, 4, 5):
                        raise NativeError("Unsupported channel type.", 109)
                    if t == 112:
                        role = o.get("joinRole")
                        if type(role) is not int or role not in (0, 1, 2, 3):
                            raise NativeError("Invalid channel role.", 101)
                        if role == 1:
                            waiting = await self.channels.waiting()
                            approval = await waiting.find_one(
                                {
                                    "_id": f"{ndc}:{tid}:{uid}",
                                    "kind": "approved",
                                    "expiresAt": {"$gt": datetime.now(timezone.utc)},
                                }
                            )
                            if (
                                not self.access.organizer(room, uid)
                                and room.get("vvChatJoinType", 1) in (2, 3)
                                and not approval
                            ):
                                raise NativeError("Presenter approval required.")
                            if approval:
                                await waiting.delete_one(
                                    {"_id": approval["_id"], "kind": "approved"}
                                )
                            busy = await (await self.channels.collection()).find_one(
                                {
                                    "_id": {"$ne": self.channels.key(ndc, tid)},
                                    "channelType": {"$ne": 0},
                                    "members": {
                                        "$elemMatch": {
                                            "uid": uid,
                                            "joinRole": 1,
                                            "lastSeen": {
                                                "$gt": datetime.now(timezone.utc)
                                                - LEASE
                                            },
                                        }
                                    },
                                }
                            )
                            if busy:
                                raise NativeError(
                                    "Already presenting in another channel.", 111
                                )
                    if t == 200 and not any(
                        m["uid"] == uid for m in self.manager.get_channel_members(tid)
                    ):
                        raise NativeError(
                            "Join the live channel before requesting credentials."
                        )
                    yield
                finally:
                    self.frames.pop(ws, None)
        else:
            if t not in (20, 116):
                await self.access.account(uid, o.get("ndcId"))
            yield

    async def reply(self, ws, t, o, **extra):
        await self.manager.answer(
            {
                "t": t,
                "o": {k: o[k] for k in ("id", "ndcId", "threadId") if k in o} | extra,
            },
            ws,
        )

    async def send_to(self, uids, frame):
        if uids:
            await get_redis().publish(
                "ws:commands", json.dumps({"message": frame, "uids": list(set(uids))})
            )

    async def broadcast_room(self, tid, frame, exclude=None):
        ndc = self.manager.thread_ndc_ids.get(tid, 0)
        redis = get_redis()
        key = f"ws:thread-subs:{ndc}:{tid}"
        await redis.zremrangebyscore(key, "-inf", time.time())
        raw = await redis.zrange(key, 0, -1)
        uids = {
            str(x.decode() if isinstance(x, bytes) else x).split(":")[0] for x in raw
        }
        uids.update(self.manager.thread_subscribers.get(tid, set()))
        uids.update(m["uid"] for m in self.manager.get_channel_members(tid))
        allowed = []
        for target in uids - {exclude}:
            try:
                await self.access.room(target, ndc, tid)
            except NativeError:
                continue
            allowed.append(target)
        await self.send_to(allowed, frame)

    async def waiting_list(self, ndc, tid, viewer):
        profiles = []
        cursor = (
            (await self.channels.waiting())
            .find(
                {
                    "ndcId": ndc,
                    "threadId": tid,
                    "kind": "waiting",
                    "expiresAt": {"$gt": datetime.now(timezone.utc)},
                }
            )
            .sort([("requestedAt", 1), ("_id", 1)])
        )
        async for entry in cursor:
            try:
                await self.access.room(entry["uid"], ndc, tid)
            except NativeError:
                continue
            profile = await self.access.profile(entry["uid"], ndc, viewer)
            if profile:
                profiles.append(profile)
        return profiles

    async def publish_waiting(self, ndc, tid, viewer):
        await self.broadcast_room(
            tid,
            {
                "t": 131,
                "o": {
                    "ndcId": ndc,
                    "threadId": tid,
                    "userProfileList": await self.waiting_list(ndc, tid, viewer),
                },
            },
        )

    async def handle(self, data, uid, ws):
        t, o = data["t"], data["o"]
        if t == 20:

            def clean(value, depth=0):
                if depth > 8:
                    return None
                if isinstance(value, dict):
                    return {
                        k: clean(v, depth + 1)
                        for k, v in value.items()
                        if not re.search(
                            "secret|password|token|ndcauth|authorization|cookie",
                            k,
                            re.I,
                        )
                    }
                if isinstance(value, list):
                    return [clean(x, depth + 1) for x in value[:100]]
                return value

            await (await self.access.global_collection("WsProtocolEvents")).insert_one(
                {
                    "uid": uid,
                    "eventType": 20,
                    "payload": clean(o),
                    "createdAt": datetime.now(timezone.utc),
                    "expiresAt": datetime.now(timezone.utc) + timedelta(days=7),
                }
            )
            return True
        if t == 116:
            from helpers.wsobjs import WSObjects

            await self.activities.heartbeat(ws)
            await self.manager.answer(
                WSObjects.Pong(o.get("id"), await self.channels.heartbeat(uid)), ws
            )
            return True
        if t in (300, 302):
            await self.activities.subscribe(
                ws, uid, o["ndcId"], o.get("topic"), t == 300
            )
            await self.reply(ws, t + 1, o)
            return True
        if t in (304, 306):
            await self.activities.activity(
                ws, uid, o["ndcId"], o.get("target"), o.get("actions"), t == 304
            )
            await self.reply(ws, t + 1, o)
            return True
        if t not in (120, 122, 126, 132, 134, 136, 138):
            return False
        ndc, tid, room, state = self.frames[ws]
        waiting = await self.channels.waiting()
        now = datetime.now(timezone.utc)
        members = self.manager.get_channel_members(tid)
        if not any(m.get("joinRole") == 1 for m in members):
            raise NativeError("No presenter is active.", 117)
        if t in (120, 122):
            if self.manager.get_channel_type(tid) != 5:
                raise NativeError("Screening room is not active.", 117)
            if t == 120:
                if not self.access.organizer(room, uid):
                    raise NativeError(
                        "Only the screening-room host controls the playlist."
                    )
                playlist = o.get("playlist")
                if (
                    not isinstance(playlist, dict)
                    or len(json.dumps(playlist).encode()) > 32768
                ):
                    raise NativeError("Invalid or oversized playlist.", 101)
                if "items" in playlist and (
                    not isinstance(playlist["items"], list)
                    or len(playlist["items"]) > 200
                ):
                    raise NativeError("Invalid playlist items.", 101)
                state["playlist"] = playlist
                await self.broadcast_room(
                    tid,
                    {
                        "t": 119,
                        "o": {"ndcId": ndc, "threadId": tid, "playlist": playlist},
                    },
                    uid,
                )
                await self.reply(ws, 121, o)
            else:
                await self.reply(
                    ws, 119, o, playlist=state.get("playlist", {"items": []})
                )
            return True
        if t == 126:
            target = native_id(o.get("targetUid"))
            await self.access.room(target, ndc, tid)
            found = next(
                (m for m in members if m["uid"] == target and m["joinRole"] == 1), None
            )
            if found is None:
                raise NativeError("Target is not presenting.")
            found["joinRole"] = 2
            await self.send_to(
                [target],
                {"t": 128, "o": {"ndcId": ndc, "threadId": tid, "joinRole": 2}},
            )
            from handlers.call import _finish_or_broadcast

            await _finish_or_broadcast(
                uid, ndc, tid, self.manager, self.manager.get_channel_type(tid)
            )
            await self.reply(ws, 127, o)
            return True
        if t == 138:
            if not any(m["uid"] == uid for m in members):
                raise NativeError("Join the live channel before requesting to speak.")
            await waiting.update_one(
                {"_id": f"{ndc}:{tid}:{uid}"},
                {
                    "$setOnInsert": {
                        "uid": uid,
                        "ndcId": ndc,
                        "threadId": tid,
                        "kind": "waiting",
                        "requestedAt": now,
                        "expiresAt": now + timedelta(minutes=15),
                    }
                },
                upsert=True,
            )
            await self.publish_waiting(ndc, tid, uid)
            await self.reply(ws, 139, o)
            return True
        if t == 134:
            target = native_id(o.get("uid"))
            await self.access.room(target, ndc, tid)
            result = await waiting.update_one(
                {
                    "_id": f"{ndc}:{tid}:{target}",
                    "kind": "waiting",
                    "expiresAt": {"$gt": now},
                },
                {
                    "$set": {
                        "kind": "approved",
                        "approvedBy": uid,
                        "expiresAt": now + timedelta(minutes=5),
                    }
                },
            )
            if not result.matched_count:
                raise NativeError("The request is no longer waiting.")
            await self.send_to(
                [target], {"t": 130, "o": {"ndcId": ndc, "threadId": tid}}
            )
            await self.reply(
                ws, 135, o, isOnline=any(m["uid"] == target for m in members)
            )
        elif t == 136:
            target = native_id(o.get("uid", uid))
            if target != uid and not self.access.organizer(room, uid):
                raise NativeError("Only an organizer can reject other requests.")
            await waiting.delete_one(
                {"_id": f"{ndc}:{tid}:{target}", "kind": "waiting"}
            )
            await self.reply(ws, 137, o)
        elif t == 132:
            await waiting.delete_many(
                {"ndcId": ndc, "threadId": tid, "kind": "waiting"}
            )
            await self.reply(ws, 133, o)
        await self.publish_waiting(ndc, tid, uid)
        return True

    async def after_legacy(self, data, uid, ws):
        t, o = data["t"], data["o"]
        if t in (100, 103):
            await self.activities.room_subscription(
                ws, o["ndcId"], o["threadId"], t == 100
            )
        if t in (100, 105):
            entry = await (await self.channels.waiting()).find_one(
                {
                    "_id": f"{o['ndcId']}:{o['threadId']}:{uid}",
                    "kind": "approved",
                    "expiresAt": {"$gt": datetime.now(timezone.utc)},
                }
            )
            if entry and self.manager.get_channel_type(o["threadId"]):
                await self.manager.answer(
                    {"t": 130, "o": {"ndcId": o["ndcId"], "threadId": o["threadId"]}},
                    ws,
                )

    async def maintenance(self):
        ticks = 0
        while True:
            try:
                for ws, state in list(self.activities.sockets.items()):
                    if not await self.authenticated(ws, state["uid"]):
                        continue
                await self.activities.poll()
                ticks += 1
                if ticks % 5 == 0:
                    now = datetime.now(timezone.utc)
                    async for state in (
                        (await self.channels.collection())
                        .find(
                            {
                                "channelType": {"$ne": 0},
                                "startedAt": {"$lt": now - LEASE},
                            }
                        )
                        .limit(100)
                    ):
                        try:
                            async with self.channels.locked(
                                state["ndcId"], state["threadId"]
                            ) as fresh:
                                if not any(
                                    m.get("joinRole") == 1
                                    for m in self.manager.get_channel_members(
                                        state["threadId"]
                                    )
                                ):
                                    from handlers.call import _finish_channel

                                    room = (
                                        await (
                                            await self.access.collection(
                                                state["ndcId"], "Chats"
                                            )
                                        ).find_one(
                                            {"id": state["threadId"]}, {"hostId": 1}
                                        )
                                        or {}
                                    )
                                    if room.get("hostId"):
                                        await _finish_channel(
                                            room["hostId"],
                                            state["ndcId"],
                                            state["threadId"],
                                            self.manager,
                                            fresh.get("channelType"),
                                        )
                                    else:
                                        await self.broadcast_room(
                                            state["threadId"],
                                            {
                                                "t": 115,
                                                "o": {
                                                    "threadId": state["threadId"],
                                                    "ndcId": state["ndcId"],
                                                },
                                            },
                                        )
                                        self.manager.clear_channel_members(
                                            state["threadId"]
                                        )
                        except NativeError:
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as error:
                print("[WS protocol] maintenance retry:", error.__class__.__name__)
            await asyncio.sleep(3)
