"""Connection-scoped live-layer leases and native subscription snapshots."""

import hashlib, json, re, time
from uuid import uuid4
from helpers.database.redis import get as get_redis
from helpers.native_access import NativeError, native_id

ACTIONS = {
    "Browsing",
    "Chatting",
    "Commenting",
    "Playing",
    "Polling",
    "Recording",
    "Typing",
    "Voting",
}
CATEGORIES = {
    "users-browsing-pages",
    "users-browsing-blogs",
    "users-playing-quizzes",
    "users-polling-polls",
    "users-browsing-items",
    "users-browsing-item",
    "users-browsing-user-profiles",
    "users-chatting",
    "users-chatting-private",
    "users-commenting-blogs",
    "users-voting-blogs",
    "users-live-chatting",
}


class NativeActivities:
    def __init__(self, access, manager):
        self.access = access
        self.manager = manager
        self.sockets = {}

    def connect(self, ws, uid):
        self.sockets[ws] = {
            "id": uuid4().hex,
            "uid": uid,
            "activities": {},
            "subscriptions": {},
            "rooms": set(),
        }

    async def disconnect(self, ws):
        state = self.sockets.pop(ws, None)
        if not state:
            return
        for entry in list(state["activities"].values()):
            await self.write(state, entry, False)
        redis = get_redis()
        for ndc, tid in state["rooms"]:
            await redis.zrem(
                f"ws:thread-subs:{ndc}:{tid}", state["uid"] + ":" + state["id"]
            )

    @staticmethod
    def topic(ndc, value):
        prefix = f"ndtopic:{'x' + str(ndc) if ndc else 'g'}:"
        topic = (
            value[len(prefix) :]
            if isinstance(value, str) and value.startswith(prefix)
            else value
        )
        if topic in CATEGORIES and ndc:
            return topic, prefix + topic
        if isinstance(topic, str) and re.fullmatch(
            r"users-(?:start|end)-(?:typing|recording)-at:[0-9a-f-]{36}", topic
        ):
            native_id(topic.rsplit(":", 1)[1])
            return topic, prefix + topic
        if (
            ndc
            and isinstance(topic, str)
            and re.fullmatch(
                r"users-browsing-(?:blog|item|user-profile)-at:[0-9a-f-]{36}", topic
            )
        ):
            native_id(topic.rsplit(":", 1)[1])
            return topic, prefix + topic
        raise NativeError("Unsupported live-layer topic.", 109)

    async def subscribe(self, ws, uid, ndc, value, enabled):
        await self.access.account(uid, ndc)
        topic, wire = self.topic(ndc, value)
        if "typing-at:" in topic or "recording-at:" in topic:
            await self.access.room(uid, ndc, topic.rsplit(":", 1)[1])
        state = self.sockets[ws]
        key = (ndc, wire)
        if enabled:
            if len(state["subscriptions"]) >= 100 and key not in state["subscriptions"]:
                raise NativeError("Too many subscriptions.", 109)
            state["subscriptions"][key] = None
            await self.send_snapshot(ws, state, ndc, wire, topic)
        else:
            state["subscriptions"].pop(key, None)

    @staticmethod
    def target(ndc, value):
        expected = "ndc://" + ("x" + str(ndc) if ndc else "g") + "/"
        if not isinstance(value, str) or not value.startswith(expected):
            raise NativeError("Activity scope mismatch.", 101)
        path = value[len(expected) :]
        allowed = r"(?:|featured|online-members|public-chats|shared-folder|(?:blog|quiz|poll|wiki|item|user-profile|chat-thread|blog-category|item-category|shared-folder)/[0-9a-f-]{36})"
        if not re.fullmatch(allowed, path) or (
            ndc == 0 and not path.startswith("chat-thread/")
        ):
            raise NativeError("Unsupported activity target.", 109)
        return path

    async def write(self, state, entry, enabled):
        redis = get_redis()
        ndc, path, actions = entry["ndc"], entry["path"], entry["actions"]
        lease = hashlib.sha256((path + "|" + ",".join(actions)).encode()).hexdigest()[
            :24
        ]
        key = f"x{ndc}:activity:{state['uid']}:{state['id']}:{lease}"
        if enabled:
            row = await self.access.account(state["uid"], ndc)
            value = {
                "uid": state["uid"],
                "path": path,
                "actions": actions,
                "visible": row.get("onlineStatus", 1) != 3,
            }
            await redis.set(key, json.dumps(value), ex=90)
        else:
            await redis.delete(key)
        await self.mirror(state["uid"], ndc, path)

    async def mirror(self, uid, ndc, path):
        redis = get_redis()
        active = []
        async for key in redis.scan_iter(match=f"x{ndc}:activity:{uid}:*", count=100):
            raw = await redis.get(key)
            if raw:
                try:
                    active.append(json.loads(raw))
                except (TypeError, ValueError):
                    continue
        visible = [e for e in active if e.get("visible")]
        if visible:
            await redis.set(f"x{ndc}:online:{uid}", "1", ex=90)
        else:
            await redis.delete(f"x{ndc}:online:{uid}")
        segments = path.split("/")
        kind = segments[0] or "root"
        target = segments[1] if len(segments) > 1 else "-"
        key = f"x{ndc}:browsing:{kind}:{target}:{uid}"
        if any(e["path"] == path and "Browsing" in e["actions"] for e in visible):
            await redis.set(key, "1", ex=90)
        else:
            await redis.delete(key)

    async def activity(self, ws, uid, ndc, target, actions, enabled):
        if (
            not isinstance(actions, list)
            or not 1 <= len(actions) <= 8
            or any(not isinstance(a, str) or a not in ACTIONS for a in actions)
        ):
            raise NativeError("Invalid activity actions.", 101)
        path = self.target(ndc, target)
        await self.access.account(uid, ndc)
        state = self.sockets[ws]
        constraints = {
            "Typing": ("chat-thread/",),
            "Recording": ("chat-thread/",),
            "Playing": ("blog/", "quiz/"),
            "Polling": ("blog/", "poll/"),
            "Commenting": ("blog/",),
            "Voting": ("blog/",),
        }
        for action in actions:
            if action in constraints and not path.startswith(constraints[action]):
                raise NativeError("Activity action does not match its target.", 101)
        if (
            "Chatting" in actions
            and path != "public-chats"
            and not path.startswith("chat-thread/")
        ):
            raise NativeError("Chatting requires a chat target.", 101)
        if path and "/" in path and not path.startswith("chat-thread/"):
            kind, target_id = path.split("/", 1)
            native_id(target_id)
            collection = (
                "Users"
                if kind == "user-profile"
                else "Blogs"
                if kind in ("blog", "quiz", "poll", "wiki", "item")
                else None
            )
            if collection is None:
                raise NativeError("This activity resource is not implemented.", 109)
            target_row = await (await self.access.collection(ndc, collection)).find_one(
                {"id": target_id, "status": {"$nin": [5, 9, 10]}}, {"id": 1}
            )
            if not target_row:
                raise NativeError("Activity resource is unavailable.", 109)
        if path.startswith("chat-thread/"):
            tid = path.split("/")[1]
            await self.access.room(uid, ndc, tid)
            from handlers.chat import (
                on_chat_message_typing,
                on_chat_message_typing_end,
                on_chat_voice_recording,
                on_chat_voice_recording_end,
                on_chatting,
                on_chatting_end,
            )

            pairs = {
                "Typing": (on_chat_message_typing, on_chat_message_typing_end),
                "Recording": (on_chat_voice_recording, on_chat_voice_recording_end),
                "Chatting": (on_chatting, on_chatting_end),
            }
            for action in set(actions) & pairs.keys():
                await pairs[action][0 if enabled else 1](uid, tid, ndc, self.manager)
        nontransient = sorted(set(actions) - {"Typing", "Recording"})
        # Track actions separately: start [Browsing, Commenting] followed by
        # end [Commenting] must leave Browsing active, not retain a stale group.
        additions = {(ndc, path, (a,)) for a in nontransient} - state[
            "activities"
        ].keys()
        if enabled and len(state["activities"]) + len(additions) > 32:
            raise NativeError("Too many activity leases.", 109)
        for action in nontransient:
            entry = {"ndc": ndc, "path": path, "actions": [action]}
            key = (ndc, path, (action,))
            if enabled:
                state["activities"][key] = entry
            else:
                state["activities"].pop(key, None)
            await self.write(state, entry, enabled)

    async def heartbeat(self, ws):
        state = self.sockets.get(ws)
        if not state:
            return
        for entry in list(state["activities"].values()):
            try:
                await self.write(state, entry, True)
                if "Chatting" in entry["actions"] and entry["path"].startswith(
                    "chat-thread/"
                ):
                    from handlers.chat import _refresh_chatting

                    await _refresh_chatting(
                        state["uid"], entry["path"].split("/")[1], entry["ndc"]
                    )
            except NativeError:
                state["activities"].pop(
                    (entry["ndc"], entry["path"], tuple(entry["actions"])), None
                )
        redis = get_redis()
        for ndc, tid in list(state["rooms"]):
            try:
                await self.access.room(state["uid"], ndc, tid)
            except NativeError:
                state["rooms"].discard((ndc, tid))
                continue
            await redis.zadd(
                f"ws:thread-subs:{ndc}:{tid}",
                {state["uid"] + ":" + state["id"]: time.time() + 90},
            )
            await redis.expire(f"ws:thread-subs:{ndc}:{tid}", 120)
            from handlers.chat import _redis_set_add_with_ttl

            await _redis_set_add_with_ttl(
                f"x{ndc}:chat:{tid}:viewers", state["uid"], 40
            )

    async def room_subscription(self, ws, ndc, tid, enabled):
        state = self.sockets.get(ws)
        if state is None:
            return
        if enabled:
            state["rooms"].add((ndc, tid))
            await self.heartbeat(ws)
        else:
            state["rooms"].discard((ndc, tid))
            await get_redis().zrem(
                f"ws:thread-subs:{ndc}:{tid}", state["uid"] + ":" + state["id"]
            )

    async def snapshot_ids(self, ndc, topic, viewer):
        redis = get_redis()
        uids = set()
        if "typing-at:" in topic or "recording-at:" in topic:
            from handlers.chat import _redis_set_members

            kind = "typing" if "typing" in topic else "recording"
            return set(
                await _redis_set_members(
                    f"x{ndc}:chat:{topic.rsplit(':', 1)[1]}:{kind}"
                )
            )
        async for key in redis.scan_iter(match=f"x{ndc}:activity:*", count=100):
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                e = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not e.get("visible"):
                continue
            path, actions = e["path"], e["actions"]
            match = False
            if "-at:" in topic:
                kind = topic.split("users-browsing-", 1)[1].split("-at:", 1)[0]
                match = "Browsing" in actions and path in (
                    kind + "/" + topic.rsplit(":", 1)[1],
                    ("wiki/" if kind == "item" else kind + "/")
                    + topic.rsplit(":", 1)[1],
                )
            elif topic == "users-browsing-pages":
                match = "Browsing" in actions and not path.startswith(
                    (
                        "blog/",
                        "quiz/",
                        "poll/",
                        "item/",
                        "wiki/",
                        "user-profile/",
                        "chat-thread/",
                        "public-chats",
                    )
                )
            elif topic == "users-browsing-blogs":
                match = "Browsing" in actions and path.startswith("blog/")
            elif topic in ("users-browsing-items", "users-browsing-item"):
                match = "Browsing" in actions and path.startswith(("wiki/", "item/"))
            elif topic == "users-browsing-user-profiles":
                match = "Browsing" in actions and path.startswith("user-profile/")
            elif topic == "users-playing-quizzes":
                match = path.startswith(("quiz/", "blog/")) and (
                    "Playing" in actions
                    or (path.startswith("quiz/") and "Browsing" in actions)
                )
            elif topic == "users-polling-polls":
                match = path.startswith(("poll/", "blog/")) and (
                    "Polling" in actions
                    or (path.startswith("poll/") and "Browsing" in actions)
                )
            elif topic == "users-commenting-blogs":
                match = "Commenting" in actions and path.startswith("blog/")
            elif topic == "users-voting-blogs":
                match = "Voting" in actions and path.startswith("blog/")
            elif topic == "users-chatting" and path == "public-chats":
                match = bool(set(actions) & {"Chatting", "Browsing"})
            elif (
                topic in ("users-chatting", "users-chatting-private")
                and bool(set(actions) & {"Chatting", "Browsing"})
                and path.startswith("chat-thread/")
            ):
                tid = path.split("/")[1]
                room = (
                    await (await self.access.collection(ndc, "Chats")).find_one(
                        {"id": tid}, {"chatType": 1, "memberList": 1}
                    )
                    or {}
                )
                match = (room.get("chatType") == 2) == (topic == "users-chatting")
                if topic == "users-chatting-private" and viewer not in room.get(
                    "memberList", []
                ):
                    match = False
            if match:
                uids.add(e["uid"])
        return uids

    async def send_snapshot(self, ws, state, ndc, wire, topic):
        await self.access.account(state["uid"], ndc)
        if "typing-at:" in topic or "recording-at:" in topic:
            await self.access.room(state["uid"], ndc, topic.rsplit(":", 1)[1])
        ids = await self.snapshot_ids(ndc, topic, state["uid"])
        previous = state["subscriptions"].get((ndc, wire))
        marker = tuple(sorted(ids))
        if previous == marker:
            return
        if previous is not None:
            transient = "typing-at:" in topic or "recording-at:" in topic
            groups = [(400, ids - set(previous)), (401, set(previous) - ids)]
            if transient:
                groups = [
                    (
                        400,
                        set(previous) - ids
                        if "users-end-" in topic
                        else ids - set(previous),
                    )
                ]
            for code, changed in groups:
                if not changed:
                    continue
                profiles = []
                for uid in sorted(changed)[:100]:
                    profile = await self.access.profile(uid, ndc, state["uid"])
                    if profile:
                        profiles.append(
                            dict(
                                profile,
                                onlineStatus=0
                                if code == 401
                                else profile.get("onlineStatus", 1),
                            )
                        )
                    elif code == 401 or "users-end-" in topic:
                        profiles.append({"uid": uid, "ndcId": ndc, "onlineStatus": 0})
                await self.manager.answer(
                    {
                        "t": code,
                        "o": {
                            "ndcId": ndc,
                            "topic": wire,
                            "userProfileCount": len(ids),
                            "userProfileList": profiles,
                        },
                    },
                    ws,
                )
        state["subscriptions"][(ndc, wire)] = marker

    async def poll(self):
        for ws, state in list(self.sockets.items()):
            for ndc, wire in list(state["subscriptions"]):
                try:
                    topic, _ = self.topic(ndc, wire)
                    await self.send_snapshot(ws, state, ndc, wire, topic)
                except NativeError:
                    state["subscriptions"].pop((ndc, wire), None)
