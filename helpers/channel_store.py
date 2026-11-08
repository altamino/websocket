"""Shared signalling state with real participant leases; Agora remains the provider."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from helpers.native_access import NativeError

LEASE = timedelta(seconds=90)


class ChannelStore:
    def __init__(self, access, manager):
        self.access = access
        self.manager = manager

    @staticmethod
    def key(ndc, tid):
        return f"{ndc}:{tid}"

    async def collection(self):
        return await self.access.global_collection("WsChannelState")

    async def waiting(self):
        return await self.access.global_collection("WsVoiceWaiting")

    @asynccontextmanager
    async def locked(self, ndc, tid, uid=None):
        locks = await self.access.global_collection("WsChannelLocks")
        owner = uuid4().hex
        key = self.key(ndc, tid)
        for attempt in range(20):
            now = datetime.now(timezone.utc)
            try:
                lock = await locks.find_one_and_update(
                    {
                        "_id": key,
                        "$or": [{"expiresAt": {"$lte": now}}, {"owner": owner}],
                    },
                    {
                        "$set": {
                            "owner": owner,
                            "expiresAt": now + timedelta(seconds=15),
                        }
                    },
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                )
                if lock:
                    break
            except DuplicateKeyError:
                pass
            await asyncio.sleep(0.05)
        else:
            raise NativeError("Channel is busy; retry.", 111)

        async def renew():
            while True:
                await asyncio.sleep(4)
                result = await locks.update_one(
                    {"_id": key, "owner": owner},
                    {
                        "$set": {
                            "expiresAt": datetime.now(timezone.utc)
                            + timedelta(seconds=15)
                        }
                    },
                )
                if not result.matched_count:
                    raise NativeError("Channel lock expired.", 111)

        task = asyncio.create_task(renew())
        try:
            state = await (await self.collection()).find_one({"_id": key}) or {}
            now = datetime.now(timezone.utc)
            room = (
                await (await self.access.collection(ndc, "Chats")).find_one(
                    {"id": tid}, {"memberList": 1, "bannedUids": 1, "channelType": 1}
                )
                or {}
            )
            candidates = [m["uid"] for m in state.get("members", [])]
            active = {
                r["id"]
                async for r in (await self.access.collection(ndc, "Users")).find(
                    {"id": {"$in": candidates}, "status": {"$nin": [5, 9, 10]}},
                    {"id": 1},
                )
            }
            active &= {
                r["id"]
                async for r in (await self.access.global_collection("Users")).find(
                    {"id": {"$in": candidates}, "status": {"$nin": [5, 9, 10]}},
                    {"id": 1},
                )
            }
            active &= set(room.get("memberList", [])) - set(room.get("bannedUids", []))
            members = []
            for entry in state.get("members", []):
                seen = entry.get("lastSeen")
                if (
                    entry["uid"] in active
                    and seen
                    and seen.replace(tzinfo=seen.tzinfo or timezone.utc) > now - LEASE
                ):
                    members.append(dict(entry))
            self.manager.channel_members[tid] = members
            self.manager.thread_ndc_ids[tid] = ndc
            channel = state.get("channelType")
            if channel is None:
                room = (
                    await (await self.access.collection(ndc, "Chats")).find_one(
                        {"id": tid}, {"channelType": 1}
                    )
                    or {}
                )
                channel = room.get("channelType", 0)
            self.manager.set_channel_type(tid, channel)
            self.manager.user_busy_threads = {
                u: t for u, t in self.manager.user_busy_threads.items() if t != tid
            }
            for entry in members:
                if entry.get("joinRole") == 1:
                    self.manager.user_busy_threads[entry["uid"]] = tid
            yield state
            if task.done() and not task.cancelled() and task.exception():
                raise task.exception()
            if not await locks.find_one({"_id": key, "owner": owner}):
                raise NativeError("Channel lock expired.", 111)
            current = self.manager.get_channel_members(tid)
            previous = {e["uid"]: e for e in members}
            for entry in current:
                if entry["uid"] == uid or entry["uid"] not in previous:
                    entry["lastSeen"] = now
                else:
                    entry["lastSeen"] = previous[entry["uid"]].get("lastSeen", now)
            channel = self.manager.get_channel_type(tid)
            doc = {
                "_id": key,
                "ndcId": ndc,
                "threadId": tid,
                "members": current,
                "channelType": channel,
                "playlist": state.get("playlist", {"items": []})
                if state.get("channelType") and channel
                else {"items": []},
                "startedAt": state.get("startedAt", now)
                if state.get("channelType")
                else now,
                "updatedAt": now,
                "expiresAt": now + timedelta(days=1),
                "version": state.get("version", 0) + 1,
            }
            col = await self.collection()
            try:
                result = await col.replace_one(
                    {"_id": key, "version": state.get("version", 0)},
                    doc,
                    upsert=not bool(state),
                )
            except DuplicateKeyError:
                raise NativeError("Concurrent channel update; retry.", 111)
            if state and not result.matched_count:
                raise NativeError("Concurrent channel update; retry.", 111)
            if not channel:
                await (await self.waiting()).delete_many(
                    {"ndcId": ndc, "threadId": tid}
                )
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await locks.delete_one({"_id": key, "owner": owner})

    async def heartbeat(self, uid):
        now = datetime.now(timezone.utc)
        col = await self.collection()
        result = []
        async for state in col.find(
            {
                "members": {
                    "$elemMatch": {"uid": uid, "lastSeen": {"$gt": now - LEASE}}
                },
                "channelType": {"$ne": 0},
            }
        ):
            async with self.locked(state["ndcId"], state["threadId"], uid):
                try:
                    await self.access.room(uid, state["ndcId"], state["threadId"])
                except NativeError:
                    self.manager.remove_channel_member(state["threadId"], uid)
                    continue
                for member in self.manager.get_channel_members(state["threadId"]):
                    if member["uid"] == uid:
                        result.append(
                            {
                                "ndcId": state["ndcId"],
                                "threadId": state["threadId"],
                                "uid": uid,
                                "joinRole": member["joinRole"],
                            }
                        )
        return result
