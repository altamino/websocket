"""Native signalling ACLs. Identity always comes from the authenticated SID."""

from uuid import UUID
from helpers.database.mongo import Database


class NativeError(Exception):
    def __init__(self, message, code=110):
        super().__init__(message)
        self.code = code


def native_id(value):
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise NativeError("Invalid object identifier.", 101)


class NativeAccess:
    def __init__(self):
        self.db = None

    async def store(self):
        if self.db is None:
            self.db = await Database().init()
        return self.db

    async def close(self):
        if self.db:
            await self.db.close()

    async def collection(self, ndc, name):
        return await (await self.store()).get(f"x{ndc}", name)

    async def global_collection(self, name):
        return await (await self.store()).get(table=name)

    async def account(self, uid, ndc):
        if (
            not isinstance(ndc, int)
            or isinstance(ndc, bool)
            or not 0 <= ndc <= 2147483647
        ):
            raise NativeError("Invalid community.", 101)
        global_row = await (await self.global_collection("Users")).find_one(
            {"id": uid}, {"status": 1}
        )
        row = await (await self.collection(ndc, "Users")).find_one({"id": uid})
        if (
            not global_row
            or global_row.get("status", 0) in (5, 9, 10)
            or not row
            or row.get("status", 0) in (5, 9, 10)
        ):
            raise NativeError("Active community membership required.")
        return row

    async def room(self, uid, ndc, tid, organizer=False):
        await self.account(uid, ndc)
        native_id(tid)
        room = await (await self.collection(ndc, "Chats")).find_one({"id": tid})
        if (
            not room
            or uid not in room.get("memberList", [])
            or uid in room.get("bannedUids", [])
        ):
            raise NativeError("Active chat membership required.")
        if organizer and not self.organizer(room, uid):
            raise NativeError("Host or co-host permission required.")
        return room

    @staticmethod
    def organizer(room, uid):
        return (
            uid == room.get("hostId")
            or uid in room.get("cohostsIds", [])
            or (room.get("chatType") == 0 and uid in room.get("memberList", []))
        )

    async def profile(self, uid, ndc, viewer):
        from objects.user import User

        try:
            row = await self.account(uid, ndc)
        except NativeError:
            return None
        reader = (
            await (await self.collection(ndc, "Users")).find_one(
                {"id": viewer}, {"following": 1}
            )
            or {}
        )
        profile = User.OwnNonSensetiveProfile(row, ndcId=ndc)
        bits = (
            0
            if uid == viewer
            else int(uid in (reader.get("following") or []))
            + 2 * int(viewer in (row.get("following") or []))
        )
        return dict(profile, followingStatus=bits, membershipStatus=bits)
