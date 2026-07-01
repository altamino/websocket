import time
import zlib

from agora_token_builder import RtcTokenBuilder

ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2


def uid_from_uuid(user_uuid: str) -> int:
    return zlib.crc32(user_uuid.encode()) & 0xFFFFFFFF


def build_rtc_token(
    app_id: str,
    app_certificate: str,
    channel_name: str,
    uid: int,
    role: int,
    expire_seconds: int = 3600,
) -> str:
    expire_ts = int(time.time()) + expire_seconds
    return RtcTokenBuilder.buildTokenWithUid(
        app_id, app_certificate, channel_name, uid, role, expire_ts
    )
