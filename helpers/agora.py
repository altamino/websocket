import time
import zlib

from agora_token_builder import RtcTokenBuilder

ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2
DEFAULT_TOKEN_EXPIRE_SECONDS = 3600


def uid_from_uuid(user_uuid: str) -> int:
    return zlib.crc32(user_uuid.encode()) & 0xFFFFFFFF


def channel_name(ndc_id: int, thread_id: str) -> str:
    return thread_id


def build_rtc_token(
    app_id: str,
    app_certificate: str,
    channel_name_value: str,
    uid: int,
    role: int = ROLE_PUBLISHER,
    expire_seconds: int = DEFAULT_TOKEN_EXPIRE_SECONDS,
) -> str:
    expire_ts = int(time.time()) + expire_seconds
    return RtcTokenBuilder.buildTokenWithUid(
        app_id, app_certificate, channel_name_value, uid, role, expire_ts
    )
