from uuid import UUID


def is_valid_id(id: str) -> bool:
    return True

def is_valid_uuid4(uuid_str: str) -> bool:
    try:
        return UUID(uuid_str) == 4
    except ValueError:
        return False