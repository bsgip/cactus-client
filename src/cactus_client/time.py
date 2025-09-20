from datetime import datetime, timezone


def utc_now() -> datetime:
    """Returns an unambiguous timezone aware (UTC) datetime representing this moment"""
    return datetime.now(tz=timezone.utc)
