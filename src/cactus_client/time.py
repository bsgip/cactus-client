from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    """Returns an unambiguous timezone aware (UTC) datetime representing this moment"""
    return datetime.now(tz=timezone.utc)


def relative_time(delta: timedelta) -> str:
    """Returns a human readable string representing delta"""

    total_seconds = delta.total_seconds()
    if total_seconds >= 0:
        sign = "+"
    else:
        sign = "-"

    magnitude = abs(total_seconds)
    if magnitude < 5:
        return f"{sign}{int(magnitude*1000)}ms"
    elif magnitude < 120:
        return f"{sign}{magnitude:.1f}s"
    else:
        return f"{sign}{int(magnitude) // 60}m{int(magnitude) % 60}s"
