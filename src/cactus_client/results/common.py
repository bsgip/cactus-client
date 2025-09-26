from datetime import datetime

from cactus_client.model.context import ExecutionContext


def relative_time(context: ExecutionContext, dt: datetime) -> str:
    """Returns the time relative to context as a human readable string"""
    delta = dt - context.created_at

    total_seconds = delta.total_seconds()
    if total_seconds >= 0:
        sign = "+"
    else:
        sign = "-"

    magnitude = abs(total_seconds)
    if magnitude < 5:
        return f"{sign}{int(magnitude*1000)}ms"
    elif magnitude < 120:
        return f"{sign}{int(magnitude)}s"
    else:
        return f"{sign}{int(magnitude) // 60}m{int(magnitude) % 60}s"
