from datetime import datetime

from cactus_client.model.context import ExecutionContext
from cactus_client.time import relative_time


def context_relative_time(context: ExecutionContext, dt: datetime) -> str:
    """Returns the time relative to context as a human readable string"""
    return relative_time(dt - context.created_at)
