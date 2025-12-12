import asyncio
import logging
from typing import Any


from cactus_client.model.execution import ActionResult

logger = logging.getLogger(__name__)


async def action_wait(resolved_parameters: dict[str, Any]) -> ActionResult:
    """Asyncio wait for the requested time period."""

    duration_seconds: int = int(resolved_parameters["duration_seconds"])  # mandatory param
    logger.debug(f"Requested wait for {duration_seconds} seconds...")
    await asyncio.sleep(duration_seconds)
    return ActionResult.done()
