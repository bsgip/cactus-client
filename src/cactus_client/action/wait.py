import asyncio
import logging
from typing import Any


from cactus_client.model.execution import ActionResult

logger = logging.getLogger(__name__)


async def action_wait(resolved_parameters: dict[str, Any]) -> ActionResult:
    """Asyncio wait for the requested time period."""

    wait_seconds: int = int(resolved_parameters["wait_seconds"])  # mandatory param
    logger.debug(f"Requested wait for {wait_seconds} seconds...")
    await asyncio.sleep(wait_seconds)
    return ActionResult.done()
