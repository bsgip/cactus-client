from cactus_client.model.execution import ActionResult


async def action_noop() -> ActionResult:
    """Literally a "no operation" action - used mainly by preconditions"""
    return ActionResult.done()
