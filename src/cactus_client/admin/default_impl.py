from . import hookimpl

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution, ActionResult

from typing import Any


@hookimpl(trylast=True)
async def admin_setup(step: StepExecution, context: ExecutionContext) -> ActionResult:
    """General setup function to be run at the beginning of all test runs."""
    return ActionResult(completed=True, repeat=False, not_before=None)


@hookimpl(trylast=True)
async def admin_teardown(step: StepExecution, context: ExecutionContext) -> ActionResult:
    """General teardown function to be run at the end of all test runs."""
    return ActionResult(completed=True, repeat=False, not_before=None)


@hookimpl(trylast=True)
async def admin_device_register(
    resolved_params: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Out-of-band register a device."""
    return ActionResult(completed=True, repeat=False, not_before=None)
