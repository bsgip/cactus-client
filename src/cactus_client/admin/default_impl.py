from . import hookimpl

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution, ActionResult

from typing import Any


class DefaultPlugin:
    @hookimpl(trylast=True)
    async def admin_setup(self, step: StepExecution, context: ExecutionContext) -> ActionResult:
        """General setup function to be run at the beginning of all test runs."""
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_teardown(self, step: StepExecution, context: ExecutionContext) -> ActionResult:
        """General teardown function to be run at the end of all test runs."""
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_device_register(
        self, resolved_params: dict[str, Any], step: StepExecution, context: ExecutionContext
    ) -> ActionResult:
        """Out-of-band register a device."""
        return ActionResult.done()
