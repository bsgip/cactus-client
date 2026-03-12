from . import hookspec

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution, ActionResult

from typing import Any


class Spec:
    @hookspec
    def admin_setup(self, step: StepExecution, context: ExecutionContext) -> ActionResult:  # type: ignore[empty-body]
        """General setup function to be run at the beginning of all test runs."""

    @hookspec
    def admin_teardown(self, step: StepExecution, context: ExecutionContext) -> ActionResult:  # type: ignore[empty-body]
        """General teardown function to be run at the end of all test runs."""

    @hookspec
    async def admin_device_register(  # type: ignore[empty-body]
        self, resolved_params: dict[str, Any], step: StepExecution, context: ExecutionContext
    ) -> ActionResult:
        """Out-of-band register a device."""
