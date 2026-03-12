import apluggy
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_test_definitions.server.test_procedures import AdminInstruction

project_name = "cactus_client.admin"
hookspec = apluggy.HookspecMarker(project_name)
hookimpl = apluggy.HookimplMarker(project_name)


class AdminSpec:
    # --- Per-test lifecycle hooks ---
    # Called once per test run regardless of how many steps or instructions exist.

    @hookspec
    def admin_setup(self, context: ExecutionContext) -> ActionResult:  # type: ignore[empty-body]
        """Called once before any test steps execute."""

    @hookspec
    def admin_teardown(self, context: ExecutionContext) -> ActionResult:  # type: ignore[empty-body]
        """Called once after all test steps complete (or on failure)."""

    # --- Per-instruction hook ---
    # Called once per AdminInstruction before the first attempt of a step.

    @hookspec
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: ExecutionContext
    ) -> ActionResult | None:
        """Handle a single admin instruction.

        Return None if this instruction type is not handled by this plugin.
        """


class DefaultAdminPlugin:
    @hookimpl(trylast=True)
    async def admin_setup(self, context: ExecutionContext) -> ActionResult:
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_teardown(self, context: ExecutionContext) -> ActionResult:
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: ExecutionContext
    ) -> ActionResult | None:
        return None
