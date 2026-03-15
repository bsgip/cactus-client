import apluggy
from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_test_definitions.server.test_procedures import AdminInstruction

project_name = "cactus_client.admin"
hookspec = apluggy.HookspecMarker(project_name)
hookimpl = apluggy.HookimplMarker(project_name)


class AdminSpec:
    """Base interface for admin plugins. Implement any subset of these hooks in your plugin class."""

    @hookspec
    def admin_setup(self, context: AdminContext) -> ActionResult:  # type: ignore[empty-body]
        """Called once before any test steps execute.

        Use this to perform any setup required before the test begins (e.g. registering end devices,
        configuring DER controls). Return ActionResult.done() on success or ActionResult.failed(reason)
        to abort the test before it starts.

        context: Full execution context for this test run (server config, client configs, etc.)
        """

    @hookspec
    def admin_teardown(self, context: AdminContext) -> ActionResult:  # type: ignore[empty-body]
        """Called once after all test steps complete (or on failure). Always runs, even if setup failed.

        Use this to clean up any state created during setup or the test run. Exceptions raised here
        are caught and logged — they will not mask the test result.

        context: Full execution context for this test run (server config, client configs, etc.)
        """

    @hookspec
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: AdminContext
    ) -> ActionResult | None:
        """Called once per admin instruction before the first attempt of a step.

        Exceptions raised will abort test execution as a failure. Return None if this plugin
        does not handle the given instruction type — the framework will log a warning if no
        plugin handles it.

        instruction: The admin instruction to handle (type + parameters)
        step: The step that owns this instruction
        context: Full execution context for this test run
        """


class DefaultAdminPlugin:
    """Default implementation. Registered last (trylast) so provider plugins run first."""

    @hookimpl(trylast=True)
    async def admin_setup(self, context: AdminContext) -> ActionResult:
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_teardown(self, context: AdminContext) -> ActionResult:
        return ActionResult.done()

    @hookimpl(trylast=True)
    async def admin_instruction(
        self, instruction: AdminInstruction, step: StepExecution, context: AdminContext
    ) -> ActionResult | None:
        return None
