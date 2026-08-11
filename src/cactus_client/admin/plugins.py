import apluggy
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client.model.context import AdminContext
from cactus_client.model.execution import ActionResult, StepExecution

project_name = "cactus_client.admin"
hookspec = apluggy.HookspecMarker(project_name)
hookimpl = apluggy.HookimplMarker(project_name)


class AdminSpec:
    """Base interface for admin plugins. Implement any subset of these hooks in your plugin class."""

    @hookspec
    def admin_setup(self, context: AdminContext) -> ActionResult:  # type: ignore
        """Called once before any test steps execute.

        Use this to perform any setup required before the test begins (e.g. registering end devices,
        configuring DER controls). Return ActionResult.done() on success or ActionResult.failed(reason)
        to abort the test before it starts.

        context: Full execution context for this test run (server config, client configs, etc.)
        """

    @hookspec
    def admin_teardown(self, context: AdminContext) -> ActionResult:  # type: ignore
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

        Exceptions raised will abort test execution as a failure.

        There are three ways to respond:
          ActionResult.done()          The instruction was handled - the step proceeds normally.
          ActionResult.skip_step(why)  The step's action still runs (so downstream steps
                                       keep their resources) but its pass/fail judgement is waived.
                                       Honoured ONLY when the run enables skips (--allow-skips /
                                       runner.allow_skips) - otherwise it is a normal step failure.

        NOTE: Skipping a precondition step will still fail any steps that depend on the missing state.

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
