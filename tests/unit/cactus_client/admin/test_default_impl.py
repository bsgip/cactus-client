import unittest.mock as mock
from typing import Callable

import apluggy
import pytest
from aiohttp import ClientSession
from cactus_test_definitions.server.test_procedures import AdminInstruction

from cactus_client.admin.plugins import AdminSpec, DefaultAdminPlugin, hookimpl, project_name
from cactus_client.model.context import AdminContext, ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution


def make_plugin_manager() -> apluggy.PluginManager:
    """Creates a fresh (uncached) plugin manager with only the default plugin registered."""
    pm = apluggy.PluginManager(project_name)
    pm.add_hookspecs(AdminSpec)
    pm.register(DefaultAdminPlugin())
    return pm


@pytest.mark.asyncio
async def test_admin_setup_default(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Default admin_setup returns a successful ActionResult."""
    context, _ = testing_contexts_factory(mock.Mock())
    pm = make_plugin_manager()

    results = await pm.ahook.admin_setup(context=context.to_admin_context())

    assert len(results) == 1
    assert results[0] == ActionResult.done()


@pytest.mark.asyncio
async def test_admin_teardown_default(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Default admin_teardown returns a successful ActionResult."""
    context, _ = testing_contexts_factory(mock.Mock())
    pm = make_plugin_manager()

    results = await pm.ahook.admin_teardown(context=context.to_admin_context())

    assert len(results) == 1
    assert results[0] == ActionResult.done()


@pytest.mark.asyncio
async def test_admin_instruction_default_returns_none(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Default admin_instruction returns None — no instruction types are handled without a provider plugin."""
    context, step = testing_contexts_factory(mock.Mock())
    pm = make_plugin_manager()
    instr = AdminInstruction(type="ensure-end-device", parameters={"registered": True})

    results = await pm.ahook.admin_instruction(instruction=instr, step=step, context=context.to_admin_context())

    assert len(results) == 1
    assert results[0] is None


@pytest.mark.asyncio
async def test_provider_plugin_runs_before_default(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """A provider plugin registered after the default runs first (LIFO), and its result is at index 0."""
    context, _ = testing_contexts_factory(mock.Mock())

    class ProviderPlugin:
        @hookimpl
        async def admin_setup(self, context: AdminContext) -> ActionResult:
            return ActionResult.failed("provider ran")

    pm = make_plugin_manager()
    pm.register(ProviderPlugin())

    results = await pm.ahook.admin_setup(context=context.to_admin_context())

    assert len(results) == 2
    assert results[0] == ActionResult.failed("provider ran")  # provider first
    assert results[1] == ActionResult.done()  # default last (trylast)


@pytest.mark.asyncio
async def test_provider_plugin_handles_instruction(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """A provider plugin can handle specific instruction types and return None for others."""
    context, step = testing_contexts_factory(mock.Mock())

    class ProviderPlugin:
        @hookimpl
        async def admin_instruction(
            self, instruction: AdminInstruction, step: StepExecution, context: AdminContext
        ) -> ActionResult | None:
            if instruction.type == "ensure-end-device":
                return ActionResult.done()
            return None

    pm = make_plugin_manager()
    pm.register(ProviderPlugin())

    handled = AdminInstruction(type="ensure-end-device", parameters={})
    unhandled = AdminInstruction(type="set-poll-rate", parameters={})

    admin_context = context.to_admin_context()
    handled_results = await pm.ahook.admin_instruction(instruction=handled, step=step, context=admin_context)
    unhandled_results = await pm.ahook.admin_instruction(instruction=unhandled, step=step, context=admin_context)

    assert handled_results[0] == ActionResult.done()  # provider handled it
    assert unhandled_results[0] is None  # provider returned None, default returned None
