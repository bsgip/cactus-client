import unittest.mock as mock

import pytest
from aiohttp import ClientSession

from cactus_client import admin
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution, ActionResult

from typing import Callable


@pytest.mark.asyncio
async def test_admin_device_register(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Ensure that the default implementation of the admin_device_register hook is seen"""
    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())

    pm = admin.manager.get_plugin_manager()

    # Execute
    result = await pm.ahook.admin_device_register(resolved_params={}, step=step, context=context)

    # Assert
    assert len(result) == 1
    assert result[0] == ActionResult(completed=True, repeat=False, not_before=None)


@pytest.mark.asyncio
async def test_admin_setup(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Ensure that the default implementation of teh admin_setup hook is seen"""
    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())

    pm = admin.manager.get_plugin_manager()

    # Execute
    result = await pm.ahook.admin_setup(step=step, context=context)

    # Assert
    assert len(result) == 1
    assert result[0] == ActionResult(completed=True, repeat=False, not_before=None)


@pytest.mark.asyncio
async def test_admin_teardown(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
) -> None:
    """Ensure that the default implementation of the admin_teardown hook is seen"""
    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())

    pm = admin.manager.get_plugin_manager()

    # Execute
    result = await pm.ahook.admin_teardown(step=step, context=context)

    # Assert
    assert len(result) == 1
    assert result[0] == ActionResult(completed=True, repeat=False, not_before=None)
