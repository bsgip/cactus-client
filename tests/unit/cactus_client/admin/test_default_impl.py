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
    context, step = testing_contexts_factory(mock.Mock())

    # Execute
    result = await admin.manager.pm.ahook.admin_device_register({}, step, context)

    # Assert
    assert result == ActionResult(completed=True, repeat=False, not_before=None)
