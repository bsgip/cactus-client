import unittest.mock as mock
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlResponse
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceResponse,
)

from cactus_client.action.forget import action_forget
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution


@pytest.mark.asyncio
async def test_action_forget(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """basic integration-esque check that action_forget calls through to the appropriate resource store"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())

    # Build up the store with some values (we aren't going deep here as the unit tests on the ResourceStore)
    store = context.discovered_resources(step)
    sr1 = store.append_resource(
        CSIPAusResource.DeviceCapability, None, generate_class_instance(DeviceCapabilityResponse, seed=101)
    )
    sr2 = store.append_resource(CSIPAusResource.EndDevice, sr1.id, generate_class_instance(EndDeviceResponse, seed=202))
    sr3 = store.append_resource(
        CSIPAusResource.DERControl, sr2.id, generate_class_instance(DERControlResponse, seed=303)
    )
    sr4 = store.append_resource(
        CSIPAusResource.DERControl, sr2.id, generate_class_instance(DERControlResponse, seed=404)
    )

    resources = [CSIPAusResource.DeviceCapability, "DERControl"]
    resolved_params = {"resources": resources, "next_polling_window": True}

    # Act
    result = await action_forget(resolved_params, step, context)

    # Assert
    assert result.done()
    assert store.get_for_id(sr1.id) is None
    assert store.get_for_id(sr2.id) is not None
    assert store.get_for_id(sr3.id) is None
    assert store.get_for_id(sr4.id) is None
