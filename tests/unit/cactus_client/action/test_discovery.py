from datetime import datetime, timezone
import unittest.mock as mock
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse

from cactus_client.action.discovery import discover_resource, calculate_wait_next_polling_window, action_discovery
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.resource import RESOURCE_SEP2_TYPES


@pytest.mark.parametrize("has_href", [True, False])
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_dcap(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    has_href: bool,
):
    """DeviceCapability is a special discovery case - it can go direct to the device capability URI"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    dcap = generate_class_instance(DeviceCapabilityResponse, href="/my/dcap/href" if has_href else "")
    mock_get_resource_for_step.return_value = dcap

    # Act
    await discover_resource(CSIPAusResource.DeviceCapability, step, context)

    # Assert
    stored_resources = context.discovered_resources(step).get(CSIPAusResource.DeviceCapability)
    assert len(stored_resources) == 1
    assert stored_resources[0].resource is dcap
    assert stored_resources[0].resource_type == CSIPAusResource.DeviceCapability
    assert stored_resources[0].parent is None
    mock_get_resource_for_step.assert_called_once_with(DeviceCapabilityResponse, step, context, context.dcap_path)
    mock_paginate_list_resource_items.assert_not_called()

    if has_href:
        assert len(context.warnings.warnings) == 0
    else:
        assert len(context.warnings.warnings) == 1


@pytest.mark.parametrize(
    "resource, matched_parents, has_href, expect_warnings",
    [
        (CSIPAusResource.DERList, 1, True, False),
        (CSIPAusResource.DERList, 1, False, True),
        (CSIPAusResource.EndDeviceList, 2, True, False),
        (CSIPAusResource.EndDeviceList, 2, False, True),
        (CSIPAusResource.FunctionSetAssignmentsList, 2, True, False),
        (CSIPAusResource.FunctionSetAssignmentsList, 2, False, True),
        (CSIPAusResource.SubscriptionList, 0, True, False),
        (CSIPAusResource.SubscriptionList, 0, False, False),  # No warnings as there are no items in the list
    ],
)
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_singular(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    resource: CSIPAusResource,
    matched_parents: int,
    has_href: bool,
    expect_warnings: bool,
):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Load the resource store with some pseudo parent records (including a dud record with no href)
    parent_resource = context.resource_tree.parent_resource(resource)
    stored_parent_resources = [
        resource_store.append_resource(
            parent_resource,
            None,
            generate_class_instance(RESOURCE_SEP2_TYPES[parent_resource], generate_relationships=True, seed=idx),
        )
        for idx in range(matched_parents)
    ]
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(
            RESOURCE_SEP2_TYPES[parent_resource], seed=1001, generate_relationships=True, optional_is_none=True
        ),
    )  # This will never be queried due to a missing href
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(RESOURCE_SEP2_TYPES[parent_resource], seed=1001, href=""),
    )  # This will never be queried due to a empty href

    # Prep the returned resources
    expected_resource_type = RESOURCE_SEP2_TYPES[resource]
    fetched_resources = [
        generate_class_instance(expected_resource_type, seed=idx * 101, optional_is_none=not has_href)
        for idx in range(matched_parents)
    ]
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    await discover_resource(resource, step, context)

    # Assert
    added_resources = resource_store.get(resource)
    assert [sr.resource for sr in added_resources] == fetched_resources, "Store the results from get_resource_for_step"
    assert all([sr.resource_type == resource for sr in added_resources])
    assert all([added_sr.parent is parent_sr for added_sr, parent_sr in zip(added_resources, stored_parent_resources)])

    mock_get_resource_for_step.assert_has_calls(
        mock.call(expected_resource_type, step, context, spr.resource_link_hrefs[resource])
        for spr in stored_parent_resources
    )
    mock_paginate_list_resource_items.assert_not_called()

    if expect_warnings:
        assert len(context.warnings.warnings) > 0
    else:
        assert len(context.warnings.warnings) == 0


@pytest.mark.parametrize(
    "poll_rate,current_seconds,expected_wait",
    [
        # 60-second poll rate (default)
        (60, 0, 60),  # Start of minute
        (60, 30, 30),  # Middle
        (60, 59, 1),  # End
        # 2-minute poll rate
        (120, 0, 120),  # Start
        (120, 60, 60),  # Middle
        # 30-second poll rate
        (30, 15, 15),  # Middle
        # pollRate is None (defult=60)
        (None, 0, 60),  # Start
        (None, 45, 15),  # Middle
    ],
)
def test_calculate_wait_next_polling_window_with_dcap(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    poll_rate: int | None,
    current_seconds: int,
    expected_wait: int,
):

    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    dcap = generate_class_instance(DeviceCapabilityResponse, pollRate=poll_rate, href="/dcap")
    resource_store.append_resource(CSIPAusResource.DeviceCapability, None, dcap)

    now = datetime.fromtimestamp(current_seconds, tz=timezone.utc)

    # Act
    wait = calculate_wait_next_polling_window(now, resource_store)

    assert wait == expected_wait


def test_calculate_wait_next_polling_window_no_dcap(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test no DeviceCapability defaults to 60 seconds"""

    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    now = datetime.fromtimestamp(45, tz=timezone.utc)

    wait = calculate_wait_next_polling_window(now, resource_store)

    assert wait == 15  # 60 - 45


@mock.patch("cactus_client.action.discovery.discover_resource")
@pytest.mark.asyncio
async def test_action_discovery_multiple_resources(
    mock_discover: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test that discovery follows the resource tree plan without polling window (next_polling_window default=False)."""

    context, step = testing_contexts_factory(mock.Mock())

    # Include FunctionSetAssignments that has dependencies
    resources = [CSIPAusResource.EndDevice, CSIPAusResource.FunctionSetAssignments]
    resolved_params = {"resources": resources}

    # Act
    result = await action_discovery(resolved_params, step, context)

    assert result.done()
    # resource tree plan should include parent resources too
    assert mock_discover.call_count > len(resources)


@mock.patch("cactus_client.action.discovery.discover_resource")
@mock.patch("cactus_client.action.discovery.calculate_wait_next_polling_window")
@mock.patch("asyncio.sleep")
@pytest.mark.asyncio
async def test_action_discovery_with_polling_window(
    mock_sleep: mock.MagicMock,
    mock_calc_wait: mock.MagicMock,
    mock_discover: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test discovery action with next_polling_window enabled"""

    context, step = testing_contexts_factory(mock.Mock())
    resources = [CSIPAusResource.DeviceCapability]
    resolved_params = {"resources": resources, "next_polling_window": True}
    mock_calc_wait.return_value = 42

    # Act
    result = await action_discovery(resolved_params, step, context)

    assert result.done()
    mock_calc_wait.assert_called_once()
    mock_sleep.assert_called_once_with(42)  # mock to avoid actually waiting
    assert mock_discover.call_count >= 1
