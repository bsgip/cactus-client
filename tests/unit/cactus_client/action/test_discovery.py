import unittest.mock as mock
from datetime import datetime, timezone
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from cactus_client.action.discovery import (
    DISCOVERY_LIST_PAGE_SIZE,
    action_discovery,
    calculate_wait_next_polling_window,
    discover_resource,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.resource import RESOURCE_SEP2_TYPES
from envoy_schema.server.schema.sep2.end_device import EndDeviceResponse, EndDeviceListResponse


def setup_discovery_test(testing_contexts_factory, resource: CSIPAusResource, matched_parents: int):
    """Common setup for discovery tests."""
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    parent_resource = context.resource_tree.parent_resource(resource)
    parent_type = RESOURCE_SEP2_TYPES[parent_resource]

    # Create parent resources with valid hrefs
    stored_parents = [
        resource_store.append_resource(
            parent_resource,
            None,
            generate_class_instance(
                parent_type,
                generate_relationships=True,
                seed=idx,
                href=f"/{parent_resource.value}/{idx}",
            ),
        )
        for idx in range(matched_parents)
    ]

    expected_type = RESOURCE_SEP2_TYPES[resource]

    return context, step, resource_store, stored_parents, expected_type


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
    if has_href:
        await discover_resource(CSIPAusResource.DeviceCapability, step, context, None)
    else:
        with pytest.raises(CactusClientException):
            await discover_resource(CSIPAusResource.DeviceCapability, step, context, None)

    # Assert
    stored_resources = context.discovered_resources(step).get_for_type(CSIPAusResource.DeviceCapability)
    mock_get_resource_for_step.assert_called_once_with(DeviceCapabilityResponse, step, context, context.dcap_path)
    mock_paginate_list_resource_items.assert_not_called()

    if has_href:
        assert len(context.warnings.warnings) == 0
        assert len(stored_resources) == 1
        assert stored_resources[0].resource is dcap
        assert stored_resources[0].resource_type == CSIPAusResource.DeviceCapability
        assert stored_resources[0].id.parent_id() is None
    else:
        assert len(context.warnings.warnings) == 1
        assert len(stored_resources) == 0


@pytest.mark.parametrize(
    "resource, matched_parents, has_href, expect_warnings",
    [
        (CSIPAusResource.DERList, 1, True, False),
        (CSIPAusResource.DERList, 1, False, True),
        (CSIPAusResource.EndDeviceList, 2, True, False),
        (CSIPAusResource.EndDeviceList, 2, False, True),
        (CSIPAusResource.FunctionSetAssignmentsList, 2, True, False),
        (CSIPAusResource.FunctionSetAssignmentsList, 2, False, True),
        (CSIPAusResource.DERProgramList, 2, True, False),
        (CSIPAusResource.DERProgramList, 2, False, True),
        (CSIPAusResource.DERControlList, 1, True, False),
        (CSIPAusResource.DERControlList, 1, False, True),
        (CSIPAusResource.MirrorUsagePointList, 2, True, False),
        (CSIPAusResource.MirrorUsagePointList, 2, False, True),
        (CSIPAusResource.SubscriptionList, 0, True, False),
        (CSIPAusResource.SubscriptionList, 0, False, False),  # No warnings as there are no parents to fetch from
    ],
)
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@pytest.mark.asyncio
async def test_discover_resource_list_containers(
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    resource: CSIPAusResource,
    matched_parents: int,
    has_href: bool,
    expect_warnings: bool,
):
    """
    Discover list containers via parent link.
    e.g. EndDevice.FunctionSetAssignmentsListLink.href - FunctionSetAssignmentsList

    Fetches the LIST CONTAINER itself (not items within). Uses get_resource_for_step, not pagination.
    """
    # Arrange
    context, step, resource_store, stored_parents, expected_type = setup_discovery_test(
        testing_contexts_factory, resource, matched_parents
    )

    fetched_resources = [
        generate_class_instance(
            expected_type,
            seed=idx * 101,
            href=f"/{resource.value}/{idx}" if has_href else None,
        )
        for idx in range(matched_parents)
    ]
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    if has_href or matched_parents == 0:
        await discover_resource(resource, step, context, None)
    else:
        with pytest.raises(CactusClientException):
            await discover_resource(resource, step, context, None)

    # Assert
    added_resources = resource_store.get_for_type(resource)
    if has_href:
        assert [sr.resource for sr in added_resources] == fetched_resources
        assert all(sr.resource_type == resource for sr in added_resources)
        assert all(
            added_sr.id.parent_id() == parent_sr.id for added_sr, parent_sr in zip(added_resources, stored_parents)
        )
    else:
        assert len(added_resources) == 0

    if matched_parents:
        mock_get_resource_for_step.assert_called()
    else:
        mock_get_resource_for_step.assert_not_called()

    assert len(context.warnings.warnings) > 0 if expect_warnings else len(context.warnings.warnings) == 0


@pytest.mark.parametrize(
    "resource, matched_parents, has_href, expect_warnings",
    [
        (CSIPAusResource.Time, 1, True, False),
        (CSIPAusResource.Time, 1, False, True),
        (CSIPAusResource.Registration, 2, True, False),
        (CSIPAusResource.Registration, 2, False, True),
        (CSIPAusResource.ConnectionPoint, 1, True, False),
        (CSIPAusResource.ConnectionPoint, 1, False, True),
        (CSIPAusResource.DERCapability, 1, True, False),
        (CSIPAusResource.DERCapability, 1, False, True),
        (CSIPAusResource.DERSettings, 2, True, False),
        (CSIPAusResource.DERSettings, 2, False, True),
        (CSIPAusResource.DERStatus, 1, True, False),
        (CSIPAusResource.DERStatus, 1, False, True),
        (CSIPAusResource.DefaultDERControl, 1, True, False),
        (CSIPAusResource.DefaultDERControl, 1, False, True),
    ],
)
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_singular_resources(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    resource: CSIPAusResource,
    matched_parents: int,
    has_href: bool,
    expect_warnings: bool,
):
    """
    Discover singular resources via parent link (e.g., EndDevice.RegistrationLink.href → Registration).

    Tests 1-to-1 parent-child relationships. Uses get_resource_for_step, not pagination.
    """
    # Arrange
    context, step, resource_store, stored_parents, expected_type = setup_discovery_test(
        testing_contexts_factory, resource, matched_parents
    )

    fetched_resources = [
        generate_class_instance(
            expected_type, seed=idx * 101, href=None if not has_href else f"/{resource.value}/{idx}"
        )
        for idx in range(matched_parents)
    ]
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    if has_href or matched_parents == 0:
        await discover_resource(resource, step, context, None)
    else:
        with pytest.raises(CactusClientException):
            await discover_resource(resource, step, context, None)

    # Assert
    added_resources = resource_store.get_for_type(resource)
    if has_href:
        assert [sr.resource for sr in added_resources] == fetched_resources
        assert all(sr.resource_type == resource for sr in added_resources)
        assert all(
            added_sr.id.parent_id() == parent_sr.id for added_sr, parent_sr in zip(added_resources, stored_parents)
        )
    else:
        assert len(added_resources) == 0

    mock_get_resource_for_step.assert_called()
    mock_paginate_list_resource_items.assert_not_called()

    assert len(context.warnings.warnings) > 0 if expect_warnings else len(context.warnings.warnings) == 0


@pytest.mark.parametrize(
    "list_resource, child_resource, num_parents, items_per_parent",
    [
        (CSIPAusResource.MirrorUsagePointList, CSIPAusResource.MirrorUsagePoint, 1, [3]),
        (CSIPAusResource.MirrorUsagePointList, CSIPAusResource.MirrorUsagePoint, 2, [3, 2]),
        (CSIPAusResource.EndDeviceList, CSIPAusResource.EndDevice, 1, [2]),
        (CSIPAusResource.EndDeviceList, CSIPAusResource.EndDevice, 2, [3, 2]),
        (CSIPAusResource.DERList, CSIPAusResource.DER, 1, [2]),
        (CSIPAusResource.DERList, CSIPAusResource.DER, 2, [1, 3]),
        (CSIPAusResource.DERProgramList, CSIPAusResource.DERProgram, 1, [1]),
        (CSIPAusResource.DERProgramList, CSIPAusResource.DERProgram, 2, [1, 3]),
        (CSIPAusResource.DERControlList, CSIPAusResource.DERControl, 1, [4]),
        (CSIPAusResource.DERControlList, CSIPAusResource.DERControl, 2, [2, 2]),
        (CSIPAusResource.FunctionSetAssignmentsList, CSIPAusResource.FunctionSetAssignments, 1, [2]),
        (CSIPAusResource.FunctionSetAssignmentsList, CSIPAusResource.FunctionSetAssignments, 2, [2, 2]),
        (CSIPAusResource.SubscriptionList, CSIPAusResource.Subscription, 1, [1]),
        (CSIPAusResource.SubscriptionList, CSIPAusResource.Subscription, 2, [3, 1]),
    ],
)
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_paginated_items(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    list_resource: CSIPAusResource,
    child_resource: CSIPAusResource,
    num_parents: int,
    items_per_parent: list[int],
):
    """
    Discover child items from list containers via pagination (e.g., EndDeviceList to [EndDevice, EndDevice, ...]).

    Uses paginate_list_resource_items, not get_resource_for_step.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    stored_parents = [
        resource_store.append_resource(
            list_resource,
            None,
            generate_class_instance(
                RESOURCE_SEP2_TYPES[list_resource],
                seed=idx,
                href=f"/{list_resource.value}/{idx}",
                generate_relationships=True,
            ),
        )
        for idx in range(num_parents)
    ]

    child_type = RESOURCE_SEP2_TYPES[child_resource]
    child_items_by_parent = [
        [
            generate_class_instance(
                child_type, seed=parent_idx * 100 + child_idx, href=f"/item/{parent_idx}/{child_idx}"
            )
            for child_idx in range(items_per_parent[parent_idx])
        ]
        for parent_idx in range(num_parents)
    ]
    mock_paginate_list_resource_items.side_effect = child_items_by_parent

    # Act
    await discover_resource(child_resource, step, context, None)

    # Assert
    assert mock_paginate_list_resource_items.call_count == num_parents
    for parent_idx, parent_sr in enumerate(stored_parents):
        call_args = mock_paginate_list_resource_items.call_args_list[parent_idx]
        assert call_args[0][0] == RESOURCE_SEP2_TYPES[list_resource]
        assert call_args[0][1] == step
        assert call_args[0][2] == context
        assert call_args[0][3] == parent_sr.resource.href
        assert call_args[0][4] == DISCOVERY_LIST_PAGE_SIZE
        assert callable(call_args[0][5])  # harder to assert on the lambda

    stored_children = resource_store.get_for_type(child_resource)
    assert len(stored_children) == sum(items_per_parent)

    child_idx = 0
    for parent_idx, parent_sr in enumerate(stored_parents):
        for item in child_items_by_parent[parent_idx]:
            assert stored_children[child_idx].resource is item
            assert stored_children[child_idx].resource_type == child_resource
            assert stored_children[child_idx].id.parent_id() == parent_sr.id
            child_idx += 1

    assert len(context.warnings.warnings) == 0
    mock_get_resource_for_step.assert_not_called()


@pytest.mark.parametrize(
    "poll_rate, current_seconds, expected_wait",
    [
        (60, 0, 60),
        (60, 30, 30),
        (60, 59, 1),
        (120, 0, 120),
        (120, 60, 60),
        (30, 15, 15),
        (None, 0, 60),  # defaults to 60
        (None, 45, 15),
    ],
)
def test_calculate_wait_next_polling_window(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    poll_rate: int | None,
    current_seconds: int,
    expected_wait: int,
):
    """Poll rate from DCAP determines wait time to next window boundary"""
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    dcap = generate_class_instance(DeviceCapabilityResponse, pollRate=poll_rate, href="/dcap")
    resource_store.append_resource(CSIPAusResource.DeviceCapability, None, dcap)
    now = datetime.fromtimestamp(current_seconds, tz=timezone.utc)

    wait = calculate_wait_next_polling_window(now, resource_store)

    assert wait == expected_wait


@mock.patch("cactus_client.action.discovery.calculate_wait_next_polling_window")
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("asyncio.sleep")
@pytest.mark.asyncio
async def test_action_discovery_with_polling_window(
    mock_sleep: mock.MagicMock,
    mock_get_resource: mock.MagicMock,
    mock_calculate_wait: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """basic integration-esque check that action_discovery waits for next polling window when requested"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    mock_calculate_wait.return_value = 90
    mock_get_resource.return_value = mock.Mock()
    resources = [CSIPAusResource.DeviceCapability]
    resolved_params = {"resources": resources, "next_polling_window": True}

    # Act
    result = await action_discovery(resolved_params, step, context)

    # Assert
    mock_sleep.assert_called_once_with(90)
    assert result.done()


@mock.patch("cactus_client.action.discovery.get_list_resource_items")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@pytest.mark.asyncio
async def test_discover_resource_with_list_limit(
    mock_get_resource_for_step: mock.MagicMock,
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_list_resource_items: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test that discover_resource uses get_list_resource_items when list_limit is provided."""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    list_limit = 5

    stored_parent = resource_store.append_resource(
        CSIPAusResource.EndDeviceList,
        None,
        generate_class_instance(EndDeviceListResponse, seed=1, href="/edev", generate_relationships=True),
    )

    # Mock the limited response
    limited_items = [
        generate_class_instance(EndDeviceResponse, seed=idx, href=f"/edev/{idx}") for idx in range(list_limit)
    ]
    mock_get_list_resource_items.return_value = limited_items

    # Act
    await discover_resource(CSIPAusResource.EndDevice, step, context, list_limit)

    # Assert - should call get_list_resource_items, NOT paginate_list_resource_items
    mock_get_list_resource_items.assert_called_once()
    mock_paginate_list_resource_items.assert_not_called()

    call_args = mock_get_list_resource_items.call_args
    assert call_args[0][0] == EndDeviceListResponse
    assert call_args[0][1] == step
    assert call_args[0][2] == context
    assert call_args[0][3] == stored_parent.resource.href
    assert call_args[0][4] == list_limit
    assert callable(call_args[0][5])

    # Verify items were stored
    stored_children = resource_store.get_for_type(CSIPAusResource.EndDevice)
    assert len(stored_children) == list_limit
    assert all(sr.resource_type == CSIPAusResource.EndDevice for sr in stored_children)
