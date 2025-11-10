import pytest
from datetime import datetime, timezone
import unittest.mock as mock
from typing import Callable

from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse

from cactus_client.action.discovery import (
    DISCOVERY_LIST_PAGE_SIZE,
    discover_resource,
    calculate_wait_next_polling_window,
    action_discovery,
)
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.resource import RESOURCE_SEP2_TYPES


def setup_parent_resources(context, step, parent_resource_type, count, seed_base=0):
    """Helper to create parent resources with valid hrefs"""
    resource_store = context.discovered_resources(step)
    parent_type = RESOURCE_SEP2_TYPES[parent_resource_type]

    parents = []
    for i in range(count):
        parent = generate_class_instance(
            parent_type, seed=seed_base + i, href=f"/{parent_resource_type.value}/{i}", generate_relationships=True
        )
        stored = resource_store.append_resource(parent_resource_type, None, parent)
        parents.append(stored)

    return parents


def add_invalid_parent_resources(resource_store, parent_resource_type):
    """Add parent resources with missing/empty hrefs that should be skipped during discovery"""
    resource_store.append_resource(
        parent_resource_type,
        None,
        generate_class_instance(
            RESOURCE_SEP2_TYPES[parent_resource_type], seed=1001, generate_relationships=True, optional_is_none=True
        ),
    )
    resource_store.append_resource(
        parent_resource_type,
        None,
        generate_class_instance(RESOURCE_SEP2_TYPES[parent_resource_type], seed=1001, href=""),
    )


def create_fetched_resources(resource_type, count, has_href, seed_base=0):
    """Create a list of resource instances to be returned by mock fetches"""
    expected_resource_type = RESOURCE_SEP2_TYPES[resource_type]
    return [
        generate_class_instance(
            expected_resource_type,
            seed=seed_base + idx * 101,
            href=f"/{resource_type.value}/{idx}" if has_href else None,
        )
        for idx in range(count)
    ]


def assert_resources_stored_correctly(resource_store, resource_type, expected_resources, expected_parents):
    """Verify that resources were stored with correct type and parent associations"""
    added_resources = resource_store.get(resource_type)
    assert [sr.resource for sr in added_resources] == expected_resources
    assert all(sr.resource_type == resource_type for sr in added_resources)
    assert all(added_sr.parent is parent_sr for added_sr, parent_sr in zip(added_resources, expected_parents))


def assert_mock_calls_match_parents(mock_func, expected_type, step, context, parents, resource):
    """Verify mock was called with correct arguments for each parent"""
    mock_func.assert_has_calls(
        [mock.call(expected_type, step, context, parent.resource_link_hrefs[resource]) for parent in parents]
    )


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
    assert len(context.warnings.warnings) == (0 if has_href else 1)


@pytest.mark.parametrize(
    "resource, matched_parents",
    [
        (CSIPAusResource.DERList, 1),
        (CSIPAusResource.EndDeviceList, 2),
        (CSIPAusResource.FunctionSetAssignmentsList, 2),
        (CSIPAusResource.DERProgramList, 2),
        (CSIPAusResource.MirrorUsagePointList, 2),
    ],
)
@pytest.mark.parametrize("has_href", [True, False])
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_list_container_resources(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    resource: CSIPAusResource,
    matched_parents: int,
    has_href: bool,
):
    """
    Discover list containers via parent link (e.g., EndDevice.FunctionSetAssignmentsListLink.href → FunctionSetAssignmentsList).

    Fetches the LIST CONTAINER itself (not items within). Uses get_resource_for_step, not pagination.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    parent_resource = context.resource_tree.parent_resource(resource)
    stored_parents = setup_parent_resources(context, step, parent_resource, matched_parents)
    add_invalid_parent_resources(resource_store, parent_resource)

    fetched_resources = create_fetched_resources(resource, matched_parents, has_href)
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    await discover_resource(resource, step, context)

    # Assert
    assert_resources_stored_correctly(resource_store, resource, fetched_resources, stored_parents)
    assert_mock_calls_match_parents(
        mock_get_resource_for_step, RESOURCE_SEP2_TYPES[resource], step, context, stored_parents, resource
    )
    mock_paginate_list_resource_items.assert_not_called()
    assert len(context.warnings.warnings) == (0 if has_href else matched_parents)


@pytest.mark.parametrize("has_href", [True, False])
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_subscription_list_with_no_parents(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    has_href: bool,
):
    """
    Tests SubscriptionList discovery with zero parent resources.

    Special case: When there are no parent resources to fetch from, no warnings should be generated regardless of href
    presence, since no fetch operations are attempted.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    resource = CSIPAusResource.SubscriptionList

    parent_resource = context.resource_tree.parent_resource(resource)
    add_invalid_parent_resources(resource_store, parent_resource)

    # Act
    await discover_resource(resource, step, context)

    # Assert
    assert len(resource_store.get(resource)) == 0
    assert len(context.warnings.warnings) == 0
    mock_get_resource_for_step.assert_not_called()
    mock_paginate_list_resource_items.assert_not_called()


@pytest.mark.parametrize(
    "resource, matched_parents",
    [
        (CSIPAusResource.Time, 1),
        (CSIPAusResource.ConnectionPoint, 1),
        (CSIPAusResource.Registration, 2),
        (CSIPAusResource.DERCapability, 1),
        (CSIPAusResource.DERSettings, 2),
        (CSIPAusResource.DERStatus, 1),
        (CSIPAusResource.DefaultDERControl, 1),
    ],
)
@pytest.mark.parametrize("has_href", [True, False])
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_singular_resources(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    resource: CSIPAusResource,
    matched_parents: int,
    has_href: bool,
):
    """
    Discover singular resources via parent link (e.g., EndDevice.RegistrationLink.href → Registration).

    Tests 1-to-1 parent-child relationships. Uses get_resource_for_step, not pagination.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    parent_resource = context.resource_tree.parent_resource(resource)
    stored_parents = setup_parent_resources(context, step, parent_resource, matched_parents)
    add_invalid_parent_resources(resource_store, parent_resource)

    fetched_resources = create_fetched_resources(resource, matched_parents, has_href)
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    await discover_resource(resource, step, context)

    # Assert
    assert_resources_stored_correctly(resource_store, resource, fetched_resources, stored_parents)
    assert_mock_calls_match_parents(
        mock_get_resource_for_step, RESOURCE_SEP2_TYPES[resource], step, context, stored_parents, resource
    )
    mock_paginate_list_resource_items.assert_not_called()
    assert len(context.warnings.warnings) == (0 if has_href else matched_parents)


@pytest.mark.parametrize(
    "list_resource, child_resource",
    [
        (CSIPAusResource.MirrorUsagePointList, CSIPAusResource.MirrorUsagePoint),
        (CSIPAusResource.EndDeviceList, CSIPAusResource.EndDevice),
        (CSIPAusResource.DERList, CSIPAusResource.DER),
        (CSIPAusResource.DERProgramList, CSIPAusResource.DERProgram),
        (CSIPAusResource.DERControlList, CSIPAusResource.DERControl),
        (CSIPAusResource.FunctionSetAssignmentsList, CSIPAusResource.FunctionSetAssignments),
        (CSIPAusResource.SubscriptionList, CSIPAusResource.Subscription),
    ],
)
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    list_resource: CSIPAusResource,
    child_resource: CSIPAusResource,
):
    """
    Discover child items from list containers via pagination (e.g., EndDeviceList → [EndDevice, EndDevice, ...]).

    Uses paginate_list_resource_items, not get_resource_for_step.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())

    num_parents = 2
    items_per_parent = [3, 2]
    stored_parents = setup_parent_resources(context, step, list_resource, num_parents)

    # Create child items that pagination will return for each parent
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

    # Mock paginate_list_resource_items to return the children for each parent call
    mock_paginate_list_resource_items.side_effect = child_items_by_parent

    await discover_resource(child_resource, step, context)

    assert mock_paginate_list_resource_items.call_count == num_parents
    for parent_idx, parent_sr in enumerate(stored_parents):
        call_args = mock_paginate_list_resource_items.call_args_list[parent_idx]
        assert call_args[0][0] == RESOURCE_SEP2_TYPES[list_resource]
        assert call_args[0][1] == step
        assert call_args[0][2] == context
        assert call_args[0][3] == parent_sr.resource.href
        assert call_args[0][4] == DISCOVERY_LIST_PAGE_SIZE
        # call_args[0][5] is the get_list_items lambda - hard to verify directly
        assert callable(call_args[0][5])

    # check children were stored correctly
    stored_children = context.discovered_resources(step).get(child_resource)
    expected_total = sum(items_per_parent)
    assert len(stored_children) == expected_total

    # Verify each child has correct metadata and parent association
    child_idx = 0
    for parent_idx, parent_sr in enumerate(stored_parents):
        for item in child_items_by_parent[parent_idx]:
            assert stored_children[child_idx].resource is item
            assert stored_children[child_idx].resource_type == child_resource
            assert stored_children[child_idx].parent is parent_sr
            child_idx += 1

    # No warnings since all children have hrefs
    assert len(context.warnings.warnings) == 0
    # Verify get_resource_for_step was NOT called (items come from pagination, not individual fetches)
    mock_get_resource_for_step.assert_not_called()


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items_skips_invalid_parents(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Parent lists with None/empty hrefs are skipped (verifies 'if not list_href: continue' logic)"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    list_resource = CSIPAusResource.EndDeviceList
    child_resource = CSIPAusResource.EndDevice

    # Create parent lists: one valid, one with None href, one with empty
    valid_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource], seed=1, href="/valid/list", generate_relationships=True
    )
    no_href_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource],
        seed=2,
        generate_relationships=True,
        optional_is_none=True,  # This will set href=None
    )
    empty_href_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource], seed=3, href="", generate_relationships=True
    )

    # Store all parents in resource store
    valid_parent_sr = resource_store.append_resource(list_resource, None, valid_parent)
    resource_store.append_resource(list_resource, None, no_href_parent)
    resource_store.append_resource(list_resource, None, empty_href_parent)

    # Mock pagination to return children only for the valid parent
    child_items = [
        generate_class_instance(RESOURCE_SEP2_TYPES[child_resource], seed=i, href=f"/edev/{i}") for i in range(3)
    ]
    mock_paginate_list_resource_items.return_value = child_items

    # Act
    await discover_resource(child_resource, step, context)

    # Assert
    assert mock_paginate_list_resource_items.call_count == 1
    assert mock_paginate_list_resource_items.call_args[0][3] == valid_parent.href

    stored_children = resource_store.get(child_resource)
    assert len(stored_children) == 3
    assert all(child.parent is valid_parent_sr for child in stored_children)
    assert len(context.warnings.warnings) == 0


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items_empty_pagination(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Pagination returns empty list when list container exists but has no items"""
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    list_resource = CSIPAusResource.DERList
    child_resource = CSIPAusResource.DER

    parent_list = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource], seed=1, href="/der/list/empty", generate_relationships=True
    )
    resource_store.append_resource(list_resource, None, parent_list)
    mock_paginate_list_resource_items.return_value = []

    await discover_resource(child_resource, step, context)

    mock_paginate_list_resource_items.assert_called_once()
    assert len(resource_store.get(child_resource)) == 0
    assert len(context.warnings.warnings) == 0
    mock_get_resource_for_step.assert_not_called()


@pytest.mark.parametrize("has_href", [True, False])
@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items_href_warnings(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    has_href: bool,
):
    """Missing hrefs on paginated list items generate warnings"""
    context, step = testing_contexts_factory(mock.Mock())

    list_resource = CSIPAusResource.EndDeviceList
    child_resource = CSIPAusResource.EndDevice

    setup_parent_resources(context, step, list_resource, 1)

    child_items = [
        generate_class_instance(RESOURCE_SEP2_TYPES[child_resource], seed=i, href=f"/edev/{i}" if has_href else None)
        for i in range(3)
    ]
    mock_paginate_list_resource_items.return_value = child_items

    await discover_resource(child_resource, step, context)

    expected_warnings = 0 if has_href else 3
    assert len(context.warnings.warnings) == expected_warnings


@pytest.mark.parametrize(
    "poll_rate, current_seconds, expected_wait",
    [
        (60, 0, 60),
        (60, 30, 30),
        (60, 59, 1),
        (120, 0, 120),
        (120, 60, 60),
        (30, 15, 15),
        (None, 0, 60),
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


def test_calculate_wait_next_polling_window_no_dcap(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Without DCAP, defaults to 60 second poll rate"""
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    now = datetime.fromtimestamp(45, tz=timezone.utc)

    wait = calculate_wait_next_polling_window(now, resource_store)

    assert wait == 15


@mock.patch("cactus_client.action.discovery.discover_resource")
@pytest.mark.asyncio
async def test_action_discovery_follows_resource_tree(
    mock_discover: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """action_discovery follows resource tree and includes parent dependencies"""
    context, step = testing_contexts_factory(mock.Mock())

    resources = [CSIPAusResource.EndDevice, CSIPAusResource.FunctionSetAssignments]
    resolved_params = {"resources": resources}

    result = await action_discovery(resolved_params, step, context)

    assert result.done()
    # Should discover more than requested (includes parents)
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
    """action_discovery waits for next polling window when requested"""
    context, step = testing_contexts_factory(mock.Mock())
    resources = [CSIPAusResource.DeviceCapability]
    resolved_params = {"resources": resources, "next_polling_window": True}
    mock_calc_wait.return_value = 42

    result = await action_discovery(resolved_params, step, context)

    assert result.done()
    mock_calc_wait.assert_called_once()
    mock_sleep.assert_called_once_with(42)
    assert mock_discover.call_count >= 1
