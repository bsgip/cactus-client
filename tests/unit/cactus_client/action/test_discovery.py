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
    """Helper to create parent resources with varying href states"""
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


# List container resources (discovered via direct link to the list itself)
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
    Tests discovery of list container resources via direct href link from their parent.

    E.g. EndDevice.FunctionSetAssignmentsListLink.href -> fetches FunctionSetAssignmentsList container

    Note: This discovers the LIST CONTAINER itself, not items within it.
    List item discovery is tested separately.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    parent_resource = context.resource_tree.parent_resource(resource)
    stored_parent_resources = setup_parent_resources(context, step, parent_resource, matched_parents)

    # Add parents with missing/empty hrefs that should be skipped
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(
            RESOURCE_SEP2_TYPES[parent_resource], seed=1001, generate_relationships=True, optional_is_none=True
        ),
    )
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(RESOURCE_SEP2_TYPES[parent_resource], seed=1001, href=""),
    )

    # Prep the returned resources
    expected_resource_type = RESOURCE_SEP2_TYPES[resource]
    fetched_resources = [
        generate_class_instance(
            expected_resource_type,
            seed=idx * 101,
            href=f"/{resource.value}/{idx}" if has_href else None,
        )
        for idx in range(matched_parents)
    ]
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    await discover_resource(resource, step, context)

    # Assert
    added_resources = resource_store.get(resource)
    assert [sr.resource for sr in added_resources] == fetched_resources
    assert all([sr.resource_type == resource for sr in added_resources])
    assert all([added_sr.parent is parent_sr for added_sr, parent_sr in zip(added_resources, stored_parent_resources)])

    mock_get_resource_for_step.assert_has_calls(
        mock.call(expected_resource_type, step, context, spr.resource_link_hrefs[resource])
        for spr in stored_parent_resources
    )
    mock_paginate_list_resource_items.assert_not_called()

    if has_href:
        assert len(context.warnings.warnings) == 0
    else:
        assert len(context.warnings.warnings) > 0


# Singular non-list resources (special case for SubscriptionList)
@pytest.mark.parametrize(
    "resource, matched_parents, warns_on_missing_href",
    [
        (CSIPAusResource.SubscriptionList, 0, False),  # Special case: no warnings when no items
        (CSIPAusResource.Time, 1, True),
        (CSIPAusResource.ConnectionPoint, 1, True),
        (CSIPAusResource.Registration, 2, True),
        (CSIPAusResource.DERCapability, 1, True),
        (CSIPAusResource.DERSettings, 2, True),
        (CSIPAusResource.DERStatus, 1, True),
        (CSIPAusResource.DefaultDERControl, 1, True),
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
    warns_on_missing_href: bool,
):
    """
    Tests discovery of singular (non-list) resources via direct href link from their parent.

    E.g. EndDevice.RegistrationLink.href -> fetches Registration resource

    This tests direct 1-to-1 parent-child relationships via explicit links.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    parent_resource = context.resource_tree.parent_resource(resource)
    stored_parent_resources = setup_parent_resources(context, step, parent_resource, matched_parents)

    # Add parents with missing/empty hrefs that should be skipped
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(
            RESOURCE_SEP2_TYPES[parent_resource], seed=1001, generate_relationships=True, optional_is_none=True
        ),
    )
    resource_store.append_resource(
        parent_resource,
        None,
        generate_class_instance(RESOURCE_SEP2_TYPES[parent_resource], seed=1001, href=""),
    )

    # Prep the returned resources
    expected_resource_type = RESOURCE_SEP2_TYPES[resource]
    fetched_resources = [
        generate_class_instance(
            expected_resource_type,
            seed=idx * 101,
            href=f"/{resource.value}/{idx}" if has_href else None,
        )
        for idx in range(matched_parents)
    ]
    mock_get_resource_for_step.side_effect = fetched_resources

    # Act
    await discover_resource(resource, step, context)

    # Assert
    added_resources = resource_store.get(resource)
    assert [sr.resource for sr in added_resources] == fetched_resources
    assert all([sr.resource_type == resource for sr in added_resources])
    assert all([added_sr.parent is parent_sr for added_sr, parent_sr in zip(added_resources, stored_parent_resources)])

    mock_get_resource_for_step.assert_has_calls(
        mock.call(expected_resource_type, step, context, spr.resource_link_hrefs[resource])
        for spr in stored_parent_resources
    )
    mock_paginate_list_resource_items.assert_not_called()

    expect_warnings = not has_href and warns_on_missing_href
    if expect_warnings:
        assert len(context.warnings.warnings) > 0
    else:
        assert len(context.warnings.warnings) == 0


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
    """Tests discovery of child items from list-type parent resources via pagination.

    Example: EndDeviceList (at /edev) -> pagination returns [EndDevice, EndDevice, EndDevice]

    This tests the list resource branch in discover_resource():
        if is_list_resource(parent_resource):
            list_items = await paginate_list_resource_items(...)
            for item in list_items:
                resource_store.append_resource(resource, parent_sr, item)

    Verifies: pagination calls, parent-child associations, href warnings, no direct fetches.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())

    # Setup 2 parent lists
    num_parents = 2
    items_per_parent = [3, 2]  # Different counts per parent
    stored_parents = setup_parent_resources(context, step, list_resource, num_parents)

    # Create child items that pagination will return for each parent
    child_type = RESOURCE_SEP2_TYPES[child_resource]
    child_items_by_parent = [
        [
            generate_class_instance(
                child_type,
                seed=parent_idx * 100 + child_idx,
                href=f"/item/{parent_idx}/{child_idx}",
            )
            for child_idx in range(items_per_parent[parent_idx])
        ]
        for parent_idx in range(num_parents)
    ]

    # Mock paginate_list_resource_items to return the children for each parent call
    mock_paginate_list_resource_items.side_effect = child_items_by_parent

    # Act
    await discover_resource(child_resource, step, context)

    # Assert - check paginate_list_resource_items was called correctly for each parent
    assert mock_paginate_list_resource_items.call_count == num_parents

    for parent_idx, parent_sr in enumerate(stored_parents):
        call_args = mock_paginate_list_resource_items.call_args_list[parent_idx]
        assert call_args[0][0] == RESOURCE_SEP2_TYPES[list_resource], "Correct list type passed"
        assert call_args[0][1] == step, "Step passed correctly"
        assert call_args[0][2] == context, "Context passed correctly"
        assert call_args[0][3] == parent_sr.resource.href, f"Correct href for parent {parent_idx}"
        assert call_args[0][4] == DISCOVERY_LIST_PAGE_SIZE, "Correct page size"
        # call_args[0][5] is the get_list_items lambda - hard to verify directly
        assert callable(call_args[0][5]), "get_list_items lambda passed"

    # check children were stored correctly
    stored_children = context.discovered_resources(step).get(child_resource)
    expected_total_children = sum(items_per_parent)
    assert len(stored_children) == expected_total_children, f"Expected {expected_total_children} children stored"

    # Verify each child has correct metadata and parent association
    child_idx = 0
    for parent_idx, parent_sr in enumerate(stored_parents):
        for item in child_items_by_parent[parent_idx]:
            assert stored_children[child_idx].resource is item, f"Child {child_idx} has correct resource"
            assert stored_children[child_idx].resource_type == child_resource, f"Child {child_idx} has correct type"
            assert stored_children[child_idx].parent is parent_sr, f"Child {child_idx} linked to parent {parent_idx}"
            child_idx += 1

    # No warnings since all children have hrefs
    assert len(context.warnings.warnings) == 0

    # Verify get_resource_for_step was NOT called (items come from pagination, not individual fetches)
    mock_get_resource_for_step.assert_not_called()


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items_skips_parents_without_href(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Tests that parent lists without hrefs or with empty hrefs are skipped during discovery.

    Verifies the "if not list_href: continue" logic in discover_resource().
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    list_resource = CSIPAusResource.EndDeviceList
    child_resource = CSIPAusResource.EndDevice

    # Create parent lists: one valid, one with None href, one with empty href
    valid_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource],
        seed=1,
        href="/valid/list",
        generate_relationships=True,
    )
    no_href_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource],
        seed=2,
        generate_relationships=True,
        optional_is_none=True,  # This will set href=None
    )
    empty_href_parent = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource],
        seed=3,
        href="",
        generate_relationships=True,
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

    # Assert - only the valid parent should be queried
    assert mock_paginate_list_resource_items.call_count == 1, "Only valid parent queried"
    call_args = mock_paginate_list_resource_items.call_args_list[0]
    assert call_args[0][3] == valid_parent.href, "Valid parent href used"

    # Only children from valid parent should be stored
    stored_children = resource_store.get(child_resource)
    assert len(stored_children) == 3, "Only children from valid parent stored"
    assert all(child.parent is valid_parent_sr for child in stored_children), "All children linked to valid parent"

    # No warnings since valid children have hrefs
    assert len(context.warnings.warnings) == 0


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_list_items_empty_pagination_results(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Tests discovery when pagination returns empty list (no child items in the list).

    Example: DERList exists at /der/list but contains zero DER items.
    """
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    list_resource = CSIPAusResource.DERList
    child_resource = CSIPAusResource.DER

    # Create parent list
    parent_list = generate_class_instance(
        RESOURCE_SEP2_TYPES[list_resource],
        seed=1,
        href="/der/list/empty",
        generate_relationships=True,
    )
    resource_store.append_resource(list_resource, None, parent_list)

    # Mock pagination to return empty list
    mock_paginate_list_resource_items.return_value = []

    # Act
    await discover_resource(child_resource, step, context)

    # Assert
    mock_paginate_list_resource_items.assert_called_once()

    stored_children = resource_store.get(child_resource)
    assert len(stored_children) == 0, "No children stored from empty list"
    assert len(context.warnings.warnings) == 0, "No warnings for empty lists"
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
    """Tests that missing hrefs on list items generate appropriate warnings"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())

    list_resource = CSIPAusResource.EndDeviceList
    child_resource = CSIPAusResource.EndDevice

    setup_parent_resources(context, step, list_resource, 1)

    # Mock pagination to return items with or without hrefs
    child_items = [
        generate_class_instance(RESOURCE_SEP2_TYPES[child_resource], seed=i, optional_is_none=not has_href)
        for i in range(3)
    ]
    mock_paginate_list_resource_items.return_value = child_items

    # Act
    await discover_resource(child_resource, step, context)

    # Assert
    if has_href:
        assert len(context.warnings.warnings) == 0, "No warnings when children have hrefs"
    else:
        assert len(context.warnings.warnings) > 0, "Warnings generated when children lack hrefs"


@pytest.mark.parametrize(
    "poll_rate, current_seconds, expected_wait",
    [
        (60, 0, 60),
        (60, 30, 30),
        (60, 59, 1),
        (120, 0, 120),
        (120, 60, 60),
        (30, 15, 15),
        (None, 0, 60),  # Default 60s
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

    assert wait == 15  # 60 - 45


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
