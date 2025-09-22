import unittest.mock as mock
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.action.discovery import (
    DISCOVERY_LIST_PAGE_SIZE,
    discover_resource,
    discover_resource_plan,
    do_discovery_list_items,
    do_discovery_singular,
    get_resource_tree,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution


def test_get_resource_tree_all_resources_encoded():
    tree = get_resource_tree()
    for resource in CSIPAusResource:
        assert resource in tree


@pytest.mark.parametrize(
    "targets, expected",
    [
        ([], []),
        ([CSIPAusResource.Time], [CSIPAusResource.DeviceCapability, CSIPAusResource.Time]),
        ([CSIPAusResource.Time, CSIPAusResource.Time], [CSIPAusResource.DeviceCapability, CSIPAusResource.Time]),
        (
            [CSIPAusResource.DERSettings],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
            ],
        ),
        (
            [CSIPAusResource.DERSettings, CSIPAusResource.Time],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
            ],
        ),
        (
            [
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
                CSIPAusResource.DERProgramList,
                CSIPAusResource.DERCapability,
            ],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
                CSIPAusResource.FunctionSetAssignmentsList,
                CSIPAusResource.FunctionSetAssignments,
                CSIPAusResource.DERProgramList,
                CSIPAusResource.DERCapability,
            ],
        ),
    ],
)
def test_discover_resource_plan(targets, expected):
    tree = get_resource_tree()

    actual = discover_resource_plan(tree, targets)
    assert actual == expected
    assert_list_type(CSIPAusResource, actual, len(expected))


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@pytest.mark.asyncio
async def test_do_discovery_singular(
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Tests that the resource store is correctly updated when calling do_discovery_singular"""
    # Arrange
    target_resource = CSIPAusResource.DERList  # Doesn't particularly matter what
    target_type = mock.Mock()
    parent_resource = CSIPAusResource.DERCapability  # Doesn't particularly matter what

    existing_r1 = generate_class_instance(Resource, seed=101)
    existing_r2 = generate_class_instance(Resource, seed=202)
    existing_r3 = generate_class_instance(Resource, seed=303)
    new_r1 = generate_class_instance(Resource, seed=404)
    new_r2 = generate_class_instance(Resource, seed=505)

    context, step = testing_contexts_factory(mock.Mock())
    context.discovered_resources(step).set_resource(target_resource, None, existing_r1)
    sr2 = context.discovered_resources(step).set_resource(parent_resource, None, existing_r2)
    sr3 = context.discovered_resources(step).append_resource(parent_resource, None, existing_r3)

    mock_get_resource_for_step.side_effect = [new_r1, new_r2]

    # Act
    await do_discovery_singular(
        target_resource,
        target_type,
        parent_resource,
        lambda r: "/my/resource/1" if r is sr2 else "/my/resource/2",
        step,
        context,
    )

    # Assert
    assert context.discovered_resources(step).get(parent_resource) == [sr2, sr3], "No change to parent resource"
    assert [sr.resource for sr in context.discovered_resources(step).get(target_resource)] == [
        new_r1,
        new_r2,
    ], "Target resource updated"
    mock_get_resource_for_step.assert_has_calls(
        [
            mock.call(target_type, step, context, "/my/resource/1"),
            mock.call(target_type, step, context, "/my/resource/2"),
        ]
    )


@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_do_discovery_list_items(
    mock_paginate_list_resource_items: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Tests that the resource store is correctly updated when calling do_discovery_singular"""
    # Arrange
    target_resource = CSIPAusResource.DERList  # Doesn't particularly matter what
    list_resource = CSIPAusResource.DERCapability  # Doesn't particularly matter what
    list_type = mock.Mock()
    get_list_items = mock.Mock()

    existing_item1 = generate_class_instance(Resource, seed=101)
    existing_list1 = generate_class_instance(Resource, seed=202)
    existing_list2 = generate_class_instance(Resource, seed=303)

    new_r1 = generate_class_instance(Resource, seed=404)
    new_r2 = generate_class_instance(Resource, seed=505)
    new_r3 = generate_class_instance(Resource, seed=606)

    context, step = testing_contexts_factory(mock.Mock())
    context.discovered_resources(step).set_resource(target_resource, None, existing_item1)
    sr_list1 = context.discovered_resources(step).set_resource(list_resource, None, existing_list1)
    sr_list2 = context.discovered_resources(step).append_resource(list_resource, None, existing_list2)

    mock_paginate_list_resource_items.side_effect = [[new_r1, new_r2], [new_r3]]

    # Act
    await do_discovery_list_items(
        target_resource,
        list_resource,
        list_type,
        lambda r: "/list/1" if r is sr_list1 else "/list/2",
        get_list_items,
        step,
        context,
    )

    # Assert
    assert context.discovered_resources(step).get(list_resource) == [sr_list1, sr_list2], "No change to list resource"
    assert [sr.resource for sr in context.discovered_resources(step).get(target_resource)] == [
        new_r1,
        new_r2,
        new_r3,
    ], "Target resource updated"
    mock_paginate_list_resource_items.assert_has_calls(
        [
            mock.call(list_type, step, context, "/list/1", DISCOVERY_LIST_PAGE_SIZE, get_list_items),
            mock.call(list_type, step, context, "/list/2", DISCOVERY_LIST_PAGE_SIZE, get_list_items),
        ]
    )


@mock.patch("cactus_client.action.discovery.get_resource_for_step")
@mock.patch("cactus_client.action.discovery.paginate_list_resource_items")
@pytest.mark.asyncio
async def test_discover_resource_dcap(
    mock_paginate_list_resource_items: mock.MagicMock,
    mock_get_resource_for_step: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """DeviceCapability is a special discovery case - it can go direct to the device capability URI"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    dcap = generate_class_instance(DeviceCapabilityResponse)
    mock_get_resource_for_step.return_value = dcap

    # Act
    await discover_resource(CSIPAusResource.DeviceCapability, step, context)

    # Assert
    stored_resources = context.discovered_resources(step).get(CSIPAusResource.DeviceCapability)
    assert len(stored_resources) == 1
    assert stored_resources[0].resource is dcap
    assert stored_resources[0].type == CSIPAusResource.DeviceCapability
    assert stored_resources[0].parent is None
    mock_get_resource_for_step.assert_called_once_with(DeviceCapabilityResponse, step, context, context.dcap_path)
    mock_paginate_list_resource_items.assert_not_called()


@pytest.mark.parametrize("resource", [val for val in CSIPAusResource if val != CSIPAusResource.DeviceCapability])
@mock.patch("cactus_client.action.discovery.do_discovery_singular")
@mock.patch("cactus_client.action.discovery.do_discovery_list_items")
@pytest.mark.asyncio
async def test_discover_resource_sanity_checking_individual_params(
    mock_list_items: mock.MagicMock, mock_singular: mock.MagicMock, resource: CSIPAusResource
):
    """There are a LOT of matched resources in this function and a lot of copy pasting - it'd be easy to miss a simple
    type / resource value. This will walk all the calls and have a look for anything that's obviously missing."""

    tree = get_resource_tree()
    parent_resource = tree.ancestor(resource)
    expecting_list_items_call = parent_resource and is_list_resource(parent_resource)

    step = mock.Mock(StepExecution)
    context = mock.Mock(ExecutionContext)
    await discover_resource(resource, step, context)

    if expecting_list_items_call:
        mock_list_items.assert_called_once()
        mock_singular.assert_not_called()

        mock_list_items.assert_has_calls(
            [
                mock.call(
                    target_resource=resource,
                    list_resource=parent_resource,
                    list_type=mock.ANY,
                    get_list_href=mock.ANY,
                    get_list_items=mock.ANY,
                    step=step,
                    context=context,
                )
            ]
        )
    else:
        mock_list_items.assert_not_called()
        mock_singular.assert_called_once()

        mock_singular.assert_has_calls(
            [
                mock.call(
                    target_resource=resource,
                    target_type=mock.ANY,
                    parent_resource=parent_resource,
                    step=step,
                    context=context,
                    get_href=mock.ANY,
                )
            ]
        )


@mock.patch("cactus_client.action.discovery.do_discovery_singular")
@mock.patch("cactus_client.action.discovery.do_discovery_list_items")
@pytest.mark.asyncio
async def test_discover_resource_unsupported_resource_error(
    mock_list_items: mock.MagicMock, mock_singular: mock.MagicMock
):
    with pytest.raises(CactusClientException):
        await discover_resource("invalid respirce", mock.Mock(), mock.Mock())

    mock_list_items.assert_not_called()
    mock_singular.assert_not_called()
