from http import HTTPMethod
from unittest import mock

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.csip_aus.connection_point import ConnectionPointResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
)

from cactus_client.action.refresh_resource import action_refresh_resource
from cactus_client.error import RequestException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult


@pytest.mark.asyncio
async def test_action_refresh_resource_happy_path(testing_contexts_factory):

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create multiple EndDevices in the store
    edev1 = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=60)
    edev2 = generate_class_instance(EndDeviceResponse, href="/edev/2", postRate=60)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev1)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev2)

    with mock.patch("cactus_client.action.refresh_resource.get_resource_for_step") as mock_get:
        # Create updated versions
        updated_edev1 = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=120)
        updated_edev2 = generate_class_instance(EndDeviceResponse, href="/edev/2", postRate=200)
        mock_get.side_effect = [updated_edev1, updated_edev2]

        resolved_params = {"resource": CSIPAusResource.EndDevice.value}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Verify get_resource_for_step was called twice
        assert mock_get.call_count == 2

        # Check first call in detail
        first_call_args = mock_get.call_args_list[0]
        assert first_call_args[0][0] == EndDeviceResponse
        assert first_call_args[0][2] == context
        assert first_call_args[0][3] == "/edev/1"

        # Verify both resources were updated in the store
        stored_edevs = resource_store.get_for_type(CSIPAusResource.EndDevice)
        assert len(stored_edevs) == 2
        assert stored_edevs[0].resource.postRate == 120  # Updated value
        assert stored_edevs[1].resource.postRate == 200  # Updated value


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing ConnectionPoint
    cp = generate_class_instance(ConnectionPointResponse, href="/edev/1/cp/1")
    resource_store.upsert_resource(CSIPAusResource.ConnectionPoint, None, cp)

    with mock.patch("cactus_client.action.refresh_resource.client_error_request_for_step") as mock_error:
        mock_error.return_value = mock.Mock()  # Return a mock error response

        resolved_params = {"resource": CSIPAusResource.ConnectionPoint.value, "expect_rejection": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Verify client_error_request_for_step was called
        mock_error.assert_called_once()
        call_args = mock_error.call_args
        assert call_args[0][2] == "/edev/1/cp/1"
        assert call_args[0][3] == HTTPMethod.GET


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_failure(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing ConnectionPoint
    cp = generate_class_instance(ConnectionPointResponse, href="/edev/1/cp/1")
    resource_store.upsert_resource(CSIPAusResource.ConnectionPoint, None, cp)

    with mock.patch("cactus_client.action.refresh_resource.client_error_request_for_step") as mock_error:
        mock_error.side_effect = RequestException("mock exception abc")

        resolved_params = {"resource": CSIPAusResource.ConnectionPoint.value, "expect_rejection": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str) and "mock exception abc" in result.description

        # Verify client_error_request_for_step was called
        mock_error.assert_called_once()
        call_args = mock_error.call_args
        assert call_args[0][2] == "/edev/1/cp/1"
        assert call_args[0][3] == HTTPMethod.GET


@pytest.mark.parametrize(
    "list_resource, list_type",
    [
        (CSIPAusResource.EndDeviceList, EndDeviceListResponse),
        (CSIPAusResource.MirrorUsagePointList, MirrorUsagePointListResponse),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_list_success(
    testing_contexts_factory, list_resource: CSIPAusResource, list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty correctly for list types"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(list_type, href="/foobar")
    resource_store.upsert_resource(list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_or_empty_list_request_for_step"
    ) as mock_client_error_or_empty_list_request_for_step:

        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_or_empty_list_request_for_step.return_value = generate_class_instance(ErrorResponse)
        resolved_params = {"resource": list_resource.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called first to check response
        mock_client_error_or_empty_list_request_for_step.assert_called_once_with(
            list_type, step, context, "/foobar", HTTPMethod.GET
        )


@pytest.mark.parametrize(
    "non_list_resource, non_list_type",
    [
        (CSIPAusResource.EndDevice, EndDeviceResponse),
        (CSIPAusResource.MirrorUsagePoint, MirrorUsagePoint),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_non_list_success(
    testing_contexts_factory, non_list_resource: CSIPAusResource, non_list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty correctly for NON list types"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(non_list_type, href="/foobar")
    resource_store.upsert_resource(non_list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_client_error_request_for_step:

        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_request_for_step.return_value = generate_class_instance(ErrorResponse)
        resolved_params = {"resource": non_list_resource.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called first to check response
        mock_client_error_request_for_step.assert_called_once_with(step, context, "/foobar", HTTPMethod.GET)


@pytest.mark.parametrize(
    "list_resource, list_type",
    [
        (CSIPAusResource.EndDeviceList, EndDeviceListResponse),
        (CSIPAusResource.MirrorUsagePointList, MirrorUsagePointListResponse),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_list_failure(
    testing_contexts_factory, list_resource: CSIPAusResource, list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty raising an error correctly"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(list_type, href="/foobar")
    resource_store.upsert_resource(list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_or_empty_list_request_for_step"
    ) as mock_client_error_or_empty_list_request_for_step:

        mock_client_error_or_empty_list_request_for_step.side_effect = RequestException("mock exception")
        resolved_params = {"resource": list_resource.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str)

        # Check request_for_step was called first to check response
        mock_client_error_or_empty_list_request_for_step.assert_called_once_with(
            list_type, step, context, "/foobar", HTTPMethod.GET
        )


@pytest.mark.parametrize(
    "non_list_resource, non_list_type",
    [
        (CSIPAusResource.EndDevice, EndDeviceResponse),
        (CSIPAusResource.MirrorUsagePoint, MirrorUsagePoint),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_non_list_failure(
    testing_contexts_factory, non_list_resource: CSIPAusResource, non_list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty failures correctly for NON list types"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(non_list_type, href="/foobar")
    resource_store.upsert_resource(non_list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_client_error_request_for_step:

        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_request_for_step.side_effect = RequestException("mock exception")
        resolved_params = {"resource": non_list_resource.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str)

        # Check request_for_step was called first to check response
        mock_client_error_request_for_step.assert_called_once_with(step, context, "/foobar", HTTPMethod.GET)
