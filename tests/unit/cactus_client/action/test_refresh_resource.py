from http import HTTPMethod
from unittest import mock
import pytest
from cactus_client.action.refresh_resource import action_refresh_resource
from assertical.fake.generator import generate_class_instance
from envoy_schema.server.schema.sep2.end_device import EndDeviceResponse, EndDeviceListResponse
from envoy_schema.server.schema.csip_aus.connection_point import ConnectionPointResponse
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_client.model.execution import ActionResult


@pytest.mark.asyncio
async def test_action_refresh_resource_happy_path(testing_contexts_factory):

    # Arrange
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
        stored_edevs = resource_store.get(CSIPAusResource.EndDevice)
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
async def test_action_refresh_resource_expect_rejection_or_empty_with_rejection(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    resource_store.upsert_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request, mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_error:

        # Mock response indicating client error
        mock_response = mock.Mock()
        mock_response.is_client_error.return_value = True
        mock_response.is_success.return_value = False
        mock_request.return_value = mock_response

        resolved_params = {"resource": CSIPAusResource.EndDeviceList.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called first to check response
        mock_request.assert_called_once_with(step, context, "/edev", HTTPMethod.GET)

        # Check client_error_request_for_step was called after detecting error
        mock_error.assert_called_once_with(step, context, "/edev", HTTPMethod.GET)


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_with_empty_list(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    resource_store.upsert_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request, mock.patch(
        "cactus_client.action.refresh_resource.get_resource_for_step"
    ) as mock_get:

        # Mock response indicating success
        mock_response = mock.Mock()
        mock_response.is_client_error.return_value = False
        mock_response.is_success.return_value = True
        mock_request.return_value = mock_response

        # Mock the fetched resource as an empty list
        empty_list = generate_class_instance(EndDeviceListResponse, EndDevice=[], href="/edev", all_=0)
        mock_get.return_value = empty_list

        resolved_params = {"resource": CSIPAusResource.EndDeviceList.value, "expect_rejection_or_empty": True}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called
        mock_request.assert_called_once_with(step, context, "/edev", HTTPMethod.GET)

        # Check get_resource_for_step was called
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == EndDeviceListResponse
        assert call_args[0][3] == "/edev"
