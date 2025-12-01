from http import HTTPMethod
from unittest import mock

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.csip_aus.connection_point import (
    ConnectionPointRequest,
    ConnectionPointResponse,
)
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceRequest,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.identification import Link
from envoy_schema.server.schema.sep2.types import DeviceCategory

from cactus_client.action.end_device import (
    action_insert_end_device,
    action_upsert_connection_point,
)
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult
from cactus_client.time import utc_now


@pytest.mark.asyncio
async def test_action_upsert_connection_point(testing_contexts_factory):
    """Test that action_upsert_connection_point creates a valid ConnectionPoint request"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    cp_link = Link(href="/edev/1/cp")
    end_device = generate_class_instance(EndDeviceResponse, lFDI=client_config.lfdi, ConnectionPointLink=cp_link)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, end_device)

    with mock.patch("cactus_client.action.end_device.submit_and_refetch_resource_for_step") as mock_submit:
        cp_id = "test-cp-1"
        inserted_cp = generate_class_instance(ConnectionPointResponse, id=cp_id, href="/edev/1/cp/test-cp-1")
        mock_submit.return_value = inserted_cp

        resolved_params = {"connectionPointId": cp_id}

        # Act
        result = await action_upsert_connection_point(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.repeat is False

        # Verify submit_and_refetch_resource_for_step was called correctly
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args
        assert call_args[0][0] == ConnectionPointResponse
        assert call_args[0][3] == HTTPMethod.PUT
        assert call_args[0][4] == cp_link.href

        # Check the actual ConnectionPoint request body
        sent_xml = call_args[0][5]
        sent_request = ConnectionPointRequest.from_xml(sent_xml)

        assert sent_request.id == cp_id

        # Verify the ConnectionPoint was stored in the resource store with the NEW href
        stored_cps = resource_store.get_for_type(CSIPAusResource.ConnectionPoint)
        assert len(stored_cps) == 1
        assert stored_cps[0].resource.id == inserted_cp.id
        assert stored_cps[0].resource.href == inserted_cp.href  # Should be the new href from server
        assert stored_cps[0].resource.href == "/edev/1/cp/test-cp-1"


@pytest.mark.asyncio
async def test_action_insert_end_device(testing_contexts_factory):
    """Test that action_insert_end_device creates a valid EndDevice request"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    resource_store.append_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    with mock.patch("cactus_client.action.end_device.submit_and_refetch_resource_for_step") as mock_submit:

        inserted_edev = generate_class_instance(
            EndDeviceResponse,
            lFDI=client_config.lfdi,
            sFDI=client_config.sfdi,
            href="/edev/1",
            postRate=60,
            changedTime=int(utc_now().timestamp()),
            deviceCategory=f"{DeviceCategory.PHOTOVOLTAIC_SYSTEM.value:02X}",
        )
        mock_submit.return_value = inserted_edev

        # Act
        result = await action_insert_end_device({}, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.repeat is False

        # Verify submit_and_refetch_resource_for_step was called correctly
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args
        assert call_args[0][0] == EndDeviceResponse
        assert call_args[0][3] == HTTPMethod.POST
        assert call_args[0][4] == "/edev"

        # Check the actual EndDevice request body that was sent
        sent_xml = call_args[0][5]
        sent_request = EndDeviceRequest.from_xml(sent_xml)

        assert sent_request.lFDI == client_config.lfdi
        assert sent_request.sFDI == client_config.sfdi
        assert sent_request.postRate == 60
        assert sent_request.deviceCategory == f"{DeviceCategory.PHOTOVOLTAIC_SYSTEM.value:02X}"

        # Verify the EndDevice was stored in the resource store
        stored_edevs = resource_store.get_for_type(CSIPAusResource.EndDevice)
        assert len(stored_edevs) == 1
        assert stored_edevs[0].resource.lFDI == inserted_edev.lFDI
        assert stored_edevs[0].resource.href == inserted_edev.href
