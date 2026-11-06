from http import HTTPMethod, HTTPStatus
from unittest import mock

import pytest
from assertical.asserts.time import assert_nowish
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions import CSIPAusVersion
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
from envoy_schema.server.schema.sep2.identification import Link, Resource
from envoy_schema.server.schema.sep2.types import DeviceCategory

from cactus_client.action.end_device import (
    action_insert_end_device,
    action_upsert_connection_point,
    rewrite_legacy_csipaus_xmlns,
)
from cactus_client.action.server import resource_to_sep2_xml
from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION
from cactus_client.error import CactusClientError
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import ClientContext, ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.http import ServerResponse
from cactus_client.model.resource import ResourceStore
from cactus_client.schema.validator import validate_xml
from cactus_client.time import utc_now


@pytest.mark.parametrize(
    "resource",
    [
        generate_class_instance(ConnectionPointRequest, id_v11=None, type=None),
    ],
)
def test_rewrite_legacy_csipaus_xmlns(resource: Resource):
    current_xml = resource_to_sep2_xml(resource)
    v12_xml = rewrite_legacy_csipaus_xmlns(resource)

    # Check that the current XML is schema valid under the current CSIP Version
    assert len(validate_xml(current_xml, version=ACTIVE_CSIP_AUS_VERSION)) == 0
    assert len(validate_xml(v12_xml, version=ACTIVE_CSIP_AUS_VERSION)) > 0

    # Now do the reverse check for the rewritten "legacy" version
    assert len(validate_xml(current_xml, version=CSIPAusVersion.RELEASE_1_2)) > 0
    assert len(validate_xml(v12_xml, version=CSIPAusVersion.RELEASE_1_2)) == 0


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
        assert call_args[0][0] == ConnectionPointRequest
        assert call_args[0][3] == HTTPMethod.PUT
        assert call_args[0][4] == cp_link.href

        # Check the actual ConnectionPoint request body
        sent_request: ConnectionPointRequest = call_args[0][5]

        assert sent_request.id == cp_id

        # Verify the ConnectionPoint was stored in the resource store with the NEW href
        stored_cps = resource_store.get_for_type(CSIPAusResource.ConnectionPoint)
        assert len(stored_cps) == 1
        assert stored_cps[0].resource.id == inserted_cp.id
        assert stored_cps[0].resource.href == inserted_cp.href  # Should be the new href from server
        assert stored_cps[0].resource.href == "/edev/1/cp/test-cp-1"


@pytest.mark.parametrize(
    "returned_status, expect_success",
    [
        (HTTPStatus.BAD_REQUEST, True),
        (HTTPStatus.OK, False),
        (HTTPStatus.NO_CONTENT, False),
        (HTTPStatus.FORBIDDEN, False),
    ],
)
@pytest.mark.asyncio
async def test_action_upsert_connection_point_legacy_xmlns(
    testing_contexts_factory, returned_status: HTTPStatus, expect_success: bool
):
    """Test that action_upsert_connection_point inspects the returned status code"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    cp_link = Link(href="/edev/1/cp")
    end_device = generate_class_instance(EndDeviceResponse, lFDI=client_config.lfdi, ConnectionPointLink=cp_link)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, end_device)

    with mock.patch("cactus_client.action.end_device.request_for_step") as mock_request_for_step:
        cp_id = "test-cp-1"
        server_response = generate_class_instance(ServerResponse, status=returned_status, xsd_errors=None, headers=None)
        mock_request_for_step.return_value = server_response

        resolved_params = {"connectionPointId": cp_id, "legacy_xmlns": True}

        # Act/Assert
        if expect_success:
            result = await action_upsert_connection_point(resolved_params, step, context)
            assert isinstance(result, ActionResult)
            assert result.repeat is False
            assert result.completed is True
        else:
            with pytest.raises(CactusClientError):
                await action_upsert_connection_point(resolved_params, step, context)

        mock_request_for_step.assert_called_once()


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
        sent_request: EndDeviceRequest = call_args[0][5]

        assert_nowish(sent_request.changedTime)
        assert sent_request.lFDI == client_config.lfdi
        assert sent_request.sFDI == client_config.sfdi
        assert sent_request.postRate == 60
        assert sent_request.deviceCategory == f"{DeviceCategory.PHOTOVOLTAIC_SYSTEM.value:02X}"

        # Verify the EndDevice was stored in the resource store
        stored_edevs = resource_store.get_for_type(CSIPAusResource.EndDevice)
        assert len(stored_edevs) == 1
        assert stored_edevs[0].resource.lFDI == inserted_edev.lFDI
        assert stored_edevs[0].resource.href == inserted_edev.href


@pytest.mark.parametrize(
    "returned_status, expect_success",
    [
        (HTTPStatus.NOT_ACCEPTABLE, True),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.OK, False),
        (HTTPStatus.NO_CONTENT, False),
    ],
)
@pytest.mark.asyncio
async def test_action_insert_end_device_invalid_version_header(
    testing_contexts_factory, returned_status: HTTPStatus, expect_success: bool
):
    """Test that action_insert_end_device correctly handles sending an invalid version Accept header"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    resource_store.append_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    with mock.patch("cactus_client.action.end_device.request_for_step") as mock_request_for_step:
        server_response = generate_class_instance(ServerResponse, status=returned_status, xsd_errors=None, headers=None)
        mock_request_for_step.return_value = server_response
        resolved_params = {"invalid_version_header": True}

        # Act/Assert
        if expect_success:
            result = await action_insert_end_device(resolved_params, step, context)
            assert isinstance(result, ActionResult)
            assert result.repeat is False
            assert result.completed is True
        else:
            with pytest.raises(CactusClientError):
                await action_insert_end_device(resolved_params, step, context)

        mock_request_for_step.assert_called_once()


@pytest.mark.asyncio
async def test_action_insert_end_device_cross_client_uses_context_lfdi(
    testing_contexts_factory,
):
    """Test to ensure S-ALL-52 behaviour"""

    # Arrange - build with CLIENT-A as the base, then add CLIENT-B
    context, _ = testing_contexts_factory(mock.Mock())
    client_a_alias = list(context.clients_by_alias.keys())[0]
    client_a_lfdi = context.clients_by_alias[client_a_alias].client_config.lfdi

    client_b_alias = "CLIENT-B"
    client_b_config = generate_class_instance(ClientConfig, optional_is_none=True, lfdi="BBBBBBBBBB")
    client_b_context = ClientContext(
        test_procedure_alias=client_b_alias,
        client_config=client_b_config,
        discovered_resources=ResourceStore(context.resource_tree),
        discovered_well_known=None,
        session=mock.Mock(),
        annotations={},
        notifications=None,
    )
    context.clients_by_alias[client_b_alias] = client_b_context

    # EndDeviceList goes into CLIENT-A's resource store (the context/resources client)
    client_a_resource_store = context.clients_by_alias[client_a_alias].discovered_resources
    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    client_a_resource_store.append_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    # Step executes as CLIENT-B but uses CLIENT-A's resource context (simulates use_client_context: CLIENT-A)
    step = generate_class_instance(
        StepExecution,
        optional_is_none=True,
        client_alias=client_b_alias,
        client_resources_alias=client_a_alias,
    )

    with mock.patch("cactus_client.action.end_device.client_error_request_for_step") as mock_reject:
        mock_reject.return_value = None

        # Act
        result = await action_insert_end_device({"expect_rejection": True}, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        mock_reject.assert_called_once()

        xml_body: str = mock_reject.call_args[0][4]
        assert client_a_lfdi.upper() in xml_body.upper()  # body contains CLIENT-A's LFDI (the context client)
        assert client_b_config.lfdi.upper() not in xml_body.upper()  # not CLIENT-B's LFDI (the executing client)
