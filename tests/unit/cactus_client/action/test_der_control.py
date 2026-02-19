import unittest.mock as mock
from enum import IntEnum
from http import HTTPMethod
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlResponse
from envoy_schema.server.schema.sep2.end_device import EndDeviceResponse
from envoy_schema.server.schema.sep2.event import EventStatus
from envoy_schema.server.schema.sep2.response import ResponseType
from envoy_schema.server.schema.sep2.types import DateTimeIntervalType
from freezegun import freeze_time

from cactus_client.action.der_controls import action_respond_der_controls, action_send_malformed_response
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.schema.validator import to_hex_binary
from cactus_client.time import utc_now


@pytest.mark.parametrize(
    "event_status,time_offset,duration,previous_tags,expected_status,expect_response",
    [
        # Scheduled - send 'EVENT_RECEIVED'
        (0, 3600, 7200, [], ResponseType.EVENT_RECEIVED, True),
        # Cancelled
        (2, -300, 3600, [], ResponseType.EVENT_CANCELLED, True),
        # Superseded
        (4, -300, 3600, [], ResponseType.EVENT_SUPERSEDED, True),
        # Active - no previous tags (send 'EVENT_RECEIVED')
        (1, -300, 3600, [], ResponseType.EVENT_RECEIVED, True),
        # Active - started, already sent 'EVENT_RECEIVED' (send 'EVENT_STARTED')
        (1, -300, 3600, [ResponseType.EVENT_RECEIVED], ResponseType.EVENT_STARTED, True),
        # Active - completed, sent "EVENT_RECEIVED",'EVENT_STARTED' (send 'EVENT_COMPLETED')
        (
            1,
            -3600,
            1800,
            [ResponseType.EVENT_RECEIVED, ResponseType.EVENT_STARTED],
            ResponseType.EVENT_COMPLETED,
            True,
        ),
        # Active - completed, sent "EVENT_RECEIVED",'EVENT_STARTED' and 'EVENT_COMPLETED' (no new response)
        (
            1,
            -3600,
            1800,
            [ResponseType.EVENT_RECEIVED, ResponseType.EVENT_STARTED, ResponseType.EVENT_COMPLETED],
            None,
            False,
        ),
        # Active - in progress, already sent "EVENT_RECEIVED",'EVENT_STARTED' (no new response)
        (1, -1800, 3600, [ResponseType.EVENT_RECEIVED, ResponseType.EVENT_STARTED], None, False),
        # ---------------- ACTIONS WHICH HAVE ALREADY BEEN RESPONDED TO (DONT SEND) ----------------------
        (4, -300, 3600, [ResponseType.EVENT_SUPERSEDED], None, False),
        (2, -300, 3600, [ResponseType.EVENT_CANCELLED], None, False),
        (0, 3600, 7200, [ResponseType.EVENT_RECEIVED], None, False),
    ],
)
@freeze_time("2025-11-19 12:00:00")
@mock.patch("cactus_client.action.der_controls.submit_and_refetch_resource_for_step")
@pytest.mark.asyncio
async def test_action_respond_der_controls_with_previous_responses(
    mock_submit_and_refetch: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    event_status: int,
    time_offset: int,
    duration: int,
    previous_tags: list[IntEnum],
    expected_status: ResponseType | None,
    expect_response: bool,
):
    """Test responding to DERControls including various previous response states.

    This test checks:
    - Responses are only sent when appropriate (not sent already)
    - Previous responses are tracked using ResponseType enum names
    - In-progress notifications are sent when control has started (active EventStatus has two possible response codes).
    """

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    current_timestamp = int(utc_now().timestamp())

    # Create EndDevice
    edev = generate_class_instance(EndDeviceResponse, seed=1, generate_relationships=True)
    edev.lFDI = to_hex_binary(1000)
    stored_edev = resource_store.append_resource(CSIPAusResource.EndDevice, None, edev)

    # Create DERControl with previous responses tracked in tags
    der_control = generate_class_instance(DERControlResponse, seed=1, generate_relationships=True)
    der_control.replyTo = "/edev/rsp"
    der_control.responseRequired = to_hex_binary(1)
    der_control.EventStatus_ = generate_class_instance(EventStatus, currentStatus=event_status)
    der_control.mRID = to_hex_binary(2000)
    der_control.interval = DateTimeIntervalType(start=current_timestamp + time_offset, duration=duration)

    stored_der_control = resource_store.append_resource(CSIPAusResource.DERControl, stored_edev.id, der_control)

    # Set previous tags to simulate already-sent responses
    annotations = context.resource_annotations(step, stored_der_control.id)
    for tag in previous_tags:
        annotations.add_tag(AnnotationNamespace.RESPONSES, tag)

    mock_submit_and_refetch.return_value = mock.Mock()

    # Act
    result = await action_respond_der_controls(step, context)

    # Assert
    assert result.done()

    if expect_response:
        assert mock_submit_and_refetch.call_count == 1

        call = mock_submit_and_refetch.call_args_list[0]
        assert call[0][5].status == expected_status

        # Verify the new tag was added
        stored_controls = list(resource_store.get_for_type(CSIPAusResource.DERControl))
        assert len(stored_controls) == 1
        assert annotations.has_tag(AnnotationNamespace.RESPONSES, expected_status)

        # Previous tags should still be present
        assert all(annotations.has_tag(AnnotationNamespace.RESPONSES, tag) for tag in previous_tags)
    else:
        # Should NOT have sent a response
        assert mock_submit_and_refetch.call_count == 0

        # Tags should remain unchanged
        stored_controls = list(resource_store.get_for_type(CSIPAusResource.DERControl))
        assert len(stored_controls) == 1
        assert all(annotations.has_tag(AnnotationNamespace.RESPONSES, tag) for tag in previous_tags)


@freeze_time("2025-11-19 12:00:00")
@mock.patch("cactus_client.action.der_controls.client_error_request_for_step")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mrid_unknown,lfdi_unknown,response_invalid,expected_fake_mrid,expected_fake_lfdi,expected_status",
    [
        (True, False, False, "0xFFFFFFFF", None, 1),
        (False, True, False, None, to_hex_binary(999999), 1),
        (False, False, True, None, None, 15),
    ],
)
async def test_action_send_malformed_response(
    mock_client_error_request: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    mrid_unknown: bool,
    lfdi_unknown: bool,
    response_invalid: bool,
    expected_fake_mrid: str,
    expected_fake_lfdi: str,
    expected_status: int,
):
    """Test sending malformed Response with various malformations - expects rejection"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create DERControl
    edev = generate_class_instance(EndDeviceResponse, seed=1, generate_relationships=True)
    edev.lFDI = to_hex_binary(1001)
    stored_edev = resource_store.append_resource(CSIPAusResource.EndDevice, None, edev)

    der_control = generate_class_instance(DERControlResponse, seed=1, generate_relationships=True)
    der_control.replyTo = "/edev/1/rsp"
    der_control.responseRequired = to_hex_binary(1)
    der_control.mRID = to_hex_binary(2001)

    resource_store.append_resource(CSIPAusResource.DERControl, stored_edev.id, der_control)

    resolved_params = {
        "mrid_unknown": mrid_unknown,
        "endDeviceLFDI_unknown": lfdi_unknown,
        "response_invalid": response_invalid,
    }

    # Act
    result = await action_send_malformed_response(resolved_params, step, context)

    # Assert
    assert result.done()
    assert mock_client_error_request.call_count == 1

    # Verify the correct endpoint and payload
    call = mock_client_error_request.call_args
    assert call[0][2] == "/edev/1/rsp"
    assert call[0][3] == HTTPMethod.POST

    xml_payload = call[0][4]

    # Verify expected malformations in payload
    mrid_expected = expected_fake_mrid or der_control.mRID
    lfdi_expected = expected_fake_lfdi or edev.lFDI

    assert mrid_expected in xml_payload
    assert lfdi_expected in xml_payload
    assert f"status>{expected_status}<" in xml_payload

    if expected_fake_mrid:
        assert der_control.mRID not in xml_payload
    if expected_fake_lfdi:
        assert edev.lFDI not in xml_payload
