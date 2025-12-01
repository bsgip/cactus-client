import unittest.mock as mock
from http import HTTPMethod
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DER,
    ActivePower,
    ConnectStatusTypeValue,
    DERCapability,
    DERSettings,
    DERStatus,
    DERType,
    OperationalModeStatusType,
    OperationalModeStatusTypeValue,
)
from freezegun import freeze_time

from cactus_client.action.der import (
    action_send_malformed_der_settings,
    action_upsert_der_capability,
    action_upsert_der_settings,
    action_upsert_der_status,
)
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.schema.validator import to_hex_binary
from cactus_client.time import utc_now


@mock.patch("cactus_client.action.der.submit_and_refetch_resource_for_step")
@pytest.mark.asyncio
async def test_action_upsert_der_capability(
    mock_submit_and_refetch: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create multiple DERs
    num_devices = 3
    for i in range(num_devices):
        der = generate_class_instance(DER, seed=i, generate_relationships=True)
        resource_store.append_resource(CSIPAusResource.DER, None, der)

    # Mock the response with expected values
    expected_type = DERType.PHOTOVOLTAIC_SYSTEM
    expected_rtgMaxW = ActivePower(value=5000, multiplier=0)
    expected_modesSupported = to_hex_binary(1)
    expected_doeModesSupported = to_hex_binary(1)

    inserted_dcaps = [
        generate_class_instance(
            DERCapability,
            seed=i * 101,
            type_=expected_type,
            rtgMaxW=expected_rtgMaxW,
            modesSupported=expected_modesSupported,
            doeModesSupported=expected_doeModesSupported,
        )
        for i in range(num_devices)
    ]
    mock_submit_and_refetch.side_effect = inserted_dcaps

    resolved_params = {
        "type": expected_type.value,
        "rtgMaxW": 5000,
        "modesSupported": 1,
        "doeModesSupported": 1,
    }

    # Act
    result = await action_upsert_der_capability(resolved_params, step, context)

    # Assert
    assert result.done()
    assert mock_submit_and_refetch.call_count == num_devices

    # Verify all resources were stored
    stored_dcaps = resource_store.get_for_type(CSIPAusResource.DERCapability)
    assert len(stored_dcaps) == num_devices

    # Verify contents of first device
    first_dcap = stored_dcaps[0].resource
    assert first_dcap.type_ == expected_type
    assert first_dcap.rtgMaxW == expected_rtgMaxW
    assert first_dcap.modesSupported == expected_modesSupported
    assert first_dcap.doeModesSupported == expected_doeModesSupported


@freeze_time("2025-11-13 12:00:00")
@mock.patch("cactus_client.action.der.submit_and_refetch_resource_for_step")
@pytest.mark.asyncio
async def test_action_upsert_der_settings(
    mock_submit_and_refetch: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test upserting DERSettings for multiple devices, verify one device's contents"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    expected_timestamp = int(utc_now().timestamp())  # Get the frozen time

    # Create multiple DERs with DERSettingsLinks
    num_devices = 3
    for i in range(num_devices):
        der = generate_class_instance(DER, seed=i, generate_relationships=True)
        resource_store.append_resource(CSIPAusResource.DER, None, der)

    # Mock the response with expected values
    expected_setMaxW = ActivePower(value=4500, multiplier=0)
    expected_setGradW = 100
    expected_modesEnabled = to_hex_binary(1)
    expected_doeModesEnabled = to_hex_binary(1)

    inserted_settings = [
        generate_class_instance(
            DERSettings,
            seed=i * 101,
            updatedTime=expected_timestamp,
            setMaxW=expected_setMaxW,
            setGradW=expected_setGradW,
            modesEnabled=expected_modesEnabled,
            doeModesEnabled=expected_doeModesEnabled,
        )
        for i in range(num_devices)
    ]
    mock_submit_and_refetch.side_effect = inserted_settings

    resolved_params = {
        "setMaxW": expected_setMaxW.value,
        "setGradW": expected_setGradW,
        "modesEnabled": expected_modesEnabled,
        "doeModesEnabled": expected_doeModesEnabled,
    }

    # Act
    result = await action_upsert_der_settings(resolved_params, step, context)

    # Assert
    assert result.done()
    assert mock_submit_and_refetch.call_count == num_devices

    # Verify all resources were stored
    stored_settings = resource_store.get_for_type(CSIPAusResource.DERSettings)
    assert len(stored_settings) == num_devices

    # Verify contents of first device
    first_settings = stored_settings[0].resource
    assert first_settings.updatedTime == expected_timestamp
    assert first_settings.setMaxW == expected_setMaxW
    assert first_settings.setGradW == expected_setGradW
    assert first_settings.modesEnabled == expected_modesEnabled
    assert first_settings.doeModesEnabled == expected_doeModesEnabled


@freeze_time("2025-11-13 12:00:00")
@mock.patch("cactus_client.action.der.submit_and_refetch_resource_for_step")
@pytest.mark.asyncio
async def test_action_upsert_der_status(
    mock_submit_and_refetch: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test upserting DERStatus for multiple devices, verify one device's contents"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    expected_timestamp = int(utc_now().timestamp())  # Get the frozen time

    # Create multiple DERs with DERStatusLinks
    num_devices = 3
    for i in range(num_devices):
        der = generate_class_instance(DER, seed=i, generate_relationships=True)
        resource_store.append_resource(CSIPAusResource.DER, None, der)

    # Define expected input values
    expected_gen_connect_val = 1
    expected_op_mode_val = 3
    expected_alarm_val = 2

    # Build expected objects (matching what the action creates)
    expected_gen_connect_status = ConnectStatusTypeValue(
        value=to_hex_binary(expected_gen_connect_val), dateTime=expected_timestamp
    )
    expected_operational_mode_status = OperationalModeStatusTypeValue(
        value=OperationalModeStatusType(expected_op_mode_val), dateTime=expected_timestamp
    )
    expected_alarm_status = to_hex_binary(expected_alarm_val)

    # Mock the response with expected values
    inserted_statuses = [
        DERStatus(
            href=f"/derstatus{i}",
            readingTime=expected_timestamp,
            genConnectStatus=expected_gen_connect_status,
            operationalModeStatus=expected_operational_mode_status,
            alarmStatus=expected_alarm_status,
        )
        for i in range(num_devices)
    ]
    mock_submit_and_refetch.side_effect = inserted_statuses

    resolved_params = {
        "genConnectStatus": expected_gen_connect_val,
        "operationalModeStatus": expected_op_mode_val,
        "alarmStatus": expected_alarm_val,
    }

    # Act
    result = await action_upsert_der_status(resolved_params, step, context)

    # Assert
    assert result.done()
    assert mock_submit_and_refetch.call_count == num_devices

    # Verify all resources were stored
    stored_statuses = resource_store.get_for_type(CSIPAusResource.DERStatus)
    assert len(stored_statuses) == num_devices

    # Verify contents of first device
    first_status = stored_statuses[0].resource
    assert first_status.readingTime == expected_timestamp
    assert first_status.genConnectStatus == expected_gen_connect_status
    assert first_status.operationalModeStatus == expected_operational_mode_status
    assert first_status.alarmStatus == expected_alarm_status


@mock.patch("cactus_client.action.der.client_error_request_for_step")
@pytest.mark.asyncio
async def test_action_send_malformed_der_settings(
    mock_client_error_request: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test sending malformed DERSettings with updatedTime_missing"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create multiple DERs with DERSettingsLinks
    num_devices = 2
    for i in range(num_devices):
        der = generate_class_instance(DER, seed=i, generate_relationships=True)
        resource_store.append_resource(CSIPAusResource.DER, None, der)

    resolved_params = {"updatedTime_missing": True}

    # Act
    result = await action_send_malformed_der_settings(resolved_params, step, context)

    # Assert
    assert result.done()
    assert mock_client_error_request.call_count == num_devices

    # Verify the correct endpoints were called
    calls = mock_client_error_request.call_args_list
    for call in calls:
        assert call[0][0] == step
        assert call[0][1] == context
        assert call[0][3] == HTTPMethod.PUT
        xml_payload = call[0][4]

        # Verify updatedTime was removed
        assert "<updatedTime>" not in xml_payload, "updatedTime should be missing"
