import logging
from http import HTTPMethod
import re
from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.action.server import (
    client_error_request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from envoy_schema.server.schema.sep2.der import (
    DER,
    DERCapability,
    DERType,
    ActivePower,
    DERSettings,
    DERStatus,
    DERControlType,
    DOESupportedMode,
    ConnectStatusTypeValue,
    OperationalModeStatusTypeValue,
    OperationalModeStatusType,
)

from cactus_client.schema.validator import to_hex_binary
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def _validate_fields(expected: dict[str, Any], actual: Any) -> None:
    """Validate that resource fields match expected values. Build the error explanations if not

    Args:
        expected: Dict of field_name -> expected_value.
                  Fields with None values are skipped (useful for optional fields).
        actual: The actual resource object to validate
    """
    mismatches = []

    for field_name, expected_value in expected.items():
        if expected_value is None:
            continue

        actual_value = getattr(actual, field_name, None)

        if actual_value != expected_value:
            mismatches.append(f"{field_name} expected {expected_value}, received {actual_value}")

    if mismatches:
        raise CactusClientException(
            f"Expected all {actual.__class__.__name__} fields to match. " + "; ".join(mismatches) + "."
        )


async def action_upsert_der_capability(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    type_ = DERType(int(resolved_parameters["type"]))
    rtgMaxW = ActivePower(value=resolved_parameters["rtgMaxW"], multiplier=0)
    modesSupported = to_hex_binary(int(resolved_parameters["modesSupported"]), 32)
    doeModesSupported = to_hex_binary(int(resolved_parameters["doeModesSupported"]), 8)

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        dcap_link = cast(DER, der.resource).DERCapabilityLink

        if dcap_link is None:
            raise CactusClientException(
                f"Expected every der to have a DERCapabilityLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        dcap_request = DERCapability(
            type_=type_, rtgMaxW=rtgMaxW, modesSupported=modesSupported, doeModesSupported=doeModesSupported
        )
        dcap_xml = resource_to_sep2_xml(dcap_request)

        # Send request then retreive it from the server and save to resource store
        inserted_dcap = await submit_and_refetch_resource_for_step(
            DERCapability, step, context, HTTPMethod.PUT, str(dcap_link), dcap_xml, no_location_header=True
        )

        resource_store.upsert_resource(CSIPAusResource.DERCapability, der, inserted_dcap)

        # Validate the inserted resource keeps the values we set
        _validate_fields(
            {
                "type_": type_,
                "rtgMaxW": rtgMaxW,
                "modesSupported": modesSupported,
                "doeModesSupported": doeModesSupported,
            },
            inserted_dcap,
        )

    return ActionResult.done()


async def action_upsert_der_settings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    updatedTime = int(utc_now().timestamp())
    setMaxW = ActivePower(value=int(resolved_parameters["setMaxW"]), multiplier=0)
    setGradW = int(resolved_parameters["setGradW"])
    modesEnabled = to_hex_binary(int(resolved_parameters["modesEnabled"]), 32)
    doeModesEnabled = to_hex_binary(int(resolved_parameters["doeModesEnabled"]), 8)

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        der_sett_link = cast(DER, der.resource).DERSettingsLink

        if der_sett_link is None:
            raise CactusClientException(
                f"Expected every der to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        der_settings_request = DERSettings(
            updatedTime=updatedTime,
            setMaxW=setMaxW,
            setGradW=setGradW,
            modesEnabled=modesEnabled,
            doeModesEnabled=doeModesEnabled,
        )
        der_settings_xml = resource_to_sep2_xml(der_settings_request)

        # Send request then retrieve it from the server and save to resource store
        inserted_der_settings = await submit_and_refetch_resource_for_step(
            DERSettings, step, context, HTTPMethod.PUT, str(der_sett_link), der_settings_xml, no_location_header=True
        )

        resource_store.upsert_resource(CSIPAusResource.DERSettings, der, inserted_der_settings)

        # Validate the inserted resource keeps the values we set
        _validate_fields(
            {
                "updatedTime": updatedTime,
                "setMaxW": setMaxW,
                "setGradW": setGradW,
                "modesEnabled": modesEnabled,
                "doeModesEnabled": doeModesEnabled,
            },
            inserted_der_settings,
        )

    return ActionResult.done()


async def action_upsert_der_status(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)
    expect_rejection = resolved_parameters.get("expect_rejection", False)
    current_timestamp = int(utc_now().timestamp())

    # Extract and convert parameters
    gen_connect_val = resolved_parameters.get("genConnectStatus")
    op_mode_val = resolved_parameters.get("operationalModeStatus")
    alarm_val = resolved_parameters.get("alarmStatus")

    # Build status objects
    genConnectStatus = (
        ConnectStatusTypeValue(value=to_hex_binary(int(gen_connect_val), 8), dateTime=current_timestamp)
        if gen_connect_val is not None
        else None
    )
    operationalModeStatus = (
        OperationalModeStatusTypeValue(value=OperationalModeStatusType(int(op_mode_val)), dateTime=current_timestamp)
        if op_mode_val is not None
        else None
    )
    alarmStatus = to_hex_binary(int(alarm_val), 8) if alarm_val is not None else None

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        der_status_link = cast(DER, der.resource).DERStatusLink

        if der_status_link is None:
            raise CactusClientException(
                f"Expected every der to have a DERStatusLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        der_status_request = DERStatus(
            readingTime=current_timestamp,
            genConnectStatus=genConnectStatus,
            operationalModeStatus=operationalModeStatus,
            alarmStatus=alarmStatus,
        )
        der_status_xml = resource_to_sep2_xml(der_status_request)

        if expect_rejection:
            # If we're expecting rejection - make the request and check for a client error
            await client_error_request_for_step(step, context, str(der_status_link), HTTPMethod.PUT, der_status_xml)
        else:
            inserted_der_status = await submit_and_refetch_resource_for_step(
                DERStatus, step, context, HTTPMethod.PUT, str(der_status_link), der_status_xml, no_location_header=True
            )

            resource_store.upsert_resource(CSIPAusResource.DERStatus, der, inserted_der_status)

            # Validate the inserted resource keeps the values we set
            _validate_fields(
                {
                    "readingTime": current_timestamp,
                    "genConnectStatus": genConnectStatus,
                    "operationalModeStatus": operationalModeStatus,
                    "alarmStatus": alarmStatus,
                },
                inserted_der_status,
            )

    return ActionResult.done()


async def action_send_malformed_der_settings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Sends a malformed DERSettings - expects a failure and that the server will NOT change anything"""

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    updatedTime_missing: bool = resolved_parameters["updatedTime_missing"]
    modesEnabled_int: bool = resolved_parameters["modesEnabled_int"]

    # Quick sanity check
    if not updatedTime_missing and not modesEnabled_int:
        raise CactusClientException("""Expected either updatedTime_missing or modesEnabled_int to be true.""")

    # Create a compliant DERSettings first
    der_settings_request = DERSettings(
        updatedTime=int(utc_now().timestamp()),
        setMaxW=ActivePower(value=5005, multiplier=0),  # Doesnt matter what values as it should be rejected,
        setGradW=50,
        modesEnabled=to_hex_binary(DERControlType.OP_MOD_ENERGIZE, 32),
        doeModesEnabled=to_hex_binary(DOESupportedMode.OP_MOD_EXPORT_LIMIT_W, 8),
    )

    der_settings_xml = resource_to_sep2_xml(der_settings_request)

    # Go and change the compliant XML depending on the resolved_parameters
    if updatedTime_missing:
        # Remove the entire <updatedTime>...</updatedTime> element
        der_settings_xml = re.sub(r"<updatedTime>.*?</updatedTime>", "", der_settings_xml)

    if modesEnabled_int:
        # Replace the modesEnabled hex bitmap with an integer
        der_settings_xml = re.sub(
            r"<modesEnabled>.*?</modesEnabled>", r"<modesEnabled>8</modesEnabled>", der_settings_xml
        )

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        der_sett_link = cast(DER, der.resource).DERSettingsLink

        if der_sett_link is None:
            raise CactusClientException(
                f"Expected every der to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
            )

        # Send request (expecting rejection) - make the request and check for a client error
        await client_error_request_for_step(step, context, str(der_sett_link), HTTPMethod.PUT, der_settings_xml)

    return ActionResult.done()
