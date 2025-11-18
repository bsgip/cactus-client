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
from envoy_schema.server.schema.sep2.response import Response
from envoy_schema.server.schema.sep2.der import (
    DER,
    DERCapability,
    DERType,
    ActivePower,
    DERSettings,
    DERStatus,
    DERControlResponse,
    DERControlType,
    DOESupportedMode,
    ConnectStatusTypeValue,
    OperationalModeStatusTypeValue,
    OperationalModeStatusType,
    DERControlListResponse,
    DERControlBase,
)

from cactus_client.schema.validator import to_hex32, to_hex8
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def _validate_fields(expected: Any, actual: Any, fields: list[str]) -> None:
    """Validate that specified fields match between expected and actual objects.

    Args:
        expected: Object with expected values (e.g. the request)
        actual: Object with actual values (e.g. the response)
        fields: List of field names to validate
    """
    mismatches = []

    for field_name in fields:
        expected_value = getattr(expected, field_name)
        actual_value = getattr(actual, field_name)

        if expected_value != actual_value:
            mismatches.append(f"{field_name}: expected {expected_value}, got {actual_value}")

    if mismatches:
        raise CactusClientException(f"{actual.__class__.__name__} validation failed: " + "; ".join(mismatches))


async def action_upsert_der_capability(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    type_ = DERType(int(resolved_parameters["type"]))
    rtgMaxW = ActivePower(value=resolved_parameters["rtgMaxW"], multiplier=0)
    modesSupported = to_hex32(int(resolved_parameters["modesSupported"]))
    doeModesSupported = to_hex8(int(resolved_parameters["doeModesSupported"]))

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        dercap_link = cast(DER, der.resource).DERCapabilityLink

        if dercap_link is None:
            raise CactusClientException(
                f"Expected every DER to have a DERCapabilityLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        dercap_request = DERCapability(
            type_=type_, rtgMaxW=rtgMaxW, modesSupported=modesSupported, doeModesSupported=doeModesSupported
        )
        dercap_xml = resource_to_sep2_xml(dercap_request)

        # Send request then retreive it from the server and save to resource store
        inserted_dercap = await submit_and_refetch_resource_for_step(
            DERCapability, step, context, HTTPMethod.PUT, str(dercap_link), dercap_xml, no_location_header=True
        )

        resource_store.upsert_resource(CSIPAusResource.DERCapability, der, inserted_dercap)

        # Validate the inserted resource keeps the values we set
        _validate_fields(dercap_request, inserted_dercap, ["type_", "rtgMaxW", "modesSupported", "doeModesSupported"])

    return ActionResult.done()


async def action_upsert_der_settings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    updatedTime = int(utc_now().timestamp())
    setMaxW = ActivePower(value=int(resolved_parameters["setMaxW"]), multiplier=0)
    setGradW = int(resolved_parameters["setGradW"])
    modesEnabled = to_hex32(int(resolved_parameters["modesEnabled"]))
    doeModesEnabled = to_hex8(int(resolved_parameters["doeModesEnabled"]))

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        der_sett_link = cast(DER, der.resource).DERSettingsLink

        if der_sett_link is None:
            raise CactusClientException(
                f"Expected every DER to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
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
            der_settings_request,
            inserted_der_settings,
            ["updatedTime", "setMaxW", "setGradW", "modesEnabled", "doeModesEnabled"],
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
        ConnectStatusTypeValue(value=to_hex8(int(gen_connect_val)), dateTime=current_timestamp)
        if gen_connect_val is not None
        else None
    )
    operationalModeStatus = (
        OperationalModeStatusTypeValue(value=OperationalModeStatusType(int(op_mode_val)), dateTime=current_timestamp)
        if op_mode_val is not None
        else None
    )
    alarmStatus = to_hex8(int(alarm_val)) if alarm_val is not None else None

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER)]
    for der in stored_der:
        der_status_link = cast(DER, der.resource).DERStatusLink

        if der_status_link is None:
            raise CactusClientException(
                f"Expected every DER to have a DERStatusLink, but didnt find one for device {der.resource.href}."
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
                der_status_request,
                inserted_der_status,
                ["readingTime", "genConnectStatus", "operationalModeStatus", "alarmStatus"],
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
        modesEnabled=to_hex32(DERControlType.OP_MOD_ENERGIZE),
        doeModesEnabled=to_hex8(DOESupportedMode.OP_MOD_EXPORT_LIMIT_W),
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
                f"Expected every DER to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
            )

        # Send request (expecting rejection) - make the request and check for a client error
        await client_error_request_for_step(step, context, str(der_sett_link), HTTPMethod.PUT, der_settings_xml)

    return ActionResult.done()


async def action_respond_der_controls(step: StepExecution, context: ExecutionContext) -> ActionResult:
    """Enumerates all known DERControls and sends a Response for any that require it."""

    resource_store = context.discovered_resources(step)
    stored_der_controls = [sr for sr in resource_store.get(CSIPAusResource.DERControl)]

    # Check all DERControls, look at the context alias to see which ones have already been responded to, see if any need to be responded to
    # Check The eventststus to see what code to set
    # Send the Response type

    for der_ctl in stored_der_controls:
        der_control = cast(DERControlResponse, der_ctl.resource)

        reply = der_control.replyTo
        res = der_control.responseRequired # # both must be set (check/give error?) or NULL, but not one raise error or warning? (add warning to context_)


        # Send the response
        der_control_xml = resource_to_sep2_xml(der_control)

        inserted_der_control = await submit_and_refetch_resource_for_step(
            Response, step, context, HTTPMethod.PUT # Check if post?
            , reply, der_control_xml, no_location_header=True
        )

        resource_store.upsert_resource(CSIPAusResource.DERControl, der_ctl, inserted_der_control)

    return ActionResult.done()


# async def action_send_malformed_response(
#     resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
# ) -> ActionResult:
#     """
#     Sends a malformed DER Control List Response - expects a failure response.

#     Parameters:
#         mrid_unknown: include an mRID for a DERControl that does not exist
#         endDeviceLFDI_unknown: include an LFDI for an EndDevice that does not exist.
#         response_invalid: post back control response = 15 (reserved).
#     """

#     # Extract resolved params
#     mrid_unknown: bool = resolved_parameters["mrid_unknown"]
#     endDeviceLFDI_unknown: bool = resolved_parameters["endDeviceLFDI_unknown"]
#     response_invalid: bool = resolved_parameters["response_invalid"]

#     # Find ALL devices with which to make these settings
#     resource_store = context.discovered_resources(step)
#     stored_der_controls = [sr for sr in resource_store.get(CSIPAusResource.DERControl)]


#     # Apply the malformed params where applicable
#     mrid = to_hex32(6432) if mrid_unknown else
#     lfdi =

#     # Create the DERControlList to send
#     der_control_response = DERControlResponse()
#     der_control_list = DERControlListResponse(all_=1, results=1, DERControl=[der_control_response])

#     return ActionResult.done()
