import logging
import re
from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DER,
    ActivePower,
    ConnectStatusTypeValue,
    DERCapability,
    DERControlType,
    DERSettings,
    DERStatus,
    DERType,
    DOESupportedMode,
    OperationalModeStatusType,
    OperationalModeStatusTypeValue,
)

from cactus_client.action.server import (
    client_error_request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.error import CactusClientError
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.schema.validator import to_hex_binary
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def _validate_fields(expected: Any, actual: Any, fields: list[str]) -> None:  # noqa: ANN401
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
        raise CactusClientError(f"{actual.__class__.__name__} validation failed: " + "; ".join(mismatches))


async def action_upsert_der_capability(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    type_ = DERType(int(resolved_parameters["type"]))
    rtg_max_w = ActivePower(value=resolved_parameters["rtgMaxW"], multiplier=0)
    modes_supported = to_hex_binary(int(resolved_parameters["modesSupported"]))
    doe_modes_supported = to_hex_binary(int(resolved_parameters["doeModesSupported"]))

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get_for_type(CSIPAusResource.DER)]
    for der in stored_der:
        dercap_link = cast(DER, der.resource).DERCapabilityLink

        if dercap_link is None:
            raise CactusClientError(
                f"Expected every DER to have a DERCapabilityLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        dercap_request = DERCapability(
            type_=type_, rtgMaxW=rtg_max_w, modesSupported=modes_supported, doeModesSupported=doe_modes_supported
        )

        # Send request then retreive it from the server and save to resource store
        inserted_dercap = await submit_and_refetch_resource_for_step(
            DERCapability, step, context, HTTPMethod.PUT, dercap_link.href, dercap_request, no_location_header=True
        )

        resource_store.upsert_resource(CSIPAusResource.DERCapability, der.id.parent_id(), inserted_dercap)

        # Validate the inserted resource keeps the values we set
        _validate_fields(dercap_request, inserted_dercap, ["type_", "rtgMaxW", "modesSupported", "doeModesSupported"])

    return ActionResult.done()


async def action_upsert_der_settings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    resource_store = context.discovered_resources(step)

    # Extract and convert parameters
    updated_time = int(utc_now().timestamp())
    set_max_w = ActivePower(value=int(resolved_parameters["setMaxW"]), multiplier=0)
    set_grad_w = int(resolved_parameters["setGradW"])
    modes_enabled = to_hex_binary(int(resolved_parameters["modesEnabled"]))
    doe_modes_enabled = to_hex_binary(int(resolved_parameters["doeModesEnabled"]))

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get_for_type(CSIPAusResource.DER)]
    for der in stored_der:
        der_sett_link = cast(DER, der.resource).DERSettingsLink

        if der_sett_link is None:
            raise CactusClientError(
                f"Expected every DER to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        der_settings_request = DERSettings(
            updatedTime=updated_time,
            setMaxW=set_max_w,
            setGradW=set_grad_w,
            modesEnabled=modes_enabled,
            doeModesEnabled=doe_modes_enabled,
        )

        # Send request then retrieve it from the server and save to resource store
        inserted_der_settings = await submit_and_refetch_resource_for_step(
            DERSettings,
            step,
            context,
            HTTPMethod.PUT,
            der_sett_link.href,
            der_settings_request,
            no_location_header=True,
        )

        resource_store.upsert_resource(CSIPAusResource.DERSettings, der.id.parent_id(), inserted_der_settings)

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
    gen_connect_status = (
        ConnectStatusTypeValue(value=to_hex_binary(int(gen_connect_val)), dateTime=current_timestamp)
        if gen_connect_val is not None
        else None
    )
    operational_mode_status = (
        OperationalModeStatusTypeValue(value=OperationalModeStatusType(int(op_mode_val)), dateTime=current_timestamp)
        if op_mode_val is not None
        else None
    )
    alarm_status = to_hex_binary(int(alarm_val)) if alarm_val is not None else None

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get_for_type(CSIPAusResource.DER)]
    for der in stored_der:
        der_status_link = cast(DER, der.resource).DERStatusLink

        if der_status_link is None:
            raise CactusClientError(
                f"Expected every DER to have a DERStatusLink, but didnt find one for device {der.resource.href}."
            )

        # Build the upsert request
        der_status_request = DERStatus(
            readingTime=current_timestamp,
            genConnectStatus=gen_connect_status,
            operationalModeStatus=operational_mode_status,
            alarmStatus=alarm_status,
        )

        if expect_rejection:
            # If we're expecting rejection - make the request and check for a client error
            await client_error_request_for_step(
                step, context, der_status_link.href, HTTPMethod.PUT, resource_to_sep2_xml(der_status_request)
            )
        else:
            inserted_der_status = await submit_and_refetch_resource_for_step(
                DERStatus,
                step,
                context,
                HTTPMethod.PUT,
                der_status_link.href,
                der_status_request,
                no_location_header=True,
            )

            resource_store.upsert_resource(CSIPAusResource.DERStatus, der.id.parent_id(), inserted_der_status)

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
    """Sends a malformed DERSettings - missing updatedTime"""

    resource_store = context.discovered_resources(step)
    updated_time_missing: bool = resolved_parameters["updatedTime_missing"]

    # Create a compliant DERSettings first
    der_settings_request = DERSettings(
        updatedTime=int(utc_now().timestamp()),
        setMaxW=ActivePower(value=5005, multiplier=0),  # Doesnt matter what values as it should be rejected,
        setGradW=50,
        modesEnabled=to_hex_binary(DERControlType.OP_MOD_ENERGIZE),
        doeModesEnabled=to_hex_binary(DOESupportedMode.OP_MOD_EXPORT_LIMIT_W),
    )

    der_settings_xml = resource_to_sep2_xml(der_settings_request)

    # Go and change the compliant XML depending on the resolved_parameters
    if updated_time_missing:
        # Remove the entire <updatedTime>...</updatedTime> element
        der_settings_xml = re.sub(r"<updatedTime>.*?</updatedTime>", "", der_settings_xml)

    # Loop through and upsert the resource for EVERY device
    stored_der = [sr for sr in resource_store.get_for_type(CSIPAusResource.DER)]
    for der in stored_der:
        der_sett_link = cast(DER, der.resource).DERSettingsLink

        if der_sett_link is None:
            raise CactusClientError(
                f"Expected every DER to have a DERSettingsLink, but didnt find one for device {der.resource.href}."
            )

        # Send request (expecting rejection) - make the request and check for a client error
        await client_error_request_for_step(step, context, der_sett_link.href, HTTPMethod.PUT, der_settings_xml)

    return ActionResult.done()
