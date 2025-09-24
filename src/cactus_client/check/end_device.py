from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceResponse,
    RegistrationResponse,
)

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import ResourceStore, StoredResource


def match_end_device_on_lfdi_caseless(resource_store: ResourceStore, lfdi: str) -> StoredResource | None:
    """Does a very lightweight match on EndDevice.lfdi - returning the first EndDevice that matches or None"""
    end_devices = resource_store.get(CSIPAusResource.EndDevice)
    if not end_devices:
        return None

    lfdi_folded = lfdi.casefold()
    for edev in end_devices:
        edev_resource = cast(EndDeviceResponse, edev.resource)

        if edev_resource.lFDI is not None and (edev_resource.lFDI.casefold() == lfdi_folded):
            return edev

    return None


def check_end_device(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified EndDevice's in the resource store match the check criteria"""

    matches: bool = resolved_parameters["matches_client"]  # This can be a positive or negative test
    check_pin: bool = resolved_parameters.get("matches_pin", False)

    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Start by finding a loose candidate match - then we can drill into the specifics
    matched_edev = match_end_device_on_lfdi_caseless(resource_store, client_config.lfdi)
    if matched_edev is None:
        if matches is True:
            return CheckResult(False, f"Expected to find an EndDevice with lfdi {client_config.lfdi} but got none.")
        else:
            return CheckResult(True, None)  # We wanted none - we found none

    edev = cast(EndDeviceResponse, matched_edev.resource)
    if matches is False:
        return CheckResult(False, f"Expected to find NO EndDevice with lfdi {client_config.lfdi} but found {edev.href}")

    # At this point - we are just asserting that the matched_edev is ACTUALLY a proper match

    # If we are optionally doing a PIN check - perform it now
    if check_pin:
        matched_registrations = resource_store.get_descendents_of(CSIPAusResource.Registration, matched_edev)
        if not matched_registrations:
            return CheckResult(False, f"{edev.href} doesn't have any Registrations associated with it")
        for registration in matched_registrations:
            actual_pin = cast(RegistrationResponse, registration.resource).pIN
            if actual_pin != client_config.pin:
                return CheckResult(
                    False, f"{edev.href} has a Registration with with PIN {actual_pin} but expected {client_config.pin}"
                )

    # Check for more specifics
    if edev.lFDI is not None and (edev.lFDI.upper() != edev.lFDI):
        context.warnings.log_step_warning(step, f"Expected an uppercase LFDI - received {edev.lFDI}")
    if edev.sFDI != client_config.sfdi:
        context.warnings.log_step_warning(
            step,
            f"SFDI mismatch on EndDevice {edev.href} Expected {client_config.sfdi} but got {edev.sFDI}",
        )

    return CheckResult(True, None)
