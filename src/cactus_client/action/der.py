import logging
from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.action.server import (
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.check.end_device import match_end_device_on_lfdi_caseless
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from envoy_schema.server.schema.sep2.der import DER, DERCapability, DERType, ActivePower, DERSettings
from envoy_schema.server.schema.sep2.types import TimeType

from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


async def action_upsert_der_capability(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    # params are all mandatory
    der_type: int = int(resolved_parameters["type"])
    rtgMaxW: int = int(resolved_parameters["rtgMaxW"])
    modesSupported: int = int(resolved_parameters["modesSupported"])
    doeModesSupported: int = int(resolved_parameters["doeModesSupported"])

    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Find the DER to upsert
    parent_edev = match_end_device_on_lfdi_caseless(resource_store, client_config.lfdi)
    if parent_edev is None:
        raise CactusClientException(f"Expected an already discovered EndDevice with LFDI {client_config.lfdi}.")

    # Find the single DER for this EndDevice
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER) if sr.resource.href and sr.parent == parent_edev]

    if len(stored_der) != 1:
        raise CactusClientException(
            f"Expected exactly 1 DER for EndDevice {parent_edev.resource.href}, found {len(stored_der)}."
        )

    der_sr = cast(DER, stored_der[0].resource)

    # Get the DERCapabilityLink. If not present we will create one.
    dcap_link = der_sr.DERCapabilityLink
    href = dcap_link.href if dcap_link and dcap_link.href else parent_edev.resource.href + "/der/1/dercap"

    # Create the DERCapability request with the provided parameters
    dcap_request = DERCapability(
        type_=DERType(der_type),
        rtgMaxW=ActivePower(value=rtgMaxW, multiplier=0),
        modesSupported=f"{modesSupported:032x}",  # 32 bit hex
        doeModesSupported=f"{doeModesSupported:08x}",  # 8 bit hex
    )

    dcap_xml = resource_to_sep2_xml(dcap_request)

    # Upsert the DERCapability
    inserted_dcap = await submit_and_refetch_resource_for_step(
        DERCapability, step, context, HTTPMethod.PUT, href, dcap_xml
    )

    resource_store.upsert_resource(CSIPAusResource.DERCapability, der_sr, inserted_dcap)

    return ActionResult.done()


async def action_upsert_der_settings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:

    # params are all mandatory
    setMaxW: int = int(resolved_parameters["setMaxW"])
    setGradW: int = int(resolved_parameters["setGradW"])
    modesEnabled: int = int(resolved_parameters["modesEnabled"])
    doeModesEnabled: int = int(resolved_parameters["doeModesEnabled"])

    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Find the DER to upsert
    parent_edev = match_end_device_on_lfdi_caseless(resource_store, client_config.lfdi)
    if parent_edev is None:
        raise CactusClientException(f"Expected an already discovered EndDevice with LFDI {client_config.lfdi}.")

    # Find the single DER for this EndDevice
    stored_der = [sr for sr in resource_store.get(CSIPAusResource.DER) if sr.resource.href and sr.parent == parent_edev]

    if len(stored_der) != 1:
        raise CactusClientException(
            f"Expected exactly 1 DER for EndDevice {parent_edev.resource.href}, found {len(stored_der)}."
        )

    der_sr = cast(DER, stored_der[0].resource)

    # Get the DERSettingsLink. If not present we will create one.
    der_sett_link = der_sr.DERSettingsLink
    href = der_sett_link.href if der_sett_link and der_sett_link.href else parent_edev.resource.href + "/der/1/dercap"

    # Create the DERCapability request with the provided parameters
    dcap_request = DERSettings(
        updatedTime=utc_now().timestamp()),
        setMaxW=ActivePower(value=setMaxW, multiplier=0),
        setGradW=setGradW,
        modesEnabled=f"{modesEnabled:032x}",  # 32 bit hex
        doeModesEnabled=f"{doeModesEnabled:08x}",  # 8 bit hex
    )

    dcap_xml = resource_to_sep2_xml(dcap_request)

    # Upsert the DERCapability
    inserted_dcap = await submit_and_refetch_resource_for_step(
        DERCapability, step, context, HTTPMethod.PUT, href, dcap_xml
    )

    resource_store.upsert_resource(CSIPAusResource.DERCapability, der_sr, inserted_dcap)

    return ActionResult.done()
