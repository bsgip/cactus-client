from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceRequest,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.types import DeviceCategory

from cactus_client.action.server import (
    request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution


def generate_end_device_request(
    step: StepExecution, context: ExecutionContext, force_lfdi: str | None
) -> EndDeviceRequest:
    client_config = context.client_config(step)
    deviceCategory = f"{DeviceCategory.PHOTOVOLTAIC_SYSTEM.value:02X}"

    return EndDeviceRequest(
        postRate=60,
        lFDI=force_lfdi if force_lfdi else client_config.lfdi,
        sFDI=client_config.sfdi,
        deviceCategory=deviceCategory,
    )


async def action_insert_end_device(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Inserts an EndDevice and then resolves the Location header, updating resource stores along the way"""

    force_lfdi: str | None = resolved_parameters.get("force_lfdi", None)
    expect_rejection: bool = resolved_parameters.get("expect_rejection", False)

    resource_store = context.discovered_resources(step)
    edev_list_resources = resource_store.get(CSIPAusResource.EndDeviceList)

    list_edevs = [sr for sr in edev_list_resources if sr.resource.href]
    if len(list_edevs) != 1:
        raise CactusClientException(
            f"Expected only a single {CSIPAusResource.EndDeviceList} href but found {len(list_edevs)}."
        )

    list_href = cast(str, list_edevs[0].resource.href)  # This will be set due to the earlier filter
    edev_xml = resource_to_sep2_xml(generate_end_device_request(step, context, force_lfdi))
    if expect_rejection:
        # If we're expecting rejection - make the request and check for a client error
        response = await request_for_step(step, context, list_href, HTTPMethod.POST, edev_xml)
        if not response.is_client_error():
            raise CactusClientException(
                f"Expected a 4XX error when executing POST {list_href} but got {response.status}."
            )
    else:
        # Otherwise insert and refetch the returned EndDevice
        inserted_edev = await submit_and_refetch_resource_for_step(
            EndDeviceResponse, step, context, HTTPMethod.POST, list_href, edev_xml
        )

        resource_store.upsert_resource(CSIPAusResource.EndDevice, list_edevs[0], inserted_edev)
    return ActionResult.done()
