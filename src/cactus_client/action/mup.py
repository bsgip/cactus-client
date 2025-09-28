import logging
from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.csip_aus.connection_point import (
    ConnectionPointRequest,
    ConnectionPointResponse,
)
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceRequest,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.metering import ReadingType
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorUsagePointRequest,
)
from envoy_schema.server.schema.sep2.types import (
    DeviceCategory,
    FlowDirectionType,
    ReasonCodeType,
)

from cactus_client.action.server import (
    request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.check.end_device import match_end_device_on_lfdi_caseless
from cactus_client.check.mup import (
    generate_mup_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.error import CactusClientException, RequestException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution

logger = logging.getLogger(__name__)


def generate_mup_request(
    step: StepExecution,
    context: ExecutionContext,
    location: CSIPAusReadingLocation,
    reading_types: list[CSIPAusReadingType],
    mmr_mrids: list[str] | None,
    pow10_multiplier: int,
) -> MirrorUsagePointRequest:

    client_config = context.client_config(step)
    mrids = generate_mup_mrids(location, reading_types, mmr_mrids, client_config)
    role_flags = generate_role_flags(location)

    mmrs: list[MirrorMeterReading] = []
    for rt in reading_types:
        (uom, kind, dq) = generate_reading_type_values(rt)
        mmr_mrid = mrids.mmr_mrids[rt]

        mmrs.append(
            MirrorMeterReading(
                mrid=mmr_mrid,
                readingType=ReadingType(
                    uom=uom,
                    kind=kind,
                    dataQualifier=dq,
                    flowDirection=FlowDirectionType.FORWARD,
                    powerOfTenMultiplier=pow10_multiplier,
                ),
            )
        )

    return MirrorUsagePointRequest(
        roleFlags=f"{int(role_flags):04X}",
        deviceLFDI=client_config.lfdi,
        mRID=mrids.mup_mrid,
        status=1,
        mirrorMeterReadings=mmrs,
    )


async def action_upsert_mup(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Inserts or creates MirrorUsage point with the specified parameters"""

    mup_id: str = resolved_parameters["mup_id"]  # mandatory param
    location: CSIPAusReadingLocation = resolved_parameters["location"]  # mandatory param
    reading_types: list[CSIPAusReadingType] = resolved_parameters["reading_types"]  # mandatory param
    expect_rejection: bool = resolved_parameters.get("expect_rejection", False)
    mmr_mrids: list[str] | None = resolved_parameters.get("mmr_mrids", None)
    pow10_multiplier: int = resolved_parameters.get("pow10_multiplier", 0)

    resource_store = context.discovered_resources(step)
    mup_list_resources = resource_store.get(CSIPAusResource.MirrorUsagePointList)

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
