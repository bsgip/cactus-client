import logging
from datetime import datetime, timedelta
from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.sep2.metering import Reading, ReadingType
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorMeterReadingListRequest,
    MirrorUsagePoint,
    MirrorUsagePointRequest,
)
from envoy_schema.server.schema.sep2.types import (
    DateTimeIntervalType,
    FlowDirectionType,
    ServiceKind,
)

from cactus_client.action.server import (
    client_error_request_for_step,
    request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.check.mup import (
    generate_mmr_mrid,
    generate_mup_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.error import CactusClientException, RequestException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def value_to_sep2(value: float, pow10: int) -> int:
    decimal_power = pow(10, -pow10)
    return int(value * decimal_power)


def generate_upsert_mup_request(
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
                mRID=mmr_mrid,
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
        serviceCategoryKind=ServiceKind.ELECTRICITY,
    )


def calculate_reading_time(context: ExecutionContext, post_rate_seconds: int, repeat_number: int) -> datetime:
    return context.created_at.replace(second=0, microsecond=0) + timedelta(seconds=post_rate_seconds * repeat_number)


def generate_insert_readings_request(
    step: StepExecution,
    context: ExecutionContext,
    mup_mrid: str,
    reading_values: dict[CSIPAusReadingType, list[float]],
    pow10_by_mrid: dict[str, int],
    post_rate_seconds: int,
) -> MirrorMeterReadingListRequest:

    client_config = context.client_config(step)

    # Base our readings relative to the start time of the test
    reading_time = calculate_reading_time(context, post_rate_seconds, step.repeat_number)

    mmrs: list[MirrorMeterReading] = []
    for rt, rt_values in reading_values.items():
        mmr_mrid = generate_mmr_mrid(mup_mrid, rt, client_config.pen)
        pow10 = pow10_by_mrid.get(mmr_mrid, None)
        if pow10 is None:
            logger.error(f"Couldn't find {mmr_mrid} in pow10_by_mrid: {pow10_by_mrid}")
            raise CactusClientException(f"Couldn't find the pow10 multiplier for MirrorMeterReading {mmr_mrid}")

        raw_value = rt_values[step.repeat_number]
        mmrs.append(
            MirrorMeterReading(
                mRID=mmr_mrid,
                reading=Reading(
                    value=value_to_sep2(raw_value, pow10),
                    timePeriod=DateTimeIntervalType(duration=post_rate_seconds, start=int(reading_time.timestamp())),
                ),
            )
        )

    return MirrorMeterReadingListRequest(mirrorMeterReadings=mmrs)


async def action_insert_readings(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    mup_id: str = resolved_parameters["mup_id"]  # mandatory param
    values: dict[CSIPAusReadingType, list[float]] = resolved_parameters["values"]
    expect_rejection: bool = resolved_parameters.get("expect_rejection", False)

    # sanity check our values are well formed
    all_value_lengths = [len(value_list) for value_list in values.values()]
    if len(set(all_value_lengths)) != 1:
        logger.error(f"values parameter is malformed. Not every length is the same: {values}")
        raise CactusClientException("The values parameters is malformed. This is a test definition error.")
    if step.repeat_number >= all_value_lengths[0]:
        logger.error(f"Too many repeats - at repeat {step.repeat_number} but only has {all_value_lengths[0]}")
        raise CactusClientException("The values parameters is malformed. This is a test definition error.")

    resource_store = context.discovered_resources(step)
    mups_with_id = [
        sr for sr in resource_store.get(CSIPAusResource.MirrorUsagePoint) if sr.resource.href and sr.alias == mup_id
    ]

    if len(mups_with_id) != 1:
        raise CactusClientException(
            f"Expected 1 {CSIPAusResource.MirrorUsagePoint} with alias {mup_id} but found {len(mups_with_id)}."
        )

    # We've got a parent mup - now we need to pull it apart to figure out what readings go where
    mup_sr = mups_with_id[0]
    mup = cast(MirrorUsagePoint, mup_sr.resource)
    mup_href = cast(str, mup.href)  # We know this is set from an earlier filter
    if not mup.mirrorMeterReadings:
        raise CactusClientException(
            f"{CSIPAusResource.MirrorUsagePoint} {mup_href} {mup.mRID} hasn't got any logged MirrorMeterReadings."
        )

    pow10_by_mrid: dict[str, int] = {}
    for mmr in mup.mirrorMeterReadings:
        if mmr.readingType is None:
            raise CactusClientException(
                f"MirrorUsagePoint {mup_href} {mup.mRID} has MirrorMeterReading {mmr.mRID} with no logged ReadingType"
            )
        if mmr.readingType.powerOfTenMultiplier is None:
            raise CactusClientException(
                f"MirrorUsagePoint {mup_href} {mup.mRID} has MirrorMeterReading {mmr.mRID} with no powerOfTenMultiplier"
            )
        pow10_by_mrid[mmr.mRID] = mmr.readingType.powerOfTenMultiplier

    # Now we are ready to submit readings
    post_rate_seconds = mup.postRate or 60
    mmr_list_xml = resource_to_sep2_xml(
        generate_insert_readings_request(step, context, mup.mRID, values, pow10_by_mrid, post_rate_seconds)
    )
    if expect_rejection:
        # If we're expecting rejection - make the request and check for a client error
        await client_error_request_for_step(step, context, mup_href, HTTPMethod.POST, mmr_list_xml)
    else:
        # Otherwise submit the readings
        response = await request_for_step(step, context, mup_href, HTTPMethod.POST, mmr_list_xml)
        if not response.is_success():
            raise RequestException(f"Received {response.status} from POST {mup_href} when submitting readings")

    # Repeat if we have more readings to send
    if step.repeat_number >= (all_value_lengths[0] - 1):
        return ActionResult.done()
    else:
        # If we get delayed (eg slow server or being blocked by a precondition) we don't want to send
        # all of our readings in a quick burst - we always want to have a minimum wait period
        next_reading_time = calculate_reading_time(context, post_rate_seconds, step.repeat_number + 1)
        minimum_wait = utc_now() + timedelta(seconds=60)
        return ActionResult(True, max(next_reading_time, minimum_wait))


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
    mup_list_resources = [sr for sr in resource_store.get(CSIPAusResource.MirrorUsagePointList) if sr.resource.href]

    if len(mup_list_resources) != 1:
        raise CactusClientException(
            f"Expected only a single {CSIPAusResource.MirrorUsagePointList} href but found {len(mup_list_resources)}."
        )

    list_href = cast(str, mup_list_resources[0].resource.href)  # This will be set due to the earlier filter
    mup_xml = resource_to_sep2_xml(
        generate_upsert_mup_request(step, context, location, reading_types, mmr_mrids, pow10_multiplier)
    )
    if expect_rejection:
        # If we're expecting rejection - make the request and check for a client error
        await client_error_request_for_step(step, context, list_href, HTTPMethod.POST, mup_xml)
    else:
        # Otherwise insert and refetch the returned EndDevice
        inserted_mup = await submit_and_refetch_resource_for_step(
            MirrorUsagePoint, step, context, HTTPMethod.POST, list_href, mup_xml
        )

        resource_store.upsert_resource(
            CSIPAusResource.MirrorUsagePoint, mup_list_resources[0], inserted_mup, alias=mup_id
        )
    return ActionResult.done()
