from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.time import TimeResponse

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution

# We will accept a "desync" in time up to this value
# This will need to compensate for transmission / processing time delays so we are being pretty generous
MAX_TIME_DRIFT_SECONDS = 5


def check_time_synced(step: StepExecution, context: ExecutionContext) -> CheckResult:
    resource_store = context.discovered_resources(step)

    time_resources = resource_store.get(CSIPAusResource.Time)
    if not time_resources:
        return CheckResult(False, "Couldn't find a discovered Time response.")

    for sr in time_resources:
        time_response = cast(TimeResponse, sr.resource)
        local_time_seconds = time_response.currentTime  # Seconds since Unix Epoch

        # Local time zone offset from currentTime. Does not include any daylight savings time offsets.
        # For American time zones, a negative tzOffset SHALL be used (eg, EST = GMT-5 which is -18000).
        tz_offset_seconds = time_response.tzOffset

        # Daylight savings time offset from local standard time. A typical practice is advancing clocks one hour
        # when daylight savings time is in effect, which would result in a positive dstOffset.
        dst_offset_seconds = time_response.dstOffset

        utc_equivalent_seconds = local_time_seconds - tz_offset_seconds - dst_offset_seconds
        time_received_seconds = int(sr.created_at.timestamp())

        drift_seconds = utc_equivalent_seconds - time_received_seconds
        if abs(drift_seconds) > MAX_TIME_DRIFT_SECONDS:
            return CheckResult(
                False, f"Time drift calculated to be {drift_seconds}s. Expected a max of {MAX_TIME_DRIFT_SECONDS}s"
            )

    return CheckResult(True, None)
