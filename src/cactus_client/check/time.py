from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.time import TimeResponse

from cactus_client.constants import MAX_TIME_DRIFT_SECONDS
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


def check_time_synced(step: StepExecution, context: ExecutionContext) -> CheckResult:
    resource_store = context.discovered_resources(step)

    time_resources = resource_store.get_for_type(CSIPAusResource.Time)
    if not time_resources:
        return CheckResult(False, "Couldn't find a discovered Time response.")

    for sr in time_resources:
        time_response = cast(TimeResponse, sr.resource)
        time_received_utc = int(sr.created_at.timestamp())  # When we received the response (UTC timestamp)

        # Check 1: Verify currentTime (already in UTC)
        current_time_utc = time_response.currentTime

        drift_seconds = current_time_utc - time_received_utc
        if abs(drift_seconds) > MAX_TIME_DRIFT_SECONDS:
            return CheckResult(
                False,
                f"Time drift on currentTime is {drift_seconds}s. Expected a max of {MAX_TIME_DRIFT_SECONDS}s",
            )

        # Check 2: Verify localTime (if present) in device's local timezone - needs conversion to UTC
        if time_response.localTime is not None:
            local_time = time_response.localTime

            # tzOffset: timezone offset from UTC.
            # For American time zones, a negative tzOffset SHALL be used (eg, EST = GMT-5 which is -18000).
            tz_offset_seconds = time_response.tzOffset

            # Daylight savings time offset from local standard time. A typical practice is advancing clocks one hour
            # when daylight savings time is in effect, which would result in a positive dstOffset.
            dst_offset_seconds = time_response.dstOffset

            # Convert local time to UTC
            local_time_as_utc = local_time - tz_offset_seconds - dst_offset_seconds

            local_drift_seconds = local_time_as_utc - time_received_utc
            if abs(local_drift_seconds) > MAX_TIME_DRIFT_SECONDS:
                return CheckResult(
                    False,
                    f"Time drift on localTime is {local_drift_seconds}s. Expected a max of {MAX_TIME_DRIFT_SECONDS}s",
                )

    return CheckResult(True, None)


def check_poll_rate(resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext) -> CheckResult:
    """Checks whether the nominated resource has the specified poll rate"""

    resource_type: CSIPAusResource = resolved_parameters["resource"]
    poll_rate_seconds: int = resolved_parameters["poll_rate_seconds"]

    resource_store = context.discovered_resources(step)
    resources = resource_store.get_for_type(resource_type)

    # Check poll rate for each resource (should only be one though)
    for sr in resources:
        if not hasattr(sr.resource, "pollRate"):
            return CheckResult(
                False,
                f"{resource_type} resource at {sr.resource.href} does not have a pollRate attribute",
            )

        actual_poll_rate = sr.resource.pollRate

        if actual_poll_rate != poll_rate_seconds:
            return CheckResult(
                False,
                f"{resource_type} pollRate mismatch: expected {poll_rate_seconds}s, got {actual_poll_rate}s",
            )

    # All checks passed
    return CheckResult(True, None)
