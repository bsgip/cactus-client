import unittest.mock as mock
from datetime import datetime, timezone
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.time import TimeResponse
from freezegun import freeze_time

from cactus_client.check.time import MAX_TIME_DRIFT_SECONDS, check_time_synced
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution

NOW = datetime(2001, 4, 6, 1, 2, 3, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "received_at, current_time_utc, local_time, tz_offset, dst_offset, expected",
    [
        # No time response found
        (NOW, None, None, None, None, False),
        # currentTime only (localTime not provided)
        (NOW, int(NOW.timestamp()), None, 0, 0, True),  # Perfect
        (NOW, int(NOW.timestamp()) + MAX_TIME_DRIFT_SECONDS, None, 0, 0, True),  # Within drift range
        (NOW, int(NOW.timestamp()) - MAX_TIME_DRIFT_SECONDS, None, 0, 0, True),  # Within drift range (negative)
        (NOW, int(NOW.timestamp()) - MAX_TIME_DRIFT_SECONDS - 1, None, 0, 0, False),  # Outside drift range
        (NOW, int(NOW.timestamp()) + MAX_TIME_DRIFT_SECONDS + 1, None, 0, 0, False),  # Outside drift range
        # With localTime - both currentTime and localTime must pass
        (
            NOW,
            int(NOW.timestamp()),  # currentTime perfect
            int(NOW.timestamp()) + 1800,  # localTime offset by tzOffset + dstOffset
            1000,
            800,
            True,
        ),  # Perfect with offsets
        (
            NOW,
            int(NOW.timestamp()),  # currentTime perfect
            int(NOW.timestamp()) - 1100,  # localTime offset negative
            -500,
            -600,
            True,
        ),  # Perfect with negative offsets
        (
            NOW,
            int(NOW.timestamp()),  # currentTime passes
            int(NOW.timestamp()) + 1800 + MAX_TIME_DRIFT_SECONDS + 1,  # localTime fails
            1000,
            800,
            False,
        ),  # currentTime OK but localTime outside drift
        (
            NOW,
            int(NOW.timestamp() + MAX_TIME_DRIFT_SECONDS + 1),  # currentTime fails
            int(NOW.timestamp()) + 1800,  # localTime would pass but not called
            1000,
            800,
            False,
        ),  # localTime ok but currentTime outside drift (reverse)
    ],
)
def test_check_time_synced(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    received_at: datetime,
    current_time_utc: int | None,
    local_time: int | None,
    tz_offset: int | None,
    dst_offset: int | None,
    expected: bool,
):
    """Tests the various components of TimeResponse with check_time_synced including both currentTime and localTime.

    - currentTime is always in UTC
    - localTime (if present) is in device local timezone and converted to UTC
    """
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    if current_time_utc is not None:
        time_response = generate_class_instance(
            TimeResponse,
            currentTime=current_time_utc,
            localTime=local_time,
            tzOffset=tz_offset if tz_offset is not None else 0,
            dstOffset=dst_offset if dst_offset is not None else 0,
        )

        with freeze_time(received_at):  # Force received_at to be the time we want
            store.append_resource(CSIPAusResource.Time, None, time_response)

    result = check_time_synced(step, context)

    assert_check_result(result, expected)
