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
    "received_at, time_response, expected",
    [
        (NOW, None, False),
        (
            NOW,
            generate_class_instance(TimeResponse, currentTime=int(NOW.timestamp()), tzOffset=0, dstOffset=0),
            True,
        ),  # Perfect match
        (
            NOW,
            generate_class_instance(
                TimeResponse, currentTime=int(NOW.timestamp()) + 1800, tzOffset=1000, dstOffset=800
            ),
            True,
        ),  # Perfect match
        (
            NOW,
            generate_class_instance(
                TimeResponse, currentTime=int(NOW.timestamp()) - 1100, tzOffset=-500, dstOffset=-600
            ),
            True,
        ),  # Perfect match (negative offsets)
        (
            NOW,
            generate_class_instance(
                TimeResponse,
                currentTime=int(NOW.timestamp()) + 1800 + MAX_TIME_DRIFT_SECONDS,
                tzOffset=1000,
                dstOffset=800,
            ),
            True,
        ),  # Within drift range
        (
            NOW,
            generate_class_instance(
                TimeResponse,
                currentTime=int(NOW.timestamp()) + 1800 - MAX_TIME_DRIFT_SECONDS,
                tzOffset=1000,
                dstOffset=800,
            ),
            True,
        ),  # Within drift range
        (
            NOW,
            generate_class_instance(
                TimeResponse,
                currentTime=int(NOW.timestamp()) + 1800 - MAX_TIME_DRIFT_SECONDS - 1,
                tzOffset=1000,
                dstOffset=800,
            ),
            False,
        ),  # Outside drift range
        (
            NOW,
            generate_class_instance(
                TimeResponse,
                currentTime=int(NOW.timestamp()) + 1800 + MAX_TIME_DRIFT_SECONDS + 1,
                tzOffset=1000,
                dstOffset=800,
            ),
            False,
        ),  # Outside drift range
    ],
)
def test_check_time_synced(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    received_at: datetime,
    time_response: TimeResponse | None,
    expected: bool,
):
    """Tests the various components of TimeResponse with check_time_synced"""
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    if time_response is not None:
        with freeze_time(received_at):  # Force received_at to be the time we want
            store.append_resource(CSIPAusResource.Time, None, time_response)

    # utc_now should NOT be involved here - it's all off the received_at time
    result = check_time_synced(step, context)

    assert_check_result(result, expected)
