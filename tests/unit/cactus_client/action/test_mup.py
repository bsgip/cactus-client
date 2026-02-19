import re
from datetime import datetime, timedelta, timezone
from http import HTTPMethod, HTTPStatus
from unittest import mock

from freezegun import freeze_time

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorMeterReadingListRequest,
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
    MirrorUsagePointRequest,
    ReadingType,
)
from envoy_schema.server.schema.sep2.types import FlowDirectionType, ServiceKind

from cactus_client.action.mup import (
    action_insert_readings,
    action_upsert_mup,
    calculate_reading_time,
    value_to_sep2,
)
from cactus_client.check.mup import (
    generate_mmr_mrid,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult
from tests.unit.cactus_client.action.test_server import RouteBehaviour, TestingAppRoute, create_test_session


def assert_mrid(mrid: str, pen: int):
    assert isinstance(mrid, str)
    assert len(mrid) == 32
    assert mrid.endswith(str(pen))
    assert re.search(r"[^A-F0-9]", mrid) is None, "Should only be uppercase hex chars"


@pytest.mark.parametrize(
    "v, pow10, expected",
    [
        (0, 0, 0),
        (10.3, 0, 10),
        (821.2, 1, 82),
        (4731.3, 3, 4),
        (4731.3, -1, 47313),
        (4731.3, -2, 473130),
    ],
)
def test_value_to_sep2(v: float, pow10: int, expected: int):
    actual = value_to_sep2(v, pow10)
    assert isinstance(actual, int)
    assert actual == expected
    assert value_to_sep2(-v, pow10) == -expected


@pytest.mark.parametrize(
    "post_rate_seconds,repeat_number,expected_offset_seconds",
    [
        (60, 0, 0),  # First reading at base time
        (60, 1, 60),  # Second reading 60s later
        (60, 5, 300),  # Sixth reading 300s later
        (300, 0, 0),  # First reading at base time
        (300, 1, 300),  # Second reading 300s later
        (300, 3, 900),  # Fourth reading 900s later
    ],
)
def test_calculate_reading_time(post_rate_seconds, repeat_number, expected_offset_seconds, testing_contexts_factory):

    context, _ = testing_contexts_factory(mock.Mock())

    base_time = datetime(2025, 1, 1, 10, 30, 45, 123456)
    context.created_at = base_time

    # ACT
    result = calculate_reading_time(context, post_rate_seconds, repeat_number)

    # Assert: should be base time with seconds/microseconds zeroed, plus offset
    expected_time = base_time.replace(second=0, microsecond=0) + timedelta(seconds=expected_offset_seconds)
    assert result == expected_time


@pytest.mark.asyncio
async def test_action_upsert_mup(testing_contexts_factory):
    """Test that action_upsert_mup creates a valid MUP request with correct structure and data"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Add a MirrorUsagePointList
    mup_list = generate_class_instance(MirrorUsagePointListResponse, href="/mup")
    resource_store.append_resource(CSIPAusResource.MirrorUsagePointList, None, mup_list)

    with mock.patch("cactus_client.action.mup.submit_and_refetch_resource_for_step") as mock_submit:
        mrid = "ABC123456789012345678901TESTPEN1"
        mmr1_mrid = generate_mmr_mrid(mrid, CSIPAusReadingType.ActivePowerAverage, client_config.pen)
        mmr2_mrid = generate_mmr_mrid(mrid, CSIPAusReadingType.ReactivePowerInstantaneous, client_config.pen)

        mmr1 = generate_class_instance(
            MirrorMeterReading,
            mRID=mmr1_mrid,
            readingType=generate_class_instance(ReadingType, powerOfTenMultiplier=-2),
        )
        mmr2 = generate_class_instance(
            MirrorMeterReading,
            mRID=mmr2_mrid,
            readingType=generate_class_instance(ReadingType, powerOfTenMultiplier=-2),
        )

        inserted_mup = generate_class_instance(
            MirrorUsagePoint, mRID=mrid, href=f"/mup/{mrid}", mirrorMeterReadings=[mmr1, mmr2]
        )
        mock_submit.return_value = inserted_mup

        resolved_params = {
            "mup_id": "test-mup-1",
            "location": CSIPAusReadingLocation.Device,
            "reading_types": [CSIPAusReadingType.ActivePowerAverage, CSIPAusReadingType.ReactivePowerInstantaneous],
            "pow10_multiplier": -2,
        }

        # Act
        result = await action_upsert_mup(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.repeat is False

        # Verify submit_and_refetch_resource_for_step was called correctly
        mock_submit.assert_called_once()
        call_args = mock_submit.call_args
        assert call_args[0][0] == MirrorUsagePoint
        assert call_args[0][4] == "/mup"

        # Check the actual MUP request body that was sent
        sent_request: MirrorUsagePointRequest = call_args[0][5]

        assert sent_request.deviceLFDI == client_config.lfdi
        assert sent_request.status == 1
        assert sent_request.serviceCategoryKind == ServiceKind.ELECTRICITY

        expected_role_flags = generate_role_flags(CSIPAusReadingLocation.Device)
        assert sent_request.roleFlags == f"{int(expected_role_flags):04X}"

        assert_mrid(sent_request.mRID, client_config.pen)

        assert sent_request.mirrorMeterReadings is not None
        assert len(sent_request.mirrorMeterReadings) == 2  # Two reading types requested

        for mmr in sent_request.mirrorMeterReadings:
            assert_mrid(mmr.mRID, client_config.pen)
            assert mmr.readingType is not None
            assert mmr.readingType.powerOfTenMultiplier == -2  # From params
            assert mmr.readingType.flowDirection == FlowDirectionType.FORWARD

            # Verify the reading type values matches expected types
            found = False
            for rt in resolved_params["reading_types"]:
                expected_uom, expected_kind, expected_dq = generate_reading_type_values(rt)
                if (
                    mmr.readingType.uom == expected_uom
                    and mmr.readingType.kind == expected_kind
                    and mmr.readingType.dataQualifier == expected_dq
                ):
                    found = True
                    break
            assert found

        # Verify all expected reading types are present
        sent_reading_type_tuples = [
            (mmr.readingType.uom, mmr.readingType.kind, mmr.readingType.dataQualifier)
            for mmr in sent_request.mirrorMeterReadings
        ]
        expected_reading_type_tuples = [generate_reading_type_values(rt) for rt in resolved_params["reading_types"]]
        assert set(sent_reading_type_tuples) == set(expected_reading_type_tuples)

        # Verify the MUP was stored in the resource store with the correct alias
        stored_mups = [
            sr
            for sr in resource_store.get_for_type(CSIPAusResource.MirrorUsagePoint)
            if context.resource_annotations(step, sr.id).alias == "test-mup-1"
        ]
        assert len(stored_mups) == 1
        assert stored_mups[0].resource.mRID == inserted_mup.mRID
        assert stored_mups[0].resource.href == inserted_mup.href


@pytest.mark.parametrize(
    "pow10_multiplier, list_values, repeat_number, expected_value, expected_repeat",
    [
        (0, [100.5, 200.3, 300.7], 0, 100.5, True),
        (-1, [100.5, 200.3, 300.7], 1, 200.3, True),
        (0, [100.5, 200.3, 300.7], 2, 300.7, False),
        (0, [100.5], 0, 100.5, False),
        (0, 100.6, 0, 100.6, False),
        (0, 100.7, 99, 100.7, False),
        (-1, 100.7, 99, 100.7, False),
        (1, 100.7, 99, 100.7, False),
    ],
)
@pytest.mark.asyncio
async def test_action_insert_readings(
    pow10_multiplier: int,
    list_values: list[float] | float,
    repeat_number: int,
    expected_value: float,
    expected_repeat: bool,
    testing_contexts_factory,
):
    """Test that action_insert_readings correctly generates and submits reading data"""

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    post_rate = 60
    step.repeat_number = repeat_number
    base_time = calculate_reading_time(context, post_rate, repeat_number=0)

    context.created_at = base_time
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Create a MUP with MirrorMeterReadings
    mup_mrid = "ABC123456789012345678901TESTPEN1"
    mmr_mrid = generate_mmr_mrid(mup_mrid, CSIPAusReadingType.ActivePowerAverage, client_config.pen)

    reading_type = generate_class_instance(ReadingType, powerOfTenMultiplier=pow10_multiplier)
    mmr = generate_class_instance(MirrorMeterReading, mRID=mmr_mrid, readingType=reading_type)
    upserted_mup = generate_class_instance(
        MirrorUsagePoint, mRID=mup_mrid, href=f"/mup/{mup_mrid}", postRate=post_rate, mirrorMeterReadings=[mmr]
    )

    # Store the MUP with alias
    sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, upserted_mup)
    context.resource_annotations(step, sr.id).alias = "test-mup-1"

    # Mock the server request
    with mock.patch("cactus_client.action.mup.request_for_step") as mock_request:
        mock_response = mock.Mock()
        mock_response.is_success.return_value = True
        mock_request.return_value = mock_response

        resolved_params = {
            "mup_id": "test-mup-1",
            "values": {CSIPAusReadingType.ActivePowerAverage: list_values},
        }

        # Act
        result = await action_insert_readings(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.repeat is expected_repeat

        expected_next_time = base_time.replace(second=0, microsecond=0) + timedelta(seconds=60)
        # Should be either the expected time or later (if minimum wait)
        if expected_repeat:
            assert result.not_before >= expected_next_time

        # Verify request
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert call_args[0][2] == f"/mup/{mup_mrid}"

        # Check the actual reading data XML
        sent_xml = call_args[0][4]
        sent_request = MirrorMeterReadingListRequest.from_xml(sent_xml)
        assert sent_request.mirrorMeterReadings is not None
        assert len(sent_request.mirrorMeterReadings) == 1

        sent_mmr = sent_request.mirrorMeterReadings[0]
        assert sent_mmr.mRID == mmr_mrid
        assert sent_mmr.reading is not None

        reading = sent_mmr.reading
        assert reading.value == value_to_sep2(expected_value, pow10_multiplier)

        expected_timestamp = int(base_time.replace(second=0, microsecond=0).timestamp()) + repeat_number * post_rate
        assert reading.timePeriod.start == expected_timestamp
        assert reading.timePeriod.duration == post_rate


@freeze_time("2025-01-01 12:00:00")
@pytest.mark.parametrize("post_rate", [15, 30, 60, 120])
@pytest.mark.asyncio
async def test_action_insert_readings_minimum_wait_respects_server_post_rate(
    post_rate, aiohttp_client, testing_contexts_factory
):
    """When the server sets a postRate on a MUP, the minimum wait between readings should use that"""

    frozen_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Arrange
    post_route = TestingAppRoute(HTTPMethod.POST, "/mup/test", [RouteBehaviour(HTTPStatus.CREATED, b"", {})])
    async with create_test_session(aiohttp_client, [post_route]) as session:
        context, step = testing_contexts_factory(session)
        step.repeat_number = 0
        context.created_at = frozen_now

        resource_store = context.discovered_resources(step)
        client_config = context.client_config(step)

        # Create a MUP with the server-assigned postRate
        mup_mrid = "ABC123456789012345678901TESTPEN1"
        mmr_mrid = generate_mmr_mrid(mup_mrid, CSIPAusReadingType.ActivePowerAverage, client_config.pen)
        reading_type = generate_class_instance(ReadingType, powerOfTenMultiplier=0)
        mmr = generate_class_instance(MirrorMeterReading, mRID=mmr_mrid, readingType=reading_type)
        mup = generate_class_instance(
            MirrorUsagePoint, mRID=mup_mrid, href="/mup/test", postRate=post_rate, mirrorMeterReadings=[mmr]
        )

        sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
        context.resource_annotations(step, sr.id).alias = "test-mup"

        resolved_params = {
            "mup_id": "test-mup",
            "values": {CSIPAusReadingType.ActivePowerAverage: [100.0, 200.0]},
        }

        # Act
        result = await action_insert_readings(resolved_params, step, context)

    # Assert
    assert result.repeat is True
    assert result.not_before is not None

    # With frozen time: next_reading_time = frozen_now + post_rate = minimum_wait
    expected_not_before = frozen_now + timedelta(seconds=post_rate)
    assert result.not_before == expected_not_before
