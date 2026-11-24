import re
from datetime import UTC, datetime, timedelta
from http import HTTPMethod, HTTPStatus
from unittest import mock

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.sep2.function_set_assignments import FunctionSetAssignmentsResponse
from envoy_schema.server.schema.sep2.identification import Link
from envoy_schema.server.schema.sep2.metering import (
    MeterReading,
    MeterReadingListResponse,
    UsagePoint,
    UsagePointListResponse,
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
from freezegun import freeze_time

from cactus_client.action.mup import (
    action_insert_readings,
    action_upsert_mup,
    calculate_reading_time,
    value_to_sep2,
)
from cactus_client.check.mup import (
    check_mirror_usage_point,
    generate_mmr_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.error import CactusClientError
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult
from tests.unit.cactus_client.action.test_server import (
    RouteBehaviour,
    TestingAppRoute,
    create_test_session,
)
from tests.unit.cactus_client.check.test_mup import create_discovered_usage_point


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
        mmr1_mrid = generate_mmr_mrids(mrid, [CSIPAusReadingType.ActivePowerAverage], client_config.pen)
        mmr2_mrid = generate_mmr_mrids(mrid, [CSIPAusReadingType.ReactivePowerInstantaneous], client_config.pen)

        mmr1 = generate_class_instance(
            MirrorMeterReading,
            mRID=mmr1_mrid[CSIPAusReadingType.ActivePowerAverage],
            readingType=generate_class_instance(ReadingType, powerOfTenMultiplier=-2),
        )
        mmr2 = generate_class_instance(
            MirrorMeterReading,
            mRID=mmr2_mrid[CSIPAusReadingType.ReactivePowerInstantaneous],
            readingType=generate_class_instance(ReadingType, powerOfTenMultiplier=-2),
        )

        inserted_mup = generate_class_instance(
            MirrorUsagePoint,
            mRID=mrid,
            href=f"/mup/{mrid}",
            mirrorMeterReadings=[mmr1, mmr2],
        )
        mock_submit.return_value = inserted_mup

        resolved_params = {
            "mup_id": "test-mup-1",
            "location": CSIPAusReadingLocation.Device,
            "reading_types": [
                CSIPAusReadingType.ActivePowerAverage,
                CSIPAusReadingType.ReactivePowerInstantaneous,
            ],
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
            if mmr.readingType is not None
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
        assert stored_mups[0].resource.mirrorMeterReadings == [mmr1, mmr2]  # From the server, not our request
        assert context.resource_annotations(step, stored_mups[0].id).pow10_by_mrid == {
            mmr.mRID: -2 for mmr in sent_request.mirrorMeterReadings
        }
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
    mmr_mrids = generate_mmr_mrids(mup_mrid, [CSIPAusReadingType.ActivePowerAverage], client_config.pen)
    mmr_mrid = next(iter(mmr_mrids.values()))

    reading_type = generate_class_instance(ReadingType, powerOfTenMultiplier=pow10_multiplier)
    mmr = generate_class_instance(MirrorMeterReading, mRID=mmr_mrid, readingType=reading_type)
    upserted_mup = generate_class_instance(
        MirrorUsagePoint,
        mRID=mup_mrid,
        href=f"/mup/{mup_mrid}",
        postRate=post_rate,
        mirrorMeterReadings=[mmr],
    )

    # Store the MUP with alias
    sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, upserted_mup)
    context.resource_annotations(step, sr.id).alias = "test-mup-1"
    context.resource_annotations(step, sr.id).pow10_by_mrid = {mmr_mrid: pow10_multiplier}

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
            assert result.not_before and result.not_before >= expected_next_time

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
        assert reading.timePeriod
        assert reading.timePeriod.start == expected_timestamp
        assert reading.timePeriod.duration == post_rate


@freeze_time("2025-01-01 12:00:00")
@pytest.mark.parametrize("post_rate", [15, 30, 60, 120])
@pytest.mark.asyncio
async def test_action_insert_readings_minimum_wait_respects_server_post_rate(
    post_rate, aiohttp_client, testing_contexts_factory
):
    """When the server sets a postRate on a MUP, the minimum wait between readings should use that"""

    frozen_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

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
        mmr_mrids = generate_mmr_mrids(mup_mrid, [CSIPAusReadingType.ActivePowerAverage], client_config.pen)
        mmr_mrid = next(iter(mmr_mrids.values()))
        reading_type = generate_class_instance(ReadingType, powerOfTenMultiplier=0)
        mmr = generate_class_instance(MirrorMeterReading, mRID=mmr_mrid, readingType=reading_type)
        mup = generate_class_instance(
            MirrorUsagePoint,
            mRID=mup_mrid,
            href="/mup/test",
            postRate=post_rate,
            mirrorMeterReadings=[mmr],
        )

        sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
        context.resource_annotations(step, sr.id).alias = "test-mup"
        context.resource_annotations(step, sr.id).pow10_by_mrid = {mmr_mrid: 0}

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


@pytest.mark.parametrize(
    "has_mmrs, has_upt, server_pow10, has_reading_type, expect_warning",
    [
        (True, False, -1, True, False),  # Server agrees with our requested pow10
        (True, False, 1, True, True),  # Server disagrees - warn but still send with our requested pow10
        (True, False, None, True, True),  # Missing powerOfTenMultiplier is 0 as per 2030.5 - disagrees
        (True, False, 1, False, False),  # No ReadingType reported - nothing to compare against
        (False, True, -1, True, False),  # Server doesn't expose MMRs - compares against /upt
        (False, True, 1, True, True),
        (False, False, 1, True, False),  # Neither /mup nor /upt - nothing to compare against
    ],
)
@pytest.mark.asyncio
async def test_action_insert_readings_requested_pow10(
    has_mmrs: bool,
    has_upt: bool,
    server_pow10: int | None,
    has_reading_type: bool,
    expect_warning: bool,
    testing_contexts_factory,
):
    """Readings are always encoded with the pow10 requested at upsert-mup. A different server reported pow10 (via /mup
    or /upt) only raises a warning"""

    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    mup_mrid = "ABC123456789012345678901TESTPEN1"
    mmr_mrid = generate_mmr_mrids(mup_mrid, [CSIPAusReadingType.ActivePowerAverage], client_config.pen)[
        CSIPAusReadingType.ActivePowerAverage
    ]
    reading_type = generate_class_instance(ReadingType, powerOfTenMultiplier=server_pow10) if has_reading_type else None
    mmrs = [generate_class_instance(MirrorMeterReading, mRID=mmr_mrid, readingType=reading_type)] if has_mmrs else None
    mup = generate_class_instance(
        MirrorUsagePoint, mRID=mup_mrid, href=f"/mup/{mup_mrid}", postRate=60, mirrorMeterReadings=mmrs
    )
    sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
    context.resource_annotations(step, sr.id).alias = "test-mup"
    context.resource_annotations(step, sr.id).pow10_by_mrid = {mmr_mrid: -1}
    if has_upt:
        create_discovered_usage_point(
            resource_store, mup_mrid, [(CSIPAusReadingType.ActivePowerAverage if has_reading_type else None, mmr_mrid)]
        )
        for rt_sr in resource_store.get_for_type(CSIPAusResource.ReadingType):
            rt_sr.resource.powerOfTenMultiplier = server_pow10

    resolved_params = {"mup_id": "test-mup", "values": {CSIPAusReadingType.ActivePowerAverage: 12.3}}
    with mock.patch("cactus_client.action.mup.request_for_step") as mock_request:
        mock_response = mock.Mock()
        mock_response.is_success.return_value = True
        mock_request.return_value = mock_response

        await action_insert_readings(resolved_params, step, context)

    sent_request = MirrorMeterReadingListRequest.from_xml(mock_request.call_args[0][4])
    assert sent_request.mirrorMeterReadings is not None
    assert [(m.mRID, m.reading.value if m.reading else None) for m in sent_request.mirrorMeterReadings] == [
        (mmr_mrid, 123)
    ]
    assert len(context.warnings.warnings) == (1 if expect_warning else 0)


@pytest.mark.asyncio
async def test_action_insert_readings_no_requested_pow10(testing_contexts_factory):
    """A MUP that wasn't created via upsert-mup (no requested pow10) can't have readings encoded"""

    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    mup = generate_class_instance(MirrorUsagePoint, href="/mup/1", postRate=60)
    sr = resource_store.upsert_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
    context.resource_annotations(step, sr.id).alias = "test-mup"

    resolved_params = {"mup_id": "test-mup", "values": {CSIPAusReadingType.ActivePowerAverage: 12.3}}
    with mock.patch("cactus_client.action.mup.request_for_step") as mock_request:
        with pytest.raises(CactusClientError, match="Couldn't find the pow10 multiplier"):
            await action_insert_readings(resolved_params, step, context)
        mock_request.assert_not_called()


class FakeUptServer:
    """Simulates a server that doesn't expose MirrorMeterReadings via GET /mup. If serve_upt, the readings of each
    upserted MUP are instead exposed under a UsagePoint (/upt) with the same mRIDs."""

    def __init__(self, serve_upt: bool):
        self.serve_upt = serve_upt
        self.mups: dict[str, MirrorUsagePointRequest] = {}  # Keyed by mRID

    async def submit(self, type_, step, context, method, href, request: MirrorUsagePointRequest):
        self.mups[request.mRID] = request
        return generate_class_instance(
            MirrorUsagePoint,
            href=f"/mup/{request.mRID}",
            mRID=request.mRID,
            roleFlags=request.roleFlags,
            postRate=None,
            mirrorMeterReadings=None,
        )

    def _upt_resources(self) -> tuple[dict, dict]:
        """Returns (resources_by_href, list_items_by_href) for the current server state"""
        resources: dict = {"/upt": generate_class_instance(UsagePointListResponse, href="/upt")}
        items: dict = {"/upt": []}
        for i, mup in enumerate(self.mups.values()):
            up_href = f"/upt/{i}"
            items["/upt"].append(
                generate_class_instance(
                    UsagePoint, href=up_href, mRID=mup.mRID, MeterReadingListLink=Link(href=f"{up_href}/mr")
                )
            )
            resources[f"{up_href}/mr"] = generate_class_instance(MeterReadingListResponse, href=f"{up_href}/mr")
            items[f"{up_href}/mr"] = []
            for j, mmr in enumerate(mup.mirrorMeterReadings or []):
                mr_href = f"{up_href}/mr/{j}"
                items[f"{up_href}/mr"].append(
                    generate_class_instance(
                        MeterReading, href=mr_href, mRID=mmr.mRID, ReadingTypeLink=Link(href=f"{mr_href}/rt")
                    )
                )
                assert mmr.readingType is not None
                resources[f"{mr_href}/rt"] = mmr.readingType.model_copy(update={"href": f"{mr_href}/rt"})
        return resources, items

    async def get(self, type_, step, context, href):
        return self._upt_resources()[0][href]

    async def paginate(self, type_, step, context, list_href, page_size, item_callback):
        return self._upt_resources()[1][list_href]


@pytest.mark.parametrize("serve_upt", [True, False])
@pytest.mark.asyncio
async def test_action_upsert_mup_upt_walk(serve_upt: bool, testing_contexts_factory):
    """upsert-mup refreshes the UsagePoint tree so that the following check validates the server's state. A re-walk
    replaces (rather than appends to) the previous UsagePoint tree. Servers with neither MMRs nor /upt fail the check"""

    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)
    resource_store.append_resource(
        CSIPAusResource.MirrorUsagePointList, None, generate_class_instance(MirrorUsagePointListResponse, href="/mup")
    )
    fsa = generate_class_instance(
        FunctionSetAssignmentsResponse,
        href="/fsa/1",
        UsagePointListLink=Link(href="/upt") if serve_upt else None,
    )
    resource_store.append_resource(CSIPAusResource.FunctionSetAssignments, None, fsa)

    server = FakeUptServer(serve_upt)
    mup_mrid = "AAAAAAAAAAAAAAAAAAAAAAA000000001"

    def upsert_and_check(reading_types: list[CSIPAusReadingType], check_reading_types: list[CSIPAusReadingType]):
        async def run():
            await action_upsert_mup(
                {
                    "mup_id": "mup",
                    "location": CSIPAusReadingLocation.Site,
                    "set_mup_mrid": mup_mrid,
                    "reading_types": reading_types,
                },
                step,
                context,
            )
            return check_mirror_usage_point(
                {
                    "matches": True,
                    "location": CSIPAusReadingLocation.Site,
                    "check_mup_mrid": mup_mrid,
                    "reading_types": check_reading_types,
                },
                step,
                context,
            )

        return run()

    with (
        mock.patch("cactus_client.action.mup.submit_and_refetch_resource_for_step", side_effect=server.submit),
        mock.patch("cactus_client.action.discovery.get_resource_for_step", side_effect=server.get),
        mock.patch("cactus_client.action.discovery.paginate_list_resource_items", side_effect=server.paginate),
    ):
        apa = CSIPAusReadingType.ActivePowerAverage
        va = CSIPAusReadingType.VoltageSinglePhaseAverage

        result = await upsert_and_check([apa], [apa])
        assert result.passed is serve_upt, result.description

        # Update the MUP with a different reading type - only the new type should be stored / matched
        result = await upsert_and_check([va], [apa])
        assert result.passed is False, result.description
        result = await upsert_and_check([va], [va])
        assert result.passed is serve_upt, result.description

    if serve_upt:
        assert len(resource_store.get_for_type(CSIPAusResource.UsagePoint)) == 1
        assert len(resource_store.get_for_type(CSIPAusResource.MeterReading)) == 1
        assert len(resource_store.get_for_type(CSIPAusResource.ReadingType)) == 1
    else:
        assert resource_store.get_for_type(CSIPAusResource.UsagePoint) == []
        assert "(no MirrorMeterReadings)" in result.description
