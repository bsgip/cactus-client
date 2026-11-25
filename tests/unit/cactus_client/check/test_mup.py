import re

import pytest
from aiohttp import ClientSession
from assertical.asserts.type import assert_dict_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.sep2.metering import (
    MeterReading,
    MeterReadingListResponse,
    UsagePoint,
)
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorUsagePoint,
    ReadingType,
)
from envoy_schema.server.schema.sep2.types import (
    DataQualifierType,
    KindType,
    RoleFlagsType,
    UomType,
)

from cactus_client.check.mup import (
    MirrorUsagePointMrids,
    check_mirror_usage_point,
    find_mrids_matching,
    find_mup_readings,
    generate_hashed_mrid,
    generate_mmr_mrids,
    generate_mup_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION
from cactus_client.error import CactusClientError
from cactus_client.model.config import ClientConfig
from cactus_client.model.resource import CSIPAusResourceTree, ResourceStore


def assert_mrid(mrid: str, pen: int | None):
    assert isinstance(mrid, str)
    assert len(mrid) == 32
    if pen is not None:
        assert mrid.endswith(str(pen))
    assert re.search(r"[^A-F0-9]", mrid) is None, "Should only be uppercase hex chars"


def assert_mup_mrids(
    m: MirrorUsagePointMrids,
    reading_types: list[CSIPAusReadingType],
    pen: int,
    check_mmr_pens: bool = True,
):
    assert isinstance(m, MirrorUsagePointMrids)
    assert_mrid(m.mup_mrid, pen)

    assert_dict_type(CSIPAusReadingType, str, m.mmr_mrids, len(reading_types))
    for rt in reading_types:
        assert_mrid(m.mmr_mrids[rt], pen if check_mmr_pens else None)


def assert_all_different(m1: MirrorUsagePointMrids, m2: MirrorUsagePointMrids):
    assert m1.mup_mrid != m2.mup_mrid

    for key, m1_val in m1.mmr_mrids.items():
        if key in m2.mmr_mrids:
            assert m1_val != m2.mmr_mrids[key]

    for key, m2_val in m2.mmr_mrids.items():
        if key in m1.mmr_mrids:
            assert m2_val != m1.mmr_mrids[key]

    assert set(m1.mmr_mrids.items()) != set(m2.mmr_mrids.items())


def test_generate_hashed_mrid():
    mrid1 = generate_hashed_mrid("", 1234)
    assert_mrid(mrid1, 1234)

    mrid2 = generate_hashed_mrid("", 12345)
    assert_mrid(mrid2, 12345)

    mrid3 = generate_hashed_mrid("seed value", 12345)
    assert_mrid(mrid3, 12345)

    mrid4 = generate_hashed_mrid("seed value", 12345678)
    assert_mrid(mrid4, 12345678)

    mrid4_dup = generate_hashed_mrid("seed value", 12345678)
    assert_mrid(mrid4_dup, 12345678)
    assert mrid4_dup == mrid4

    mrid5 = generate_hashed_mrid("seed value 2", 12345678)
    assert_mrid(mrid5, 12345678)

    all_unique_mrids = [mrid1, mrid2, mrid3, mrid4, mrid5]
    assert len(all_unique_mrids) == len(set(all_unique_mrids))


def test_generate_mup_mrids():
    cfg1 = generate_class_instance(ClientConfig, seed=101)
    cfg2 = generate_class_instance(ClientConfig, seed=202)

    rts_1 = [CSIPAusReadingType.ActivePowerMaximum, CSIPAusReadingType.FrequencyMaximum]
    rts_2 = [
        CSIPAusReadingType.ActivePowerMaximum,
        CSIPAusReadingType.ActivePowerMinimum,
    ]

    mup1 = generate_mup_mrids(CSIPAusReadingLocation.Device, rts_1, None, cfg1)
    assert_mup_mrids(mup1, rts_1, cfg1.pen)

    mup1_dup = generate_mup_mrids(CSIPAusReadingLocation.Device, rts_1, None, cfg1)
    assert_mup_mrids(mup1_dup, rts_1, cfg1.pen)
    assert mup1 == mup1_dup

    mup1_reversed = generate_mup_mrids(CSIPAusReadingLocation.Device, list(reversed(rts_1)), None, cfg1)
    assert_mup_mrids(mup1_reversed, rts_1, cfg1.pen)
    assert mup1 == mup1_reversed, "Should be invariant to the order they are specified"

    mup2 = generate_mup_mrids(CSIPAusReadingLocation.Device, rts_1, None, cfg2)
    assert_mup_mrids(mup2, rts_1, cfg2.pen)

    mup3 = generate_mup_mrids(CSIPAusReadingLocation.Device, rts_2, None, cfg1)
    assert_mup_mrids(mup3, rts_2, cfg1.pen)

    assert_all_different(mup1, mup2)
    assert_all_different(mup1, mup3)


def test_generate_reading_type_values_bad_value():
    with pytest.raises(CactusClientError):
        generate_reading_type_values("not a valid value")  # type: ignore


def test_generate_reading_type_values():
    all_values: list[tuple] = []
    for rt in CSIPAusReadingType:
        # Certain reading types are only applicable to certain CSIP-AUS versions
        if ACTIVE_CSIP_AUS_VERSION == CSIPAusVersion.RELEASE_1_2:
            if rt in {CSIPAusReadingType.StoredEnergyInstantaneous}:
                continue

        all_values.append(generate_reading_type_values(rt))

    assert len(all_values) == len(set(all_values)), "For catching copy paste errors"


def test_generate_role_flags_bad_value():
    with pytest.raises(CactusClientError):
        generate_role_flags("not a valid value")  # type: ignore


def test_generate_role_flags_values():
    all_values = []
    for loc in CSIPAusReadingLocation:
        all_values.append(generate_role_flags(loc))

    assert len(all_values) == len(set(all_values)), "For catching copy paste errors"


def test_generate_mmr_mrids_basic():
    """Test that generate_mmr_mrids produces a consistent 32-character MRID with PEN suffix if mmr mrids not
    specified"""
    mup_mrid = "ABC123456789012345678901234567890"
    rts = [CSIPAusReadingType.ActivePowerAverage]
    pen = 12345678

    result = generate_mmr_mrids(mup_mrid, rts, pen)

    assert isinstance(result, dict)
    assert list(result.keys()) == rts
    mrid = result[rts[0]]
    assert mrid.endswith(str(pen))

    # Deterministic:
    result2 = generate_mmr_mrids(mup_mrid, rts, pen)
    assert result == result2

    # Override case: explicit mrids are used as-is (truncated to 32 chars)
    override = ["012345678901234567890123ABCDEF01"]
    result3 = generate_mmr_mrids(mup_mrid, rts, pen, override)
    assert result3[rts[0]] == "012345678901234567890123ABCDEF01"

    # Error case: mismatched lengths raise an error
    with pytest.raises(CactusClientError):
        generate_mmr_mrids(mup_mrid, rts, pen, ["mrid1", "mrid2"])


def test_generate_reading_type_values_fuzzy_match():
    """Test that all combinations of units and qualifiers exist and are consistent"""

    units_to_uoms = {
        "ActivePower": UomType.REAL_POWER_WATT,
        "ReactivePower": UomType.REACTIVE_POWER_VAR,
        "Frequency": UomType.FREQUENCY_HZ,
        "VoltageSinglePhase": UomType.VOLTAGE,
    }
    qualifiers = {
        "Average": DataQualifierType.AVERAGE,
        "Instantaneous": DataQualifierType.STANDARD,
        "Maximum": DataQualifierType.MAXIMUM,
        "Minimum": DataQualifierType.MINIMUM,
    }

    for unit, expected_uom in units_to_uoms.items():
        for qualifier, expected_qualifier in qualifiers.items():
            enum_name = f"{unit}{qualifier}"
            rt = CSIPAusReadingType[enum_name]

            # Act
            uom, kind, data_qualifier = generate_reading_type_values(rt)

            # Assert
            assert isinstance(data_qualifier, DataQualifierType)
            assert data_qualifier == expected_qualifier

            assert isinstance(uom, UomType)
            assert uom == expected_uom

            assert kind == KindType.POWER


def create_mirror_usage_point(
    mrid: str,
    role_flags: RoleFlagsType,
    reading_types: list[tuple[CSIPAusReadingType, str]],  # [(reading_type, mmr_mrid), ...]
    post_rate: int = 300,
) -> MirrorUsagePoint:
    """Helper to create a MirrorUsagePoint with MirrorMeterReadings"""
    mmrs = []
    for rt, mmr_mrid in reading_types:
        uom, kind, data_qualifier = generate_reading_type_values(rt)
        mmr = generate_class_instance(
            MirrorMeterReading,
            mRID=mmr_mrid,
            readingType=ReadingType(uom=uom, kind=kind, dataQualifier=data_qualifier),
        )
        mmrs.append(mmr)

    return generate_class_instance(
        MirrorUsagePoint,
        mRID=mrid,
        href=f"/mup/{mrid}",
        roleFlags=role_flags,
        mirrorMeterReadings=mmrs,
        postRate=post_rate,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location,reading_types,post_rate",
    [
        (
            CSIPAusReadingLocation.Device,
            [
                CSIPAusReadingType.ActivePowerAverage,
                CSIPAusReadingType.ReactivePowerInstantaneous,
            ],
            60,
        ),
        (
            CSIPAusReadingLocation.Site,
            [CSIPAusReadingType.ActivePowerInstantaneous],
            30,
        ),
    ],
)
async def test_check_mirror_usage_point_full_chain(
    location,
    reading_types,
    post_rate,
    testing_contexts_factory,
):
    """Verify MRID/role generation and MUP matching work correctly end-to-end"""

    client_config = generate_class_instance(ClientConfig, id="test-client-123", pen=99887766)

    # Generate the MRIDs that should be used
    generated_mrids = generate_mup_mrids(location, reading_types, None, client_config)
    generated_role_flags = generate_role_flags(location)

    # Create MUP using generated values
    mup = create_mirror_usage_point(
        mrid=generated_mrids.mup_mrid,
        role_flags=generated_role_flags,
        reading_types=[(rt, generated_mrids.mmr_mrids[rt]) for rt in reading_types],
        post_rate=post_rate,
    )

    resource_store = ResourceStore(CSIPAusResourceTree())
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, mup)

    # Verify check finds the match
    async with ClientSession() as session:
        context, step = testing_contexts_factory(session)
        context.clients_by_alias[step.client_alias].client_config = client_config
        context.clients_by_alias[step.client_alias].discovered_resources = resource_store

        result = check_mirror_usage_point(
            resolved_parameters={
                "matches": True,
                "location": location,
                "reading_types": reading_types,
                "post_rate_seconds": post_rate,
            },
            step=step,
            context=context,
        )

        assert result.passed, f"Expected check to pass but got: {result.description}"


@pytest.mark.asyncio
async def test_check_mirror_usage_point_negative_cases(testing_contexts_factory):
    """Test check_mirror_usage_point fails appropriately (matches=False or matches=True but no match)"""

    client_config = generate_class_instance(ClientConfig, id="test-client", pen=99887766)

    # Generate MRIDs
    device_reading_types = [CSIPAusReadingType.ActivePowerAverage]
    generated_mrids = generate_mup_mrids(CSIPAusReadingLocation.Device, device_reading_types, None, client_config)

    device_mup = create_mirror_usage_point(
        mrid=generated_mrids.mup_mrid,
        role_flags=generate_role_flags(CSIPAusReadingLocation.Device),
        reading_types=[
            (
                device_reading_types[0],
                generated_mrids.mmr_mrids[device_reading_types[0]],
            )
        ],
        post_rate=60,
    )

    resource_store = ResourceStore(CSIPAusResourceTree())
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, device_mup)

    async with ClientSession() as session:
        context, step = testing_contexts_factory(session)
        context.clients_by_alias[step.client_alias].client_config = client_config
        context.clients_by_alias[step.client_alias].discovered_resources = resource_store

        # matches=False but MUP exists (should fail)
        result = check_mirror_usage_point(
            resolved_parameters={
                "matches": False,
                "location": CSIPAusReadingLocation.Device,
                "reading_types": device_reading_types,
            },
            step=step,
            context=context,
        )
        assert not result.passed, "matches=False but MUP exists, should fail"

        # matches=True but wrong location (should fail)
        result = check_mirror_usage_point(
            resolved_parameters={
                "matches": True,
                "location": CSIPAusReadingLocation.Site,  # Wrong location
                "reading_types": device_reading_types,
            },
            step=step,
            context=context,
        )
        assert not result.passed, "Wrong location should fail to find match"


@pytest.mark.asyncio
async def test_find_mrids_matching_filters(testing_contexts_factory):
    """Test core filtering logic in find_mrids_matching"""

    client_config = generate_class_instance(ClientConfig, id="test-client", pen=99887766)

    # Generate MRIDs for MUPs
    device_reading_types = [CSIPAusReadingType.ActivePowerAverage]
    device_mrids = generate_mup_mrids(CSIPAusReadingLocation.Device, device_reading_types, None, client_config)

    site_reading_types = [CSIPAusReadingType.ReactivePowerInstantaneous]
    site_mrids = generate_mup_mrids(CSIPAusReadingLocation.Site, site_reading_types, None, client_config)

    # Create two MUPs
    device_mup = create_mirror_usage_point(
        mrid=device_mrids.mup_mrid,
        role_flags=generate_role_flags(CSIPAusReadingLocation.Device),
        reading_types=[(device_reading_types[0], device_mrids.mmr_mrids[device_reading_types[0]])],
        post_rate=60,
    )

    site_mup = create_mirror_usage_point(
        mrid=site_mrids.mup_mrid,
        role_flags=generate_role_flags(CSIPAusReadingLocation.Site),
        reading_types=[(site_reading_types[0], site_mrids.mmr_mrids[site_reading_types[0]])],
        post_rate=300,
    )

    resource_store = ResourceStore(CSIPAusResourceTree())
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, device_mup)
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, site_mup)

    # Act/Assert

    # No filters returns both
    result = find_mrids_matching(resource_store, None, None, None, None)
    assert result.total_examined == 2
    assert len(result.matches) == 2
    assert len(result.rejection_details) == 0

    # Filter by role_flags
    device_flags = RoleFlagsType.IS_MIRROR | RoleFlagsType.IS_DER | RoleFlagsType.IS_SUBMETER
    result = find_mrids_matching(resource_store, device_flags, None, None, None)
    assert len(result.matches) == 1
    assert result.matches[0].resource is not None and isinstance(result.matches[0].resource, MirrorUsagePoint)
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected

    # Filter by post_rate
    result = find_mrids_matching(resource_store, None, None, None, 60)
    assert len(result.matches) == 1
    assert result.matches[0].resource is not None and isinstance(result.matches[0].resource, MirrorUsagePoint)
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected

    # Filter by reading_type_vals
    active_power_vals = [(UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.AVERAGE)]
    result = find_mrids_matching(resource_store, None, None, active_power_vals, None)
    assert len(result.matches) == 1
    assert result.matches[0].resource is not None and isinstance(result.matches[0].resource, MirrorUsagePoint)
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected


def create_discovered_usage_point(
    store: ResourceStore,
    mrid: str,
    readings: list[tuple[CSIPAusReadingType | None, str]],  # [(reading_type, meter_reading_mrid), ...]
):
    """Helper to populate a ResourceStore with a UsagePoint discovered via /upt, walking the full
    UsagePoint -> MeterReadingList -> MeterReading -> ReadingType chain. A None reading_type omits the ReadingType"""
    href = f"/upt/{mrid}"
    up = generate_class_instance(UsagePoint, href=href, mRID=mrid)
    sr_up = store.append_resource(CSIPAusResource.UsagePoint, None, up)

    mrl = generate_class_instance(MeterReadingListResponse, href=f"{href}/mr")
    sr_mrl = store.append_resource(CSIPAusResource.MeterReadingList, sr_up.id, mrl)

    for i, (rt, mr_mrid) in enumerate(readings):
        mr = generate_class_instance(MeterReading, seed=i, href=f"{href}/mr/{i}", mRID=mr_mrid)
        sr_mr = store.append_resource(CSIPAusResource.MeterReading, sr_mrl.id, mr)
        if rt is not None:
            uom, kind, data_qualifier = generate_reading_type_values(rt)
            store.append_resource(
                CSIPAusResource.ReadingType,
                sr_mr.id,
                ReadingType(href=f"{href}/mr/{i}/rt", uom=uom, kind=kind, dataQualifier=data_qualifier),
            )


APA = CSIPAusReadingType.ActivePowerAverage
APM = CSIPAusReadingType.ActivePowerMaximum
FA = CSIPAusReadingType.FrequencyAverage
VA = CSIPAusReadingType.VoltageSinglePhaseAverage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "up_same_mrid, up_readings, expected_pass",
    [
        (True, [(APA, APA), (FA, FA)], True),  # exact match by mRID
        (True, [(FA, FA), (APA, APA)], True),  # order doesn't matter
        (True, [(APA, APA)], False),  # missing reading type
        (True, [(APA, APA), (FA, FA), (VA, VA)], False),  # extra reading type
        (True, [(APA, APM), (FA, FA)], False),  # MeterReading mRID matches but ReadingType is mismatched
        (True, [(APA, APA), (VA, FA)], False),  # ReadingType matches but MeterReading mRID is mismatched
        (True, [(APA, APA), (FA, None)], False),  # MeterReading has no ReadingType - rejected (not raised)
        (False, [(APA, APA), (FA, FA)], False),  # UsagePoint has a different mRID to the MUP
        (False, None, False),  # No UsagePoints discovered
    ],
)
async def test_check_mirror_usage_point_usage_point_fallback(
    up_same_mrid: bool,
    up_readings: list[tuple[CSIPAusReadingType, CSIPAusReadingType | None]] | None,
    expected_pass: bool,
    testing_contexts_factory,
):
    """When the server doesn't expose MirrorMeterReadings under the MUP, the check should fall back to the
    UsagePoint (discovered via /upt) with the same mRID. up_readings is [(rt_for_mrid, rt_for_values), ...]"""

    client_config = generate_class_instance(ClientConfig, id="test-client", pen=99887766)
    location = CSIPAusReadingLocation.Device
    reading_types = [APA, FA]
    generated_mrids = generate_mup_mrids(location, reading_types, None, client_config)

    mup = create_mirror_usage_point(
        mrid=generated_mrids.mup_mrid, role_flags=generate_role_flags(location), reading_types=[]
    )
    mup.mirrorMeterReadings = None  # Server didn't expose them

    resource_store = ResourceStore(CSIPAusResourceTree())
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
    if up_readings is not None:
        up_mrid = generated_mrids.mup_mrid if up_same_mrid else generate_hashed_mrid("other", client_config.pen)
        create_discovered_usage_point(
            resource_store,
            up_mrid,
            [
                (rt_values, generate_mmr_mrids(generated_mrids.mup_mrid, [rt_mrid], client_config.pen)[rt_mrid])
                for rt_mrid, rt_values in up_readings
            ],
        )

    async with ClientSession() as session:
        context, step = testing_contexts_factory(session)
        context.clients_by_alias[step.client_alias].client_config = client_config
        context.clients_by_alias[step.client_alias].discovered_resources = resource_store

        result = check_mirror_usage_point(
            resolved_parameters={"matches": True, "location": location, "reading_types": reading_types},
            step=step,
            context=context,
        )

        assert result.passed == expected_pass, result.description
        if up_readings is not None and any(rt_values is None for _, rt_values in up_readings):
            assert result.description and "have no readingType (from UsagePoint)" in result.description


def test_find_mup_readings():
    """MMRs on the MUP take priority over any UsagePoint. Otherwise only the UsagePoint with the same mRID is used"""

    role_flags = generate_role_flags(CSIPAusReadingLocation.Device)
    mup_with_mmrs = create_mirror_usage_point(mrid="MUP1", role_flags=role_flags, reading_types=[(FA, "MMR1")])
    mup_no_mmrs = create_mirror_usage_point(mrid="MUP2", role_flags=role_flags, reading_types=[])
    mup_no_mmrs.mirrorMeterReadings = None
    mup_no_up = create_mirror_usage_point(mrid="MUP3", role_flags=role_flags, reading_types=[])

    resource_store = ResourceStore(CSIPAusResourceTree())
    create_discovered_usage_point(resource_store, "MUP1", [(VA, "MR1")])
    create_discovered_usage_point(resource_store, "OTHER", [(APA, "MR2")])
    create_discovered_usage_point(resource_store, "MUP2", [(APA, "MR3"), (None, "MR4")])

    result, source = find_mup_readings(resource_store, mup_with_mmrs)
    assert [(mrid, rt.uom if rt else None) for mrid, rt in result] == [("MMR1", UomType.FREQUENCY_HZ)]
    assert source == ""

    result, source = find_mup_readings(resource_store, mup_no_mmrs)
    assert [(mrid, rt.uom if rt else None) for mrid, rt in result] == [
        ("MR3", UomType.REAL_POWER_WATT),
        ("MR4", None),
    ]
    assert source == " (from UsagePoint)"

    assert find_mup_readings(resource_store, mup_no_up) == (
        [],
        " (no MirrorMeterReadings or UsagePoint with mRID MUP3)",
    )
    assert find_mup_readings(ResourceStore(CSIPAusResourceTree()), mup_no_up) == ([], " (no MirrorMeterReadings)")


@pytest.mark.parametrize(
    "has_mmrs, up_mrid, expected_suffix",
    [
        (True, None, "]"),  # MMRs exposed - no source mentioned
        (False, None, " (no MirrorMeterReadings)"),  # Server not hosting /upt - UsagePoints never mentioned
        (False, "OTHER", " (no MirrorMeterReadings or UsagePoint with mRID MUP1)"),
        (False, "MUP1", " (from UsagePoint)"),
    ],
)
def test_find_mrids_matching_rejection_source(has_mmrs: bool, up_mrid: str | None, expected_suffix: str):
    """Rejections describe where readings came from, only mentioning UsagePoints when the server hosts them"""
    role_flags = generate_role_flags(CSIPAusReadingLocation.Device)
    mup = create_mirror_usage_point(mrid="MUP1", role_flags=role_flags, reading_types=[(FA, "MMR1")])
    if not has_mmrs:
        mup.mirrorMeterReadings = None

    resource_store = ResourceStore(CSIPAusResourceTree())
    resource_store.append_resource(CSIPAusResource.MirrorUsagePoint, None, mup)
    if up_mrid is not None:
        create_discovered_usage_point(resource_store, up_mrid, [(FA, "MR1")])

    result = find_mrids_matching(resource_store, None, MirrorUsagePointMrids("MUP1", {APA: "MMR2"}), None, None)

    assert result.matches == []
    assert len(result.rejection_details) == 1
    assert result.rejection_details[0].endswith(expected_suffix), result.rejection_details[0]
