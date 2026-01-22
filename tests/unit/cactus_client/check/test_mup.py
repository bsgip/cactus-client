import re

import pytest
from aiohttp import ClientSession
from assertical.asserts.type import assert_dict_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
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
    generate_hashed_mrid,
    generate_mmr_mrid,
    generate_mup_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.error import CactusClientException
from cactus_client.model.config import ClientConfig
from cactus_client.model.resource import CSIPAusResourceTree, ResourceStore


def assert_mrid(mrid: str, pen: int):
    assert isinstance(mrid, str)
    assert len(mrid) == 32
    assert mrid.endswith(str(pen))
    assert re.search(r"[^A-F0-9]", mrid) is None, "Should only be uppercase hex chars"


def assert_mup_mrids(m: MirrorUsagePointMrids, reading_types: list[CSIPAusReadingType], pen: int):
    assert isinstance(m, MirrorUsagePointMrids)
    assert_mrid(m.mup_mrid, pen)

    assert_dict_type(CSIPAusReadingType, str, m.mmr_mrids, len(reading_types))
    for rt in reading_types:
        assert_mrid(m.mmr_mrids[rt], pen)


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
    rts_2 = [CSIPAusReadingType.ActivePowerMaximum, CSIPAusReadingType.ActivePowerMinimum]

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

    mup4 = generate_mup_mrids(
        CSIPAusReadingLocation.Device,
        rts_2,
        ["012345678901234567890123XXXXXXXX", "AAAAAAAAAA01234567890123XXXXXXXX"],
        cfg1,
    )
    assert_mup_mrids(mup4, rts_2, cfg1.pen)
    assert mup4.mmr_mrids[CSIPAusReadingType.ActivePowerMaximum].startswith("012345678901234567890123")
    assert mup4.mmr_mrids[CSIPAusReadingType.ActivePowerMinimum].startswith("AAAAAAAAAA01234567890123")


def test_generate_reading_type_values_bad_value():
    with pytest.raises(CactusClientException):
        generate_reading_type_values("not a valid value")


def test_generate_reading_type_values():
    all_values: list[tuple] = []
    for rt in CSIPAusReadingType:
        all_values.append(generate_reading_type_values(rt))

    assert len(all_values) == len(set(all_values)), "For catching copy paste errors"


def test_generate_role_flags_bad_value():
    with pytest.raises(CactusClientException):
        generate_role_flags("not a valid value")


def test_generate_role_flags_values():
    all_values = []
    for loc in CSIPAusReadingLocation:
        all_values.append(generate_role_flags(loc))

    assert len(all_values) == len(set(all_values)), "For catching copy paste errors"


def test_generate_mmr_mrid_basic():
    """Test that generate_mmr_mrid produces a consistent 32-character MRID with PEN suffix"""
    mup_mrid = "ABC123456789012345678901234567890"
    rt = CSIPAusReadingType.ActivePowerAverage
    pen = 12345678

    result = generate_mmr_mrid(mup_mrid, rt, pen)

    assert isinstance(result, str)
    assert len(result) == 32
    assert result.endswith(str(pen))

    # Deterministic:
    result2 = generate_mmr_mrid(mup_mrid, rt, pen)
    assert result == result2


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
            MirrorMeterReading, mRID=mmr_mrid, readingType=ReadingType(uom=uom, kind=kind, dataQualifier=data_qualifier)
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
            [CSIPAusReadingType.ActivePowerAverage, CSIPAusReadingType.ReactivePowerInstantaneous],
            60,
        ),
        (CSIPAusReadingLocation.Site, [CSIPAusReadingType.ActivePowerInstantaneous], 30),
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

        assert result.passed, f"Expected check to pass but got: {result.message}"


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
        reading_types=[(device_reading_types[0], generated_mrids.mmr_mrids[device_reading_types[0]])],
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
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected

    # Filter by post_rate
    result = find_mrids_matching(resource_store, None, None, None, 60)
    assert len(result.matches) == 1
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected

    # Filter by reading_type_vals
    active_power_vals = [(UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.AVERAGE)]
    result = find_mrids_matching(resource_store, None, None, active_power_vals, None)
    assert len(result.matches) == 1
    assert result.matches[0].resource.mRID == device_mup.mRID
    assert len(result.rejection_details) == 1  # site_mup rejected
