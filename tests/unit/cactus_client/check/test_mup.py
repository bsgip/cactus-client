import re

import pytest
from assertical.asserts.type import assert_dict_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
)

from cactus_client.check.mup import (
    MirrorUsagePointMrids,
    generate_hashed_mrid,
    generate_mup_mrids,
    generate_reading_type_values,
    generate_role_flags,
)
from cactus_client.error import CactusClientException
from cactus_client.model.config import ClientConfig


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
