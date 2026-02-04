from datetime import datetime

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlBase, DERControlResponse
from envoy_schema.server.schema.sep2.der_control_types import ActivePower, ReactivePower
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePoint
from envoy_schema.server.schema.sep2.time import TimeResponse

from cactus_client.check.sep2 import (
    is_invalid_der_control,
    is_invalid_mrid,
    is_invalid_power_type,
    is_invalid_resource,
    is_invalid_signed_percent,
)
from cactus_client.model.resource import (
    StoredResource,
    StoredResourceId,
)


@pytest.mark.parametrize(
    "mrid, pen, expected_pass",
    [
        ("", 0, False),
        (None, 0, False),
        ("00004567", 4567, True),
        ("0ABCDEF0123456789000004567", 4567, True),
        ("FFFFFFFFFFFFFFFFFFFFFFFF00004567", 4567, True),
        ("FFFFFFFFFFFFFFFFFFFFFFFFABCD4567", 4567, False),
        ("FFFFFFFFFFFFFFFFFFFFF12399999999", 99999999, True),
        ("FFFFFFFFFFFFFFFFFFFFF12399999999", 999, False),  # wrong PEN
        ("BBAA402E1AD2D673BAE72163FE00000002", 2, False),  # Too long
        ("E1AD2D673BAE72163FE00000002", 2, False),  # Odd number of octets
        ("ffffffffffffffffffffffff00004567", 4567, False),  # Lowercase is not OK
    ],
)
def test_is_invalid_mrid(mrid: str | None, pen: int, expected_pass: bool):
    actual = is_invalid_mrid(mrid, pen)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)


@pytest.mark.parametrize(
    "entity, expected_pass",
    [
        (None, True),
        (ActivePower(multiplier=0, value=-12345), True),
        (ReactivePower(multiplier=0, value=-12345), True),
        (ActivePower(multiplier=-3, value=12345), True),
        (ReactivePower(multiplier=3, value=12345), True),
        (ActivePower(multiplier=0, value=0), True),
        (ActivePower(multiplier=0, value=40000), False),  # Out of range int16 value
        (ActivePower(multiplier=0, value=-40000), False),  # Out of range int16 value
        (ReactivePower(multiplier=0, value=-40000), False),  # Out of range int16 value
    ],
)
def test_is_invalid_power_type(entity: ActivePower | ReactivePower | None, expected_pass: bool):
    actual = is_invalid_power_type(entity)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)


@pytest.mark.parametrize(
    "value, expected_pass",
    [
        (None, True),
        (0, True),
        (13, True),
        (1234, True),
        (-1234, True),
        (-12345, False),
        (12345, False),
    ],
)
def test_is_invalid_signed_percent(value: int | None, expected_pass: bool):
    actual = is_invalid_signed_percent(value)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)


@pytest.mark.parametrize(
    "derc, expected_pass",
    [
        (
            generate_class_instance(
                DERControlResponse, DERControlBase_=generate_class_instance(DERControlBase, optional_is_none=True)
            ),
            True,
        ),
        (
            generate_class_instance(
                DERControlResponse,
                DERControlBase_=generate_class_instance(
                    DERControlBase,
                    optional_is_none=True,
                    opModImpLimW=ActivePower(multiplier=0, value=-1234),
                    opModExpLimW=ActivePower(multiplier=1, value=-2234),
                    opModLoadLimW=ActivePower(multiplier=2, value=3234),
                    opModGenLimW=ActivePower(multiplier=3, value=4234),
                ),
            ),
            True,
        ),
        (
            generate_class_instance(
                DERControlResponse,
                DERControlBase_=generate_class_instance(
                    DERControlBase,
                    optional_is_none=True,
                    opModImpLimW=ActivePower(multiplier=0, value=-1234),
                    opModExpLimW=ActivePower(multiplier=1, value=-2234),
                    opModLoadLimW=ActivePower(multiplier=2, value=33234),  # out of range value
                    opModGenLimW=ActivePower(multiplier=3, value=4234),
                ),
            ),
            False,
        ),
    ],
)
def test_is_invalid_der_control(derc: DERControlResponse, expected_pass: bool):
    actual = is_invalid_der_control(derc)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)


VALID_SERVER_PEN = 11223344


def sr(type: CSIPAusResource, resource: Resource) -> StoredResource:
    """Utility for generating a StoredResource via simple shorthand"""
    return StoredResource(StoredResourceId(("/fake/1",)), datetime(2024, 11, 1), type, {}, None, resource)


@pytest.mark.parametrize(
    "sr, expected_pass",
    [
        (
            sr(
                CSIPAusResource.DERControl,
                generate_class_instance(
                    DERControlResponse,
                    mRID=f"ABC123{VALID_SERVER_PEN}",
                    DERControlBase_=generate_class_instance(
                        DERControlBase, optional_is_none=True, opModImpLimW=ActivePower(multiplier=3, value=30000)
                    ),
                ),
            ),
            True,
        ),
        (
            sr(
                CSIPAusResource.DERControl,
                generate_class_instance(
                    DERControlResponse,
                    mRID=f"ABC123{VALID_SERVER_PEN}",
                    DERControlBase_=generate_class_instance(
                        DERControlBase, optional_is_none=True, opModImpLimW=ActivePower(multiplier=-1, value=99999)
                    ),
                ),
            ),
            False,  # out of range value
        ),
        (
            sr(
                CSIPAusResource.DERControl,
                generate_class_instance(
                    DERControlResponse,
                    mRID=f"ABC123{VALID_SERVER_PEN+1}",
                    DERControlBase_=generate_class_instance(DERControlBase, optional_is_none=True),
                ),
            ),
            False,  # PEN is mismatching
        ),
        (
            sr(
                CSIPAusResource.DERControl,
                generate_class_instance(
                    DERControlResponse,
                    mRID="ABC123",
                    DERControlBase_=generate_class_instance(DERControlBase, optional_is_none=True),
                ),
            ),
            False,  # PEN is not base 10
        ),
        (
            sr(
                CSIPAusResource.DERControl,
                generate_class_instance(
                    DERControlResponse,
                    mRID=f"456abc123{VALID_SERVER_PEN}",
                    DERControlBase_=generate_class_instance(DERControlBase, optional_is_none=True),
                ),
            ),
            False,  # PEN is not lowercase
        ),
        (
            sr(CSIPAusResource.Time, generate_class_instance(TimeResponse)),
            True,
        ),
        (
            sr(
                CSIPAusResource.MirrorUsagePoint,
                generate_class_instance(
                    MirrorUsagePoint, mRID="not validated"  # MUPs don't need the server PEN to match
                ),
            ),
            True,
        ),
    ],
)
def test_is_invalid_resource(sr: StoredResource, expected_pass: bool):
    actual = is_invalid_resource(sr, VALID_SERVER_PEN)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)
