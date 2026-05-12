from dataclasses import replace
from datetime import datetime

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlBase, DERControlResponse
from envoy_schema.server.schema.sep2.der_control_types import ActivePower, ReactivePower
from envoy_schema.server.schema.sep2.end_device import EndDeviceResponse
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePoint
from envoy_schema.server.schema.sep2.pub_sub import SubscriptionListResponse
from envoy_schema.server.schema.sep2.time import TimeResponse

from cactus_client.check.sep2 import (
    is_invalid_der_control,
    is_invalid_mrid,
    is_invalid_power_type,
    is_invalid_resource,
    is_invalid_signed_percent,
    is_invalid_subscription_list,
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
                DERControlResponse,
                DERControlBase_=generate_class_instance(DERControlBase, optional_is_none=True),
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


def test_is_invalid_subscription_list():
    edev_1 = sr(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse, seed=1))
    edev_1 = replace(edev_1, id=StoredResourceId(("/edev/1",)))
    edev_2 = sr(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse, seed=2))
    edev_2 = replace(edev_2, id=StoredResourceId(("/edev/2",)))

    # Nest SubscriptionList 1 under EndDevice 1 and SubscriptionList 2 under EndDevice 2
    sub_list_1 = sr(CSIPAusResource.SubscriptionList, generate_class_instance(SubscriptionListResponse, seed=3))
    sub_list_2 = sr(CSIPAusResource.SubscriptionList, generate_class_instance(SubscriptionListResponse, seed=4))
    sub_list_1 = replace(sub_list_1, id=StoredResourceId.from_parent(edev_1.id, "/sublist/1"))
    sub_list_2 = replace(sub_list_2, id=StoredResourceId.from_parent(edev_2.id, "/sublist/2"))

    # Check the valid combos
    assert is_invalid_subscription_list(sub_list_1, edev_1) is None
    assert is_invalid_subscription_list(sub_list_2, edev_2) is None

    # No aggregator EndDevice
    result = is_invalid_subscription_list(sub_list_1, None)
    assert result and isinstance(result, str)

    # invalid aggregator EndDevice
    result = is_invalid_subscription_list(sub_list_1, edev_2)
    assert result and isinstance(result, str)


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
                        DERControlBase,
                        optional_is_none=True,
                        opModImpLimW=ActivePower(multiplier=3, value=30000),
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
                        DERControlBase,
                        optional_is_none=True,
                        opModImpLimW=ActivePower(multiplier=-1, value=99999),
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
                    mRID=f"ABC123{VALID_SERVER_PEN + 1}",
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
                    MirrorUsagePoint,
                    mRID="not validated",  # MUPs don't need the server PEN to match
                ),
            ),
            True,
        ),
        (
            sr(
                CSIPAusResource.SubscriptionList,
                generate_class_instance(SubscriptionListResponse),
            ),
            False,  # No Aggregator EndDevice - therefore we fail - we will test more closely in a seperate test
        ),
    ],
)
def test_is_invalid_resource(sr: StoredResource, expected_pass: bool):
    actual = is_invalid_resource(sr, VALID_SERVER_PEN, None)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)
