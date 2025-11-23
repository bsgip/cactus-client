from itertools import product

import pytest
from assertical.asserts.generator import assert_class_instance_equality
from assertical.fake.generator import generate_class_instance, register_value_generator
from assertical.fixtures.generator import generator_registry_snapshot
from envoy_schema.server.schema.sep2.pub_sub import (
    NotificationResourceCombined,
)

from cactus_client.action.subscription import (
    RESOURCE_TYPE_BY_XSI,
    parse_combined_resource,
)
from cactus_client.error import CactusClientException


@pytest.fixture
def assertical_all_hexbinary8():
    """Forces all strings to generate as a hexbinary8 (eg: 0A)"""
    with generator_registry_snapshot():
        register_value_generator(str, lambda x: f"{(x % 256):02X}")
        yield


@pytest.mark.parametrize(
    "xsi_type, optional_is_none",
    product(RESOURCE_TYPE_BY_XSI.keys(), [True, False]),
)
def test_parse_combined_resource(xsi_type: str, optional_is_none: bool, assertical_all_hexbinary8):
    """This tries to stress test our conversion from NotificationResourceCombined to a specific type like DERControl"""

    # Start by generating our target type so we get the expected optional/mandatory params
    target_type = RESOURCE_TYPE_BY_XSI[xsi_type]
    source_values = generate_class_instance(target_type, optional_is_none=optional_is_none, generate_relationships=True)

    # Next - we bring those values across to a NotificationResourceCombined
    source = NotificationResourceCombined(**source_values.__dict__)

    # Finally - do the test and see if the resulting object is of the right type and has pulled the right values
    actual = parse_combined_resource(xsi_type=xsi_type, resource=source)
    assert isinstance(actual, target_type)
    assert_class_instance_equality(target_type, source, actual)
    assert_class_instance_equality(target_type, source_values, actual)


@pytest.mark.parametrize("bad_type", [None, "", "DERControlButDNE"])
def test_parse_combined_resource_bad_type(bad_type):
    with pytest.raises(CactusClientException):
        parse_combined_resource(bad_type, generate_class_instance(NotificationResourceCombined))
