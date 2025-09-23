import pytest
from assertical.asserts.time import assert_nowish
from assertical.asserts.type import assert_dict_type, assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.csip_aus.connection_point import ConnectionPointResponse
from envoy_schema.server.schema.sep2.der import (
    DER,
    DefaultDERControl,
    DERCapability,
    DERControlListResponse,
    DERListResponse,
    DERProgramListResponse,
    DERProgramResponse,
    DERSettings,
    DERStatus,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
    RegistrationResponse,
)
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
    FunctionSetAssignmentsResponse,
)
from envoy_schema.server.schema.sep2.identification import Link, Resource
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorUsagePointListResponse,
)

from cactus_client.model.resource import (
    RESOURCE_SEP2_TYPES,
    CSIPAusResourceTree,
    ResourceStore,
    StoredResource,
    generate_resource_link_hrefs,
)


def test_RESOURCE_SEP2_TYPES():
    """Trying to catch a mis-registration in RESOURCE_SEP2_TYPES"""
    for resource in CSIPAusResource:
        assert resource in RESOURCE_SEP2_TYPES
        mapped_type = RESOURCE_SEP2_TYPES[resource]
        assert isinstance(mapped_type, type)
        assert resource.name in mapped_type.__name__, "Names should approximate eachother"

    assert len(RESOURCE_SEP2_TYPES) == len(set(RESOURCE_SEP2_TYPES.values())), "Each mapping should be unique"


def test_get_resource_tree_all_resources_encoded():
    tree = CSIPAusResourceTree()
    for resource in CSIPAusResource:
        assert resource in tree.tree


@pytest.mark.parametrize(
    "targets, expected",
    [
        ([], []),
        ([CSIPAusResource.Time], [CSIPAusResource.DeviceCapability, CSIPAusResource.Time]),
        ([CSIPAusResource.Time, CSIPAusResource.Time], [CSIPAusResource.DeviceCapability, CSIPAusResource.Time]),
        (
            [CSIPAusResource.DERSettings],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
            ],
        ),
        (
            [CSIPAusResource.DERSettings, CSIPAusResource.Time],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
            ],
        ),
        (
            [
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
                CSIPAusResource.DERProgramList,
                CSIPAusResource.DERCapability,
            ],
            [
                CSIPAusResource.DeviceCapability,
                CSIPAusResource.EndDeviceList,
                CSIPAusResource.EndDevice,
                CSIPAusResource.DERList,
                CSIPAusResource.DER,
                CSIPAusResource.DERSettings,
                CSIPAusResource.Time,
                CSIPAusResource.FunctionSetAssignmentsList,
                CSIPAusResource.FunctionSetAssignments,
                CSIPAusResource.DERProgramList,
                CSIPAusResource.DERCapability,
            ],
        ),
    ],
)
def test_discover_resource_plan(targets, expected):
    tree = CSIPAusResourceTree()

    actual = tree.discover_resource_plan(targets)
    assert actual == expected
    assert_list_type(CSIPAusResource, actual, len(expected))


@pytest.mark.parametrize(
    "target, expected",
    [
        (CSIPAusResource.DeviceCapability, None),
        (CSIPAusResource.EndDeviceList, CSIPAusResource.DeviceCapability),
        (CSIPAusResource.EndDevice, CSIPAusResource.EndDeviceList),
        (CSIPAusResource.DERSettings, CSIPAusResource.DER),
    ],
)
def test_parent_resource(target: CSIPAusResource, expected: CSIPAusResource | None):
    tree = CSIPAusResourceTree()

    actual = tree.parent_resource(target)
    assert actual == expected
    if expected is not None:
        assert isinstance(actual, CSIPAusResource)


def test_ResourceStore():
    """Sanity check on the basic methods to ensure no obvious exceptions are thrown"""
    s = ResourceStore(CSIPAusResourceTree())
    s.clear()  # Ensure we can clear an empty store

    r1 = generate_class_instance(DER, seed=101, generate_relationships=True)
    r2 = generate_class_instance(DER, seed=202)
    r3 = generate_class_instance(EndDeviceResponse, seed=303, generate_relationships=True)
    r4 = generate_class_instance(DER, seed=404)

    sr1 = s.set_resource(CSIPAusResource.DER, None, r1)
    assert isinstance(sr1, StoredResource)
    assert sr1.resource is r1
    assert_nowish(sr1.created_at)
    assert sr1.parent is None
    assert sr1.resource_type == CSIPAusResource.DER
    assert sr1.member_of_list == CSIPAusResource.DERList
    assert_dict_type(CSIPAusResource, str, sr1.resource_link_hrefs, count=3)
    assert CSIPAusResource.DERSettings in sr1.resource_link_hrefs
    assert CSIPAusResource.DERCapability in sr1.resource_link_hrefs
    assert CSIPAusResource.DERStatus in sr1.resource_link_hrefs

    assert s.get(CSIPAusResource.EndDevice) == []
    assert s.get(CSIPAusResource.DER) == [sr1]

    sr2 = s.append_resource(CSIPAusResource.DER, None, r2)
    assert sr2.parent is None
    assert sr2.resource is r2
    assert sr2.resource_type == CSIPAusResource.DER
    assert sr2.resource_link_hrefs == {}, "We generated this entry with no links"

    sr3 = s.append_resource(CSIPAusResource.EndDevice, sr1, r3)
    assert sr3.parent == sr1
    assert sr3.resource is r3
    assert sr3.resource_type == CSIPAusResource.EndDevice
    assert_dict_type(CSIPAusResource, str, sr3.resource_link_hrefs, count=5)
    assert CSIPAusResource.ConnectionPoint in sr3.resource_link_hrefs
    assert CSIPAusResource.FunctionSetAssignmentsList in sr3.resource_link_hrefs
    assert CSIPAusResource.Registration in sr3.resource_link_hrefs
    assert CSIPAusResource.SubscriptionList in sr3.resource_link_hrefs
    assert CSIPAusResource.DERList in sr3.resource_link_hrefs

    assert s.get(CSIPAusResource.EndDevice) == [sr3]
    assert s.get(CSIPAusResource.DER) == [sr1, sr2]
    assert s.get(CSIPAusResource.DeviceCapability) == []

    s.clear_resource(CSIPAusResource.DeviceCapability)
    s.clear_resource(CSIPAusResource.EndDevice)

    assert s.get(CSIPAusResource.EndDevice) == []
    assert s.get(CSIPAusResource.DER) == [sr1, sr2]
    assert s.get(CSIPAusResource.DeviceCapability) == []

    sr4 = s.set_resource(CSIPAusResource.DER, None, r4)
    assert s.get(CSIPAusResource.EndDevice) == []
    assert s.get(CSIPAusResource.DER) == [sr4]
    assert s.get(CSIPAusResource.DeviceCapability) == []


SEP2_TYPES_WITH_LINKS: list[tuple[CSIPAusResource, type]] = [
    (CSIPAusResource.DeviceCapability, DeviceCapabilityResponse),
    (CSIPAusResource.EndDevice, EndDeviceResponse),
    (CSIPAusResource.DER, DER),
    (CSIPAusResource.FunctionSetAssignments, FunctionSetAssignmentsResponse),
    (CSIPAusResource.DERProgram, DERProgramResponse),
]


@pytest.mark.parametrize("resource, resource_type", SEP2_TYPES_WITH_LINKS)
def test_generate_resource_link_hrefs_specific_type(resource: CSIPAusResource, resource_type: type):
    """Ensure that the nominated "interesting" types work with generate_resource_link_hrefs"""
    result = generate_resource_link_hrefs(resource, generate_class_instance(resource_type, generate_relationships=True))
    assert_dict_type(CSIPAusResource, str, result)

    assert len(result) > 0, "Should have at least one type"
    assert len(result.values()) == len(set(result.values())), "All unique hrefs returned"

    result_optionals = generate_resource_link_hrefs(
        resource, generate_class_instance(resource_type, generate_relationships=True, optional_is_none=True)
    )
    assert_dict_type(CSIPAusResource, str, result_optionals)


@pytest.mark.parametrize(
    "resource", [resource for resource in CSIPAusResource if resource not in {r for r, _ in SEP2_TYPES_WITH_LINKS}]
)
def test_generate_resource_link_hrefs_other_types(resource: CSIPAusResource):
    """Ensure that the nominated "not interesting" types generate an empty dict for generate_resource_link_hrefs"""
    result = generate_resource_link_hrefs(resource, generate_class_instance(Resource))
    assert isinstance(result, dict)
    assert result == {}
