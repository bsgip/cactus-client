import pytest
from assertical.asserts.type import assert_list_type
from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.action.discovery import discover_resource_plan, get_resource_tree


def test_get_resource_tree_all_resources_encoded():
    tree = get_resource_tree()
    for resource in CSIPAusResource:
        assert resource in tree


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
    tree = get_resource_tree()

    actual = discover_resource_plan(tree, targets)
    assert actual == expected
    assert_list_type(CSIPAusResource, actual, len(expected))
