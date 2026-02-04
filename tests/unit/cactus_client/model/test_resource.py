import pytest
from assertical.asserts.time import assert_nowish
from assertical.asserts.type import assert_dict_type, assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DER,
    DefaultDERControl,
    DERProgramListResponse,
    DERProgramResponse,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsResponse,
)
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorUsagePointListResponse,
)
from treelib.exceptions import NodeIDAbsentError

from cactus_client.error import CactusClientException
from cactus_client.model.resource import (
    RESOURCE_SEP2_TYPES,
    CSIPAusResourceTree,
    ResourceStore,
    StoredResourceId,
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
        if resource == CSIPAusResource.Notification:
            assert resource not in tree.tree, "Notification's aren't part of the tree hierarchy"
        else:
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


def test_Notifications_raise_error():
    """Notifications aren't part of the normal resource tree - attempting to plan for them should raise an error."""
    tree = CSIPAusResourceTree()

    with pytest.raises(NodeIDAbsentError):
        tree.discover_resource_plan(CSIPAusResource.Notification)

    with pytest.raises(NodeIDAbsentError):
        tree.parent_resource(CSIPAusResource.Notification)


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


def test_StoredResourceId():
    dcap1 = StoredResourceId.from_parent(None, "/dcap1")
    dcap2 = StoredResourceId.from_parent(None, "/dcap2")

    edev1 = StoredResourceId.from_parent(dcap1, "/edev1")
    edev2 = StoredResourceId.from_parent(dcap1, "/edev2")
    edev3 = StoredResourceId.from_parent(dcap2, "/edev3")
    edev4 = StoredResourceId.from_parent(dcap2, "/edev1")  # Same HREF as edev1

    derp1 = StoredResourceId.from_parent(edev1, "/derp")  # Same HREF as other DERPs
    derp2 = StoredResourceId.from_parent(edev2, "/derp")  # Same HREF as other DERPs
    derp3 = StoredResourceId.from_parent(edev3, "/derp")  # Same HREF as other DERPs
    derp4 = StoredResourceId.from_parent(edev4, "/derp")  # Same HREF as other DERPs

    derc1 = StoredResourceId.from_parent(derp1, "/derc1")
    derc2 = StoredResourceId.from_parent(derp1, "/derc2")
    derc3 = StoredResourceId.from_parent(derp2, "/derc3")
    derc4 = StoredResourceId.from_parent(derp3, "/derc4")
    derc5 = StoredResourceId.from_parent(derp4, "/derc1")  # Same HREF as derc1

    # Testing href
    assert dcap1.href() == "/dcap1"
    assert dcap2.href() == "/dcap2"
    assert derp1.href() == "/derp"
    assert derp4.href() == "/derp"
    assert derc1.href() == "/derc1"

    # Testing equality
    assert dcap1 != dcap2
    assert dcap1 != dcap2, "Different URIs"
    assert derp1 != derp2, "Different parents"
    assert derc1 != derc5, "Different ancestors"
    assert dcap1 == StoredResourceId.from_parent(None, "/dcap1")
    assert derp1 == StoredResourceId.from_parent(edev1, "/derp")
    assert derp1 != StoredResourceId.from_parent(edev1, "/derpextra")

    # Testing parent_id
    assert dcap1.parent_id() is None
    assert edev1.parent_id() == dcap1
    assert edev2.parent_id() == dcap1
    assert edev3.parent_id() == dcap2
    assert derc1.parent_id() == derp1
    assert derc2.parent_id() == derp1
    assert derc3.parent_id() == derp2
    assert derc4.parent_id() == derp3

    # Testing is_descendent_of
    assert dcap1.is_descendent_of(derc1) is False
    assert derc1.is_descendent_of(dcap1) is True
    assert derc1.is_descendent_of(edev1) is True
    assert derc1.is_descendent_of(edev4) is False
    assert derc1.is_descendent_of(derp1) is True
    assert derc1.is_descendent_of(derp2) is False
    assert derc1.is_descendent_of(derp3) is False
    assert derc1.is_descendent_of(derp4) is False
    assert derc1.is_descendent_of(derc1) is False, "Self is not a descendent"
    assert dcap1.is_descendent_of(dcap1) is False, "Self is not a descendent"

    # Testing is_ancestor_of
    assert dcap1.is_ancestor_of(derc1) is True
    assert derc1.is_ancestor_of(dcap1) is False
    assert edev1.is_ancestor_of(derc1) is True
    assert edev4.is_ancestor_of(derc1) is False
    assert derp1.is_ancestor_of(derc1) is True
    assert derp2.is_ancestor_of(derc1) is False
    assert derp3.is_ancestor_of(derc1) is False
    assert derp4.is_ancestor_of(derc1) is False
    assert derc1.is_ancestor_of(derc1) is False, "Self is not a descendent"
    assert dcap1.is_ancestor_of(dcap1) is False, "Self is not a descendent"


@pytest.mark.parametrize("bad_href", ["", None])
def test_ResourceStore_requires_hrefs(bad_href):
    """Cant add a Resource to the store that doesn't have a HREF"""
    s = ResourceStore(CSIPAusResourceTree())

    with pytest.raises(CactusClientException):
        s.append_resource(
            CSIPAusResource.DeviceCapability, None, generate_class_instance(DeviceCapabilityResponse, href=bad_href)
        )

    with pytest.raises(CactusClientException):
        s.upsert_resource(
            CSIPAusResource.DeviceCapability, None, generate_class_instance(DeviceCapabilityResponse, href=bad_href)
        )

    assert len(s.id_store) == 0
    assert len(s.resource_store) == 0


def test_ResourceStore():
    """Sanity check on the basic methods to ensure no obvious exceptions are thrown"""
    s = ResourceStore(CSIPAusResourceTree())

    # Operations on an empty store work OK
    s.clear()
    s.clear_resource(CSIPAusResource.DER)  # Ensure we can clear an empty store
    assert s.delete_resource(StoredResourceId.from_parent(None, "/href")) is None
    assert s.get_for_type(CSIPAusResource.EndDevice) == []
    assert s.get_for_id(StoredResourceId.from_parent(None, "abc")) is None
    assert list(s.resources()) == []

    r1 = generate_class_instance(DER, seed=101, generate_relationships=True)
    r2 = generate_class_instance(DER, seed=202)
    r3 = generate_class_instance(EndDeviceResponse, seed=303, generate_relationships=True)
    r4 = generate_class_instance(EndDeviceResponse, seed=404)

    sr1 = s.append_resource(CSIPAusResource.DER, None, r1)
    assert sr1.id.parent_id() is None
    assert sr1.resource is r1
    assert sr1.resource_type == CSIPAusResource.DER
    assert sr1.member_of_list == CSIPAusResource.DERList
    assert_nowish(sr1.created_at)
    assert_dict_type(CSIPAusResource, str, sr1.resource_link_hrefs, count=3)
    assert CSIPAusResource.DERCapability in sr1.resource_link_hrefs
    assert CSIPAusResource.DERStatus in sr1.resource_link_hrefs
    assert CSIPAusResource.DERStatus in sr1.resource_link_hrefs
    assert list(s.resources()) == [sr1]

    # We can't append the same resource again
    with pytest.raises(CactusClientException):
        s.append_resource(CSIPAusResource.DER, None, r1)

    # We can append a different resource though
    sr2 = s.append_resource(CSIPAusResource.DER, None, r2)
    assert sr2.id.parent_id() is None
    assert sr1.id != sr2.id
    assert sr2.resource is r2
    assert sr2.resource_type == CSIPAusResource.DER
    assert sr2.member_of_list == CSIPAusResource.DERList
    assert sr2.resource_link_hrefs == {}, "We generated this entry with no links"
    assert list(s.resources()) == [sr1, sr2]

    sr3 = s.append_resource(CSIPAusResource.EndDevice, sr1.id, r3)
    assert sr3.id.parent_id() == sr1.id
    assert sr3.resource is r3
    assert sr3.resource_type == CSIPAusResource.EndDevice
    assert_dict_type(CSIPAusResource, str, sr3.resource_link_hrefs, count=5)
    assert CSIPAusResource.ConnectionPoint in sr3.resource_link_hrefs
    assert CSIPAusResource.FunctionSetAssignmentsList in sr3.resource_link_hrefs
    assert CSIPAusResource.Registration in sr3.resource_link_hrefs
    assert CSIPAusResource.SubscriptionList in sr3.resource_link_hrefs
    assert CSIPAusResource.DERList in sr3.resource_link_hrefs

    # We can't append the same resource again
    with pytest.raises(CactusClientException):
        s.append_resource(CSIPAusResource.EndDevice, sr1.id, r3)

    sr4 = s.append_resource(CSIPAusResource.EndDevice, sr1.id, r4)
    assert sr4.id.parent_id() == sr1.id
    assert sr4.resource is r4
    assert sr4.resource_type == CSIPAusResource.EndDevice
    assert sr4.resource_link_hrefs == {}, "We generated this entry with no links"

    assert s.get_for_type(CSIPAusResource.EndDevice) == [sr3, sr4]
    assert s.get_for_type(CSIPAusResource.DER) == [sr1, sr2]
    assert s.get_for_type(CSIPAusResource.DeviceCapability) == []

    assert s.get_for_id(sr1.id) is sr1
    assert s.get_for_id(sr2.id) is sr2
    assert s.get_for_id(sr3.id) is sr3
    assert s.get_for_id(sr4.id) is sr4
    assert s.get_for_id(StoredResourceId.from_parent(None, "/different")) is None

    # Test clearing
    s.clear_resource(CSIPAusResource.DeviceCapability)
    s.clear_resource(CSIPAusResource.EndDevice)

    assert s.get_for_type(CSIPAusResource.EndDevice) == []
    assert s.get_for_type(CSIPAusResource.DER) == [sr1, sr2]
    assert s.get_for_type(CSIPAusResource.DeviceCapability) == []

    assert s.get_for_id(sr1.id) is sr1
    assert s.get_for_id(sr2.id) is sr2
    assert s.get_for_id(sr3.id) is None, "Cleared"
    assert s.get_for_id(sr4.id) is None, "Cleared"

    # Test deleting
    assert s.delete_resource(sr1.id) is sr1
    assert s.get_for_type(CSIPAusResource.EndDevice) == []
    assert s.get_for_type(CSIPAusResource.DER) == [sr2]
    assert s.get_for_type(CSIPAusResource.DeviceCapability) == []

    assert s.get_for_id(sr1.id) is None, "Deleted"
    assert s.get_for_id(sr2.id) is sr2
    assert s.get_for_id(sr3.id) is None, "Cleared"
    assert s.get_for_id(sr4.id) is None, "Cleared"

    # Test upsert
    r5 = generate_class_instance(DER, seed=505)
    r6 = generate_class_instance(DER, seed=606, href=r5.href)

    sr5 = s.upsert_resource(CSIPAusResource.DER, None, r5)
    upserted_sr5 = s.upsert_resource(CSIPAusResource.DER, None, r5)
    assert upserted_sr5.id == sr5.id
    assert upserted_sr5.resource is sr5.resource
    assert upserted_sr5.member_of_list == sr5.member_of_list
    assert upserted_sr5.resource_link_hrefs == sr5.resource_link_hrefs

    sr6 = s.upsert_resource(CSIPAusResource.DER, None, r6)
    assert sr6.id == sr5.id
    assert sr6.resource is r6
    assert sr6.member_of_list == sr5.member_of_list
    assert sr6.resource_link_hrefs == sr5.resource_link_hrefs

    assert s.get_for_type(CSIPAusResource.EndDevice) == []
    assert s.get_for_type(CSIPAusResource.DER) == [sr2, sr6]
    assert s.get_for_type(CSIPAusResource.DeviceCapability) == []
    assert s.get_for_id(sr1.id) is None, "Deleted"
    assert s.get_for_id(sr2.id) is sr2
    assert s.get_for_id(sr3.id) is None, "Cleared"
    assert s.get_for_id(sr4.id) is None, "Cleared"
    assert s.get_for_id(sr6.id) is sr6
    assert list(s.resources()) == [sr2, sr6]


def test_ResourceStore_upsert_resource():
    s = ResourceStore(CSIPAusResourceTree())

    parent_r1 = generate_class_instance(EndDeviceListResponse, seed=101)
    parent_r2 = generate_class_instance(EndDeviceListResponse, seed=202)
    parent_r3 = generate_class_instance(EndDeviceListResponse, seed=303)

    r1 = generate_class_instance(EndDeviceResponse, seed=404)
    r2 = generate_class_instance(EndDeviceResponse, seed=505)
    r3 = generate_class_instance(EndDeviceResponse, seed=606)
    r1_dupe = generate_class_instance(EndDeviceResponse, seed=404)

    p1 = s.append_resource(CSIPAusResource.EndDeviceList, None, parent_r1)
    p2 = s.append_resource(CSIPAusResource.EndDeviceList, None, parent_r2)
    s.append_resource(CSIPAusResource.EndDeviceList, None, parent_r3)

    cr1_dupe = s.append_resource(CSIPAusResource.EndDevice, p1.id, r1_dupe)
    cr1 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r1)
    cr2 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r2)
    cr3 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r3)

    # Our initial state
    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1_dupe, cr1, cr2, cr3]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p1.id, p2.id, p2.id, p2.id]

    # Add a new item (no clash)
    r_insert = generate_class_instance(EndDeviceResponse, seed=707)
    cr_insert = s.upsert_resource(CSIPAusResource.EndDevice, p2.id, r_insert)

    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1_dupe, cr1, cr2, cr3, cr_insert]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [
        p1.id,
        p2.id,
        p2.id,
        p2.id,
        p2.id,
    ]

    # Add a new item (with clash) - It will update r1 (not the dupe as thats under a different parent)
    r_update = generate_class_instance(EndDeviceResponse, seed=404)
    cr_update = s.upsert_resource(CSIPAusResource.EndDevice, p2.id, r_update)

    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1_dupe, cr_update, cr2, cr3, cr_insert]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [
        p1.id,
        p2.id,
        p2.id,
        p2.id,
        p2.id,
    ]


def test_ResourceStore_delete_resource():
    s = ResourceStore(CSIPAusResourceTree())

    parent_r1 = generate_class_instance(EndDeviceListResponse, seed=101)
    parent_r2 = generate_class_instance(EndDeviceListResponse, seed=202)

    r1 = generate_class_instance(EndDeviceResponse, seed=303)
    r2 = generate_class_instance(EndDeviceResponse, seed=404)
    r3 = generate_class_instance(EndDeviceResponse, seed=505)
    r4 = generate_class_instance(EndDeviceResponse, seed=606)

    p1 = s.append_resource(CSIPAusResource.EndDeviceList, None, parent_r1)
    p2 = s.append_resource(CSIPAusResource.EndDeviceList, None, parent_r2)
    cr1 = s.append_resource(CSIPAusResource.EndDevice, p1.id, r1)
    cr2 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r2)
    cr3 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r3)
    cr4 = s.append_resource(CSIPAusResource.EndDevice, p2.id, r4)

    # Our initial state
    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1, cr2, cr3, cr4]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p1.id, p2.id, p2.id, p2.id]

    # Delete an item
    assert s.delete_resource(cr3.id) is cr3

    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1, cr2, cr4]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p1.id, p2.id, p2.id]

    # Re-deleting has no effect
    assert s.delete_resource(cr3.id) is None
    assert s.delete_resource(cr3.id) is None
    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr1, cr2, cr4]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p1.id, p2.id, p2.id]

    # Delete more items
    assert s.delete_resource(cr1.id) is cr1
    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr2, cr4]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p2.id, p2.id]
    assert s.get_for_id(cr1.id) is None
    assert s.get_for_id(cr2.id) is cr2
    assert s.get_for_id(cr3.id) is None
    assert s.get_for_id(cr4.id) is cr4

    assert s.delete_resource(cr4.id) is cr4
    assert s.get_for_type(CSIPAusResource.EndDevice) == [cr2]
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == [p2.id]

    assert s.delete_resource(cr2.id) is cr2
    assert s.get_for_type(CSIPAusResource.EndDevice) == []
    assert [sr.id.parent_id() for sr in s.get_for_type(CSIPAusResource.EndDevice)] == []

    # Parents are unaffected
    assert s.get_for_type(CSIPAusResource.EndDeviceList) == [p1, p2]


def test_ResourceStore_get_descendents_of():
    """Tests the various "normal" ways of looking descendents of"""
    s = ResourceStore(CSIPAusResourceTree())

    # We are building the following tree
    #
    #                         /- derp1
    #       /- edev1 - derpl1 - derp2 - dderc1
    # edevl
    #       \- edev2 - derpl2 - derp3
    #
    #
    # mupl

    edevl = generate_class_instance(EndDeviceListResponse, seed=101, generate_relationships=True)
    edev_1 = generate_class_instance(EndDeviceResponse, seed=202, generate_relationships=True)
    edev_2 = generate_class_instance(EndDeviceResponse, seed=303, generate_relationships=True)
    derpl_1 = generate_class_instance(DERProgramListResponse, seed=404, generate_relationships=True)
    derpl_2 = generate_class_instance(DERProgramListResponse, seed=505, generate_relationships=True)
    derp_1 = generate_class_instance(DERProgramResponse, seed=606, generate_relationships=True)
    derp_2 = generate_class_instance(DERProgramResponse, seed=707, generate_relationships=True)
    derp_3 = generate_class_instance(DERProgramResponse, seed=808, generate_relationships=True)
    dderc_1 = generate_class_instance(DefaultDERControl, seed=909, generate_relationships=True)
    mupl = generate_class_instance(MirrorUsagePointListResponse, seed=1010, generate_relationships=True)

    sr_edevl = s.append_resource(CSIPAusResource.EndDeviceList, None, edevl)
    sr_edev_1 = s.append_resource(CSIPAusResource.EndDevice, sr_edevl.id, edev_1)
    sr_edev_2 = s.append_resource(CSIPAusResource.EndDevice, sr_edevl.id, edev_2)
    sr_derpl_1 = s.append_resource(CSIPAusResource.DERProgramList, sr_edev_1.id, derpl_1)
    sr_derpl_2 = s.append_resource(CSIPAusResource.DERProgramList, sr_edev_2.id, derpl_2)
    sr_derp_1 = s.append_resource(CSIPAusResource.DERProgram, sr_derpl_1.id, derp_1)
    sr_derp_2 = s.append_resource(CSIPAusResource.DERProgram, sr_derpl_1.id, derp_2)
    sr_derp_3 = s.append_resource(CSIPAusResource.DERProgram, sr_derpl_2.id, derp_3)
    sr_dderc_1 = s.append_resource(CSIPAusResource.DefaultDERControl, sr_derp_2.id, dderc_1)
    sr_mupl = s.append_resource(CSIPAusResource.MirrorUsagePointList, None, mupl)

    assert s.get_descendents_of(CSIPAusResource.DERProgramList, sr_edev_1.id) == [sr_derpl_1]

    assert s.get_descendents_of(CSIPAusResource.DERProgram, sr_edev_1.id) == [sr_derp_1, sr_derp_2]
    assert s.get_descendents_of(CSIPAusResource.DERProgram, sr_edev_2.id) == [sr_derp_3]
    assert s.get_descendents_of(CSIPAusResource.DERProgram, sr_edevl.id) == [sr_derp_1, sr_derp_2, sr_derp_3]
    assert s.get_descendents_of(CSIPAusResource.DERProgram, sr_mupl.id) == []
    assert s.get_descendents_of(CSIPAusResource.DERProgram, sr_dderc_1.id) == []

    assert s.get_descendents_of(CSIPAusResource.DefaultDERControl, sr_derp_1.id) == []
    assert s.get_descendents_of(CSIPAusResource.DefaultDERControl, sr_derp_2.id) == [sr_dderc_1]
    assert s.get_descendents_of(CSIPAusResource.DefaultDERControl, sr_derpl_1.id) == [sr_dderc_1]
    assert s.get_descendents_of(CSIPAusResource.DefaultDERControl, sr_derpl_2.id) == []
    assert s.get_descendents_of(CSIPAusResource.DefaultDERControl, sr_mupl.id) == []


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


def test_ResourceStore_get_ancestor_of():
    """Tests walking up the parent chain to find ancestors of a specific type"""

    # Arrange
    s = ResourceStore(CSIPAusResourceTree())

    # Build a simple chain: edevlist -> edev -> derpl -> derp -> dderc
    edevl = generate_class_instance(EndDeviceListResponse, seed=101, generate_relationships=True)
    edev_1 = generate_class_instance(EndDeviceResponse, seed=202, generate_relationships=True)
    derpl_1 = generate_class_instance(DERProgramListResponse, seed=404, generate_relationships=True)
    derp_1 = generate_class_instance(DERProgramResponse, seed=606, generate_relationships=True)
    dderc_1 = generate_class_instance(DefaultDERControl, seed=909, generate_relationships=True)

    sr_edevl = s.append_resource(CSIPAusResource.EndDeviceList, None, edevl)
    sr_edev_1 = s.append_resource(CSIPAusResource.EndDevice, sr_edevl.id, edev_1)
    sr_derpl_1 = s.append_resource(CSIPAusResource.DERProgramList, sr_edev_1.id, derpl_1)
    sr_derp_1 = s.append_resource(CSIPAusResource.DERProgram, sr_derpl_1.id, derp_1)
    sr_dderc_1 = s.append_resource(CSIPAusResource.DefaultDERControl, sr_derp_1.id, dderc_1)

    # Act/Assert
    assert s.get_ancestor_of(CSIPAusResource.DERProgram, sr_dderc_1.id) == sr_derp_1
    assert s.get_ancestor_of(CSIPAusResource.DERProgramList, sr_dderc_1.id) == sr_derpl_1
    assert s.get_ancestor_of(CSIPAusResource.EndDevice, sr_dderc_1.id) == sr_edev_1
    assert s.get_ancestor_of(CSIPAusResource.EndDeviceList, sr_dderc_1.id) == sr_edevl

    assert s.get_ancestor_of(CSIPAusResource.EndDevice, sr_derp_1.id) == sr_edev_1
    assert s.get_ancestor_of(CSIPAusResource.EndDeviceList, sr_derp_1.id) == sr_edevl

    assert s.get_ancestor_of(CSIPAusResource.DefaultDERControl, sr_dderc_1.id) is None
    assert s.get_ancestor_of(CSIPAusResource.DERProgram, sr_edevl.id) is None
    assert s.get_ancestor_of(CSIPAusResource.EndDeviceList, sr_edevl.id) is None
