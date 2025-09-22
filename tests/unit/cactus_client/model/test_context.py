from assertical.asserts.time import assert_nowish
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.model.context import ResourceStore, StoredResource


def test_ResourceStore():
    """Sanity check on the basic methods to ensure no obvious exceptions are thrown"""
    s = ResourceStore()
    s.clear()  # Ensure we can clear an empty store

    r1 = generate_class_instance(Resource, seed=101)
    r2 = generate_class_instance(Resource, seed=202)
    r3 = generate_class_instance(Resource, seed=303)
    r4 = generate_class_instance(Resource, seed=404)

    sr1 = s.set_resource(CSIPAusResource.DER, None, r1)
    assert isinstance(sr1, StoredResource)
    assert sr1.resource is r1
    assert_nowish(sr1.created_at)
    assert sr1.parent is None
    assert sr1.type == CSIPAusResource.DER

    assert s.get(CSIPAusResource.EndDevice) == []
    assert s.get(CSIPAusResource.DER) == [sr1]

    sr2 = s.append_resource(CSIPAusResource.DER, None, r2)
    assert sr2.parent is None
    assert sr2.resource is r2
    assert sr2.type == CSIPAusResource.DER

    sr3 = s.append_resource(CSIPAusResource.EndDevice, sr1, r3)
    assert sr3.parent == sr1
    assert sr3.resource is r3
    assert sr3.type == CSIPAusResource.EndDevice

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


def test_ResourceStore_get_resource_hrefs():
    s = ResourceStore()

    assert list(s.get_resource_hrefs(CSIPAusResource.DER, lambda x: x.resource.href)) == []

    r1 = generate_class_instance(Resource, seed=101, href="/foo/1")
    r2 = generate_class_instance(Resource, seed=202, href=None)
    r3 = generate_class_instance(Resource, seed=303, href="/foo/2")

    sr1 = s.append_resource(CSIPAusResource.DER, None, r1)
    s.append_resource(CSIPAusResource.DER, None, r2)
    sr3 = s.append_resource(CSIPAusResource.DER, None, r3)

    assert list(s.get_resource_hrefs(CSIPAusResource.DER, lambda x: x.resource.href)) == [
        (sr1, "/foo/1"),
        (sr3, "/foo/2"),
    ]
    assert list(s.get_resource_hrefs(CSIPAusResource.EndDevice, lambda x: x.resource.href)) == []
