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

    sr1 = s.set_single(CSIPAusResource.DER, None, r1)
    assert isinstance(sr1, StoredResource)
    assert sr1.resource is r1
    assert_nowish(sr1.created_at)
    assert sr1.parent is None
    assert sr1.type == CSIPAusResource.DER

    assert s.get(CSIPAusResource.EndDevice) == []
    assert s.get(CSIPAusResource.DER) == [sr1]

    sr2_3 = s.set_many(CSIPAusResource.EndDevice, sr1, [r2, r3])
    assert_list_type(StoredResource, sr2_3, 2)
    assert sr2_3[0].type == CSIPAusResource.EndDevice
    assert sr2_3[0].resource == r2
    assert_nowish(sr2_3[0].created_at)
    assert sr2_3[0].parent == sr1
    assert sr2_3[1].type == CSIPAusResource.EndDevice
    assert sr2_3[1].resource == r3
    assert_nowish(sr2_3[1].created_at)
    assert sr2_3[1].parent == sr1

    assert s.get(CSIPAusResource.EndDevice) == sr2_3
    assert s.get(CSIPAusResource.DER) == [sr1]

    sr4 = s.set_single(CSIPAusResource.EndDevice, sr2_3[0], r4)
    assert isinstance(sr4, StoredResource)
    assert sr4.resource is r4
    assert_nowish(sr4.created_at)
    assert sr4.parent == sr2_3[0]
    assert sr4.type == CSIPAusResource.EndDevice

    assert s.get(CSIPAusResource.EndDevice) == [sr4]
    assert s.get(CSIPAusResource.DER) == [sr1]
