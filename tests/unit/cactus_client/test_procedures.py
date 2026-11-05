from assertical.asserts.type import assert_list_type, assert_set_type
from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.server import TestProcedure, TestProcedureId

from cactus_client.procedures import (
    get_test_procedure_for_version,
    get_test_procedure_ids_for_version,
    get_test_procedures_for_version,
)


def test_get_test_procedure_ids_for_version() -> None:

    v12 = get_test_procedure_ids_for_version(CSIPAusVersion.RELEASE_1_2)
    v13 = get_test_procedure_ids_for_version(CSIPAusVersion.RELEASE_1_3)
    empty = get_test_procedure_ids_for_version("foobar-not a version")  # ty:ignore[invalid-argument-type]

    assert_set_type(TestProcedureId, v12)
    assert_set_type(TestProcedureId, v13)
    assert_set_type(TestProcedureId, empty, count=0)

    # Should be in both
    assert TestProcedureId.S_ALL_01 in v12
    assert TestProcedureId.S_ALL_01 in v13


def test_get_test_procedure_for_version() -> None:
    v12_tp = get_test_procedure_for_version(CSIPAusVersion.RELEASE_1_2, TestProcedureId.S_ALL_01)
    v13_tp = get_test_procedure_for_version(CSIPAusVersion.RELEASE_1_3, TestProcedureId.S_ALL_01)
    vNone_tp = get_test_procedure_for_version("bad version", TestProcedureId.S_ALL_01)  # ty:ignore[invalid-argument-type]
    v12_None = get_test_procedure_for_version(CSIPAusVersion.RELEASE_1_2, "not a version")  # ty:ignore[invalid-argument-type]

    assert vNone_tp is None
    assert v12_None is None
    assert v12_tp == v13_tp, "S-ALL-01 is the same for both versions"

    # This test is a little brittle - it assumes the S-ALL-01 description.
    assert isinstance(v12_tp, TestProcedure) and v12_tp.description == "Discovery with Out-Of-Band registration"


def test_get_test_procedures_for_version() -> None:

    v12 = get_test_procedures_for_version(CSIPAusVersion.RELEASE_1_2)
    v13 = get_test_procedures_for_version(CSIPAusVersion.RELEASE_1_3)
    empty = get_test_procedures_for_version("foobar-not a version")  # ty:ignore[invalid-argument-type]

    assert_list_type(tuple, v12)
    assert_list_type(tuple, v13)
    assert_list_type(tuple, empty, count=0)

    # S_ALL_01 must be in both - it's the first one due to sorting
    assert len(v12) > 0
    assert len(v13) > 0
    assert v12[0] == v13[0]
    assert isinstance(v12[0][0], TestProcedureId) and v12[0][0] == TestProcedureId.S_ALL_01
    assert isinstance(v12[0][1], TestProcedure)
    assert isinstance(v13[0][0], TestProcedureId) and v13[0][0] == TestProcedureId.S_ALL_01
    assert isinstance(v13[0][1], TestProcedure)
