from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.server import TestProcedure, TestProcedureId, get_all_test_procedures, get_test_procedure


def get_test_procedures_for_version(version: CSIPAusVersion) -> list[tuple[TestProcedureId, TestProcedure]]:
    """Returns all TestProcedures for the specified CSIP-AUS version - will be sorted by TestProcedureId"""
    all_procedures = get_all_test_procedures()
    return sorted(
        (item for item in all_procedures.items() if version in item[1].target_versions), key=lambda item: item[0]
    )


def get_test_procedure_for_version(version: CSIPAusVersion, test_procedure_id: TestProcedureId) -> TestProcedure | None:
    """Returns a specific TestProcedure by ID and ensures its compatible with the specified CSIP-AUS version"""

    try:
        tp = get_test_procedure(test_procedure_id)
    except Exception:
        return None

    if version not in tp.target_versions:
        return None

    return tp


def get_test_procedure_ids_for_version(version: CSIPAusVersion) -> set[TestProcedureId]:
    """Returns all TestProcedureIds for the specified CSIP-AUS version"""
    all_procedures = get_all_test_procedures()
    return set(tp_id for tp_id, tp in all_procedures.items() if version in tp.target_versions)
