from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERProgramResponse
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import StoredResource


def check_der_program(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether there is a DERProgram in the resource store which matches the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    primacy: int | None = resolved_parameters.get("primacy", None)
    fsa_index: int | None = resolved_parameters.get("fsa_index", None)

    resource_store = context.discovered_resources(step)
    all_der_programs = resource_store.get_for_type(CSIPAusResource.DERProgram)

    # Get all FSAs to determine the index, sort FSAs by href for consistent ordering
    if fsa_index is not None:
        all_fsas = resource_store.get_for_type(CSIPAusResource.FunctionSetAssignments)
        sorted_fsas = sorted(all_fsas, key=lambda sr: sr.resource.href if sr.resource.href else "")

    # Perform filtering
    matching_der_programs: list[StoredResource] = []
    for derp_sr in all_der_programs:
        derp = cast(DERProgramResponse, derp_sr.resource)

        # Filter by primacy if specified
        if primacy is not None and derp.primacy != primacy:
            continue

        # Filter by FSA index if specified
        if fsa_index is not None:
            # Get the parent FSA
            fsa = resource_store.get_ancestor_of(CSIPAusResource.FunctionSetAssignments, derp_sr.id)

            if fsa is None:
                continue

            # Find the index of this FSA
            try:
                actual_index = sorted_fsas.index(fsa)
                if actual_index != fsa_index:
                    continue

            # FSA not found in list, shouldn't happen
            except ValueError:
                continue

        matching_der_programs.append(derp_sr)

    # Check match criteria
    total_matches = len(matching_der_programs)

    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(
            False, f"Matched {total_matches} DERPrograms against criteria. Expected at least {minimum_count}"
        )

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(
            False, f"Matched {total_matches} DERPrograms against criteria. Expected at most {maximum_count}"
        )

    # If min count is not set assume at least one must match
    if total_matches < 1:
        return CheckResult(False, f"Matched {total_matches} DERPrograms against criteria. Expected at least one")

    return CheckResult(True, None)
