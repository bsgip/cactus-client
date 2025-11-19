from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERProgramResponse
from envoy_schema.server.schema.sep2.function_set_assignments import FunctionSetAssignmentsResponse
from envoy_schema.server.schema.sep2.der import DERControlResponse, DefaultDERControl
from cactus_client.check.der_controls import sep2_to_value
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import StoredResource


def check_default_der_control(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether there is a DefaultDERControl in the resource store that matches the check criteria"""

    # Setup
    export_limit_w: float | None = resolved_parameters.get("opModExpLimW", None)
    load_limit_w: float | None = resolved_parameters.get("opModLoadLimW", None)
    generation_limit_w: float | None = resolved_parameters.get("opModGenLimW", None)
    set_grad_w: int | None = resolved_parameters.get("setGradW", None)

    resource_store = context.discovered_resources(step)
    default_der_controls = resource_store.get(CSIPAusResource.DefaultDERControl)

    if not default_der_controls:
        return CheckResult(False, "No DefaultDERControl found in resource store")

    # Check each DefaultDERControl (typically there should be only one)
    for dderc_sr in default_der_controls:
        dderc = cast(DefaultDERControl, dderc_sr.resource)

        # Check export limit if specified
        if export_limit_w is not None:
            actual_export = sep2_to_value(dderc.DERControlBase_.opModExpLimW)
            if actual_export != export_limit_w:
                return CheckResult(
                    False,
                    f"DefaultDERControl opModExpLimW mismatch: expected {export_limit_w}, got {actual_export}",
                )

        # Check load limit if specified
        if load_limit_w is not None:
            actual_load = sep2_to_value(dderc.DERControlBase_.opModLoadLimW)
            if actual_load != load_limit_w:
                return CheckResult(
                    False,
                    f"DefaultDERControl opModLoadLimW mismatch: expected {load_limit_w}, got {actual_load}",
                )

        # Check generation limit if specified
        if generation_limit_w is not None:
            actual_gen = sep2_to_value(dderc.DERControlBase_.opModGenLimW)
            if actual_gen != generation_limit_w:
                return CheckResult(
                    False,
                    f"DefaultDERControl opModGenLimW mismatch: expected {generation_limit_w}, got {actual_gen}",
                )

        # Check setGradW if specified
        if set_grad_w is not None:
            actual_grad_w = dderc.setGradW
            if actual_grad_w != set_grad_w:
                return CheckResult(
                    False,
                    f"DefaultDERControl setGradW mismatch: expected {set_grad_w}, got {actual_grad_w}",
                )

    # All checks passed
    return CheckResult(True, None)


def check_der_program(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified DERProgram's in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    primacy: int | None = resolved_parameters.get("primacy", None)
    fsa_index: int | None = resolved_parameters.get("fsa_index", None)

    resource_store = context.discovered_resources(step)
    all_der_programs = resource_store.get(CSIPAusResource.DERProgram)

    # Perform filtering
    matching_der_programs: list[StoredResource] = []
    for derp_sr in all_der_programs:
        derp = cast(DERProgramResponse, derp_sr.resource)

        # Filter by primacy if specified
        if primacy is not None and derp.primacy != primacy:
            continue

        # Filter by FSA index if specified
        if fsa_index is not None:
            # Get the FunctionSetAssignments relevant to these DERPrograms
            derp_list = resource_store.get_ancestor_of(CSIPAusResource.FunctionSetAssignments, derp_sr)
            if derp_list is None or derp_list.resource_type != CSIPAusResource.DERProgramList:
                continue

            # Get the parent FunctionSetAssignment
            fsa = derp_list.parent
            if fsa is None or fsa.resource_type != CSIPAusResource.FunctionSetAssignments:
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

    # All checks pass
    return CheckResult(True, None)
