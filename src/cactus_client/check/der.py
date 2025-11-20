from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERProgramResponse
from envoy_schema.server.schema.sep2.der import DefaultDERControl
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
                continue

        # Check load limit if specified
        if load_limit_w is not None:
            actual_load = sep2_to_value(dderc.DERControlBase_.opModLoadLimW)
            if actual_load != load_limit_w:
                continue

        # Check generation limit if specified
        if generation_limit_w is not None:
            actual_gen = sep2_to_value(dderc.DERControlBase_.opModGenLimW)
            if actual_gen != generation_limit_w:
                continue

        # Check setGradW if specified
        if set_grad_w is not None:
            actual_grad_w = dderc.setGradW
            if actual_grad_w != set_grad_w:
                continue

        # All checks pass
        return CheckResult(True, None)

    # If we get here, no ddercs match the criteria
    return CheckResult(
        False,
        (
            f"""No Default DER Controls were found matching the given criteria: export limit - {export_limit_w},
             load limit: {load_limit_w}, gen limit - {generation_limit_w}, setGradW - {set_grad_w}.
             {len(default_der_controls)} DER Controls were found in the database, none matching."""
        ),
    )


def check_der_program(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether there is a DERProgram in the resource store which matches the check criteria"""

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
            # Get the parent DERProgramList
            derp_list = derp_sr.parent
            if derp_list is None or derp_list.resource_type != CSIPAusResource.DERProgramList:
                continue

            # Get the parent FunctionSetAssignment
            fsa = derp_list.parent
            if fsa is None or fsa.resource_type != CSIPAusResource.FunctionSetAssignments:
                continue

            # Get all FSAs (siblings of this FSA) to determine the index
            all_fsas = resource_store.get(CSIPAusResource.FunctionSetAssignments)

            # Sort FSAs by href for consistent ordering
            sorted_fsas = sorted(all_fsas, key=lambda sr: sr.resource.href if sr.resource.href else "")

            # Find the index of this FSA
            try:
                actual_index = sorted_fsas.index(fsa)
                if actual_index != fsa_index:
                    continue
            except ValueError:
                # FSA not found in list, shouldn't happen
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

    return CheckResult(True, None)
