from typing import Any

from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.check.end_device import match_end_device_on_lfdi_caseless
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


def check_function_set_assignment(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified FunctionSetAssignment's in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    matches_client_edev: bool = resolved_parameters.get("matches_client_edev", False)
    sub_id: str | None = resolved_parameters.get("sub_id", None)

    store = context.discovered_resources(step)
    client_config = context.client_config(step)

    if matches_client_edev:
        matched_edev = match_end_device_on_lfdi_caseless(store, client_config.lfdi)
        if matched_edev is None:
            return CheckResult(False, f"Expected to find an EndDevice with lfdi {client_config.lfdi} but got none.")
    else:
        matched_edev = None

    fsas = store.get_for_type(CSIPAusResource.FunctionSetAssignments)
    matches_found = 0
    for fsa_sr in fsas:

        # We might be ONLY looking at FSA's that are a direct descendent of this EndDevice
        if matched_edev is not None:
            if not fsa_sr.id.is_descendent_of(matched_edev.id):
                continue

        # We might be ONLY looking at FSA's that arrived via a particular subscription n
        if sub_id is not None:
            annotations = context.resource_annotations(step, fsa_sr.id)
            if not annotations.has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id):
                continue

        matches_found += 1

    if minimum_count is not None and matches_found < minimum_count:
        return CheckResult(
            False, f"FunctionSetAssignment minimum_count is {minimum_count} but only found {matches_found} matches."
        )

    if maximum_count is not None and matches_found > maximum_count:
        return CheckResult(
            False, f"FunctionSetAssignment maximum_count is {maximum_count} but only found {matches_found} matches."
        )

    return CheckResult(True, None)
