from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlResponse, DERProgramResponse, DefaultDERControl
from envoy_schema.server.schema.sep2.der_control_types import ActivePower

from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import StoredResource
from cactus_client.sep2 import hex_binary_equal


def sep2_to_value(ap: ActivePower | None) -> float | None:
    if ap is None:
        return None

    return ap.value * pow(10, ap.multiplier)


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


def get_latest_derc(dercs: list[StoredResource]) -> StoredResource | None:

    latest_creation_time = -1
    latest: StoredResource | None = None
    for derc in dercs:
        resource = cast(DERControlResponse, derc.resource)
        if resource.creationTime > latest_creation_time:
            latest_creation_time = resource.creationTime
            latest = derc
    return latest


def check_der_control(  # noqa: C901 # This complexity is from the long line of filtering - can't do much about it
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified DERControl's in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    latest: bool = resolved_parameters.get("latest", False)
    import_limit_w: float | None = resolved_parameters.get("opModImpLimW", None)
    export_limit_w: float | None = resolved_parameters.get("opModExpLimW", None)
    load_limit_w: float | None = resolved_parameters.get("opModLoadLimW", None)
    generation_limit_w: float | None = resolved_parameters.get("opModGenLimW", None)
    energize: bool | None = resolved_parameters.get("opModEnergize", None)
    connect: bool | None = resolved_parameters.get("opModConnect", None)
    fixed_w: float | None = resolved_parameters.get("opModFixedW", None)
    ramp_tms: int | None = resolved_parameters.get("rampTms", None)
    randomize_start: int | None = resolved_parameters.get("randomizeStart", None)
    event_status: int | None = resolved_parameters.get("event_status", None)
    response_required: int | None = resolved_parameters.get("responseRequired", None)
    derp_primacy: int | None = resolved_parameters.get("responseRequired", None)

    resource_store = context.discovered_resources(step)

    # Get our list of candidate DERControls to examine
    all_dercontrols = resource_store.get(CSIPAusResource.DERControl)
    if latest:
        latest_derc = get_latest_derc(all_dercontrols)
        if latest_derc is None:
            all_dercontrols = []
        else:
            all_dercontrols = [latest_derc]

    # Perform filtering
    matching_der_controls: list[StoredResource] = []
    for derc_sr in all_dercontrols:
        derc = cast(DERControlResponse, derc_sr.resource)

        if import_limit_w is not None and import_limit_w != sep2_to_value(derc.DERControlBase_.opModImpLimW):
            continue

        if export_limit_w is not None and export_limit_w != sep2_to_value(derc.DERControlBase_.opModExpLimW):
            continue

        if load_limit_w is not None and load_limit_w != sep2_to_value(derc.DERControlBase_.opModLoadLimW):
            continue

        if generation_limit_w is not None and generation_limit_w != sep2_to_value(derc.DERControlBase_.opModGenLimW):
            continue

        if energize is not None and energize != derc.DERControlBase_.opModEnergize:
            continue

        if connect is not None and connect != derc.DERControlBase_.opModConnect:
            continue

        if fixed_w is not None and fixed_w != derc.DERControlBase_.opModFixedW:
            continue

        if ramp_tms is not None and ramp_tms != derc.DERControlBase_.rampTms:
            continue

        if randomize_start is not None and randomize_start != derc.randomizeStart:
            continue

        if event_status is not None and event_status != derc.EventStatus_.currentStatus:
            continue

        if response_required is not None and not hex_binary_equal(event_status, derc.responseRequired):
            continue

        if derp_primacy is not None:
            if derc_sr.parent is None:
                raise CactusClientException(
                    f"DERControl {derc.href} {derc.mRID} has no link to a parent DERControlList"
                )
            if derc_sr.parent.parent is None:
                raise CactusClientException(f"DERControl {derc.href} {derc.mRID} has no link to a parent DERProgram")
            if cast(DERProgramResponse, derc_sr.parent.parent.resource).primacy != derp_primacy:
                continue

        matching_der_controls.append(derc_sr)

    # Figure out our match criteria
    total_matches = len(matching_der_controls)
    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(
            False, f"Matched {total_matches} DERControls against criteria. Expected at least {minimum_count}"
        )

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(
            False, f"Matched {total_matches} DERControls against criteria. Expected at most {maximum_count}"
        )

    return CheckResult(True, None)
