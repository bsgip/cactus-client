from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DefaultDERControl,
    DERControlResponse,
    DERProgramResponse,
)
from envoy_schema.server.schema.sep2.der_control_types import ActivePower
from envoy_schema.server.schema.sep2.response import ResponseType

from cactus_client.error import CactusClientError
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import StoredResource
from cactus_client.sep2 import hex_binary_equal


def sep2_to_value(ap: ActivePower | None) -> float | None:
    if ap is None:
        return None

    return ap.value * pow(10, ap.multiplier)


def check_default_der_control(  # noqa: C901 # This complexity is from the long line of filtering - can't do much about it
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether there is a DefaultDERControl in the resource store that matches the check criteria"""

    # Setup
    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    export_limit_w: float | None = resolved_parameters.get("opModExpLimW", None)
    import_limit_w: float | None = resolved_parameters.get("opModImpLimW", None)
    load_limit_w: float | None = resolved_parameters.get("opModLoadLimW", None)
    generation_limit_w: float | None = resolved_parameters.get("opModGenLimW", None)
    set_grad_w: int | None = resolved_parameters.get("setGradW", None)
    sub_id: str | None = resolved_parameters.get("sub_id", None)
    derp_primacy: int | None = resolved_parameters.get("derp_primacy", None)

    resource_store = context.discovered_resources(step)
    default_der_controls = resource_store.get_for_type(CSIPAusResource.DefaultDERControl)

    if not default_der_controls:
        return CheckResult(False, "No DefaultDERControl found in resource store")

    # Check each DefaultDERControl (typically there should be only one)
    total_matches = 0
    rejection_details: list[str] = []
    for dderc_sr in default_der_controls:
        dderc = cast(DefaultDERControl, dderc_sr.resource)

        if import_limit_w is not None:
            actual_import = sep2_to_value(dderc.DERControlBase_.opModImpLimW)
            if actual_import != import_limit_w:
                rejection_details.append(f"{dderc.href}: opModImpLimW {actual_import} != expected {import_limit_w}")
                continue

        if export_limit_w is not None:
            actual_export = sep2_to_value(dderc.DERControlBase_.opModExpLimW)
            if actual_export != export_limit_w:
                rejection_details.append(f"{dderc.href}: opModExpLimW {actual_export} != expected {export_limit_w}")
                continue

        if load_limit_w is not None:
            actual_load = sep2_to_value(dderc.DERControlBase_.opModLoadLimW)
            if actual_load != load_limit_w:
                rejection_details.append(f"{dderc.href}: opModLoadLimW {actual_load} != expected {load_limit_w}")
                continue

        if generation_limit_w is not None:
            actual_gen = sep2_to_value(dderc.DERControlBase_.opModGenLimW)
            if actual_gen != generation_limit_w:
                rejection_details.append(f"{dderc.href}: opModGenLimW {actual_gen} != expected {generation_limit_w}")
                continue

        if set_grad_w is not None:
            actual_grad_w = dderc.setGradW
            if actual_grad_w != set_grad_w:
                rejection_details.append(f"{dderc.href}: setGradW {actual_grad_w} != expected {set_grad_w}")
                continue

        if sub_id is not None:
            annotations = context.resource_annotations(step, dderc_sr.id)
            if not annotations.has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id):
                rejection_details.append(f"{dderc.href}: not received via subscription {sub_id}")
                continue

        if derp_primacy is not None:
            parent_derp_sr = resource_store.get_ancestor_of(CSIPAusResource.DERProgram, dderc_sr.id)
            if parent_derp_sr is None:
                raise CactusClientError(f"DERControl {dderc.href} {dderc.mRID} has no link to a parent DERProgram")
            actual_derp_primacy = cast(DERProgramResponse, parent_derp_sr.resource).primacy
            if actual_derp_primacy != derp_primacy:
                rejection_details.append(
                    f"{dderc.href}: parent DERProgram primacy {actual_derp_primacy} != expected {derp_primacy}"
                )
                continue

        total_matches += 1

    total_found = len(default_der_controls)
    metadata = f"Found {total_found} DefaultDERControls, {total_matches} matched criteria"
    rejection_info = f" Rejections: {'; '.join(rejection_details)}" if rejection_details else ""

    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(False, f"{metadata}. Expected at least {minimum_count}.{rejection_info}")

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(False, f"{metadata}. Expected at most {maximum_count}")

    return CheckResult(True, metadata)


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
    derp_primacy: int | None = resolved_parameters.get("derp_primacy", None)
    sub_id: str | None = resolved_parameters.get("sub_id", None)
    duration: int | None = resolved_parameters.get("duration", None)

    resource_store = context.discovered_resources(step)

    # Get our list of candidate DERControls to examine
    all_dercontrols = resource_store.get_for_type(CSIPAusResource.DERControl)
    if latest:
        latest_derc = get_latest_derc(all_dercontrols)
        if latest_derc is None:
            all_dercontrols = []
        else:
            all_dercontrols = [latest_derc]

    # Perform filtering
    total_matches = 0
    rejection_details: list[str] = []
    for derc_sr in all_dercontrols:
        derc = cast(DERControlResponse, derc_sr.resource)

        if import_limit_w is not None:
            actual_import = sep2_to_value(derc.DERControlBase_.opModImpLimW)
            if import_limit_w != actual_import:
                rejection_details.append(f"{derc.href}: opModImpLimW {actual_import} != expected {import_limit_w}")
                continue

        if export_limit_w is not None:
            actual_export = sep2_to_value(derc.DERControlBase_.opModExpLimW)
            if export_limit_w != actual_export:
                rejection_details.append(f"{derc.href}: opModExpLimW {actual_export} != expected {export_limit_w}")
                continue

        if load_limit_w is not None:
            actual_load = sep2_to_value(derc.DERControlBase_.opModLoadLimW)
            if load_limit_w != actual_load:
                rejection_details.append(f"{derc.href}: opModLoadLimW {actual_load} != expected {load_limit_w}")
                continue

        if generation_limit_w is not None:
            actual_gen = sep2_to_value(derc.DERControlBase_.opModGenLimW)
            if generation_limit_w != actual_gen:
                rejection_details.append(f"{derc.href}: opModGenLimW {actual_gen} != expected {generation_limit_w}")
                continue

        if energize is not None and energize != derc.DERControlBase_.opModEnergize:
            rejection_details.append(
                f"{derc.href}: opModEnergize {derc.DERControlBase_.opModEnergize} != expected {energize}"
            )
            continue

        if connect is not None and connect != derc.DERControlBase_.opModConnect:
            rejection_details.append(
                f"{derc.href}: opModConnect {derc.DERControlBase_.opModConnect} != expected {connect}"
            )
            continue

        if fixed_w is not None and fixed_w != derc.DERControlBase_.opModFixedW:
            rejection_details.append(
                f"{derc.href}: opModFixedW {derc.DERControlBase_.opModFixedW} != expected {fixed_w}"
            )
            continue

        if ramp_tms is not None and ramp_tms != derc.DERControlBase_.rampTms:
            rejection_details.append(f"{derc.href}: rampTms {derc.DERControlBase_.rampTms} != expected {ramp_tms}")
            continue

        if randomize_start is not None and randomize_start != derc.randomizeStart:
            rejection_details.append(f"{derc.href}: randomizeStart {derc.randomizeStart} != expected {randomize_start}")
            continue

        if event_status is not None and event_status != derc.EventStatus_.currentStatus:
            rejection_details.append(
                f"{derc.href}: eventStatus {derc.EventStatus_.currentStatus} != expected {event_status}"
            )
            continue

        if response_required is not None and not hex_binary_equal(response_required, derc.responseRequired):
            rejection_details.append(
                f"{derc.href}: responseRequired {derc.responseRequired} != expected {response_required}"
            )
            continue

        if derp_primacy is not None:
            parent_derp_sr = resource_store.get_ancestor_of(CSIPAusResource.DERProgram, derc_sr.id)
            if parent_derp_sr is None:
                raise CactusClientError(f"DERControl {derc.href} {derc.mRID} has no link to a parent DERProgram")
            actual_derp_primacy = cast(DERProgramResponse, parent_derp_sr.resource).primacy
            if actual_derp_primacy != derp_primacy:
                rejection_details.append(
                    f"{derc.href}: parent DERProgram primacy {actual_derp_primacy} != expected {derp_primacy}"
                )
                continue

        if sub_id is not None:
            annotations = context.resource_annotations(step, derc_sr.id)
            if not annotations.has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id):
                rejection_details.append(f"{derc.href}: not received via subscription {sub_id}")
                continue

        if duration is not None and duration != derc.interval.duration:
            rejection_details.append(f"{derc.href}: duration {derc.interval.duration} != expected {duration}")
            continue

        total_matches += 1

    # Build metadata message
    total_found = len(all_dercontrols)
    if latest:
        metadata = f"Found {total_found} DERControls, examined latest only, {total_matches} matched criteria"
    else:
        metadata = f"Found {total_found} DERControls, {total_matches} matched criteria"
    rejection_info = f" Rejections: {'; '.join(rejection_details)}" if rejection_details else ""

    # Figure out our match criteria
    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(False, f"{metadata}. Expected at least {minimum_count}.{rejection_info}")

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(False, f"{metadata}. Expected at most {maximum_count}")

    return CheckResult(True, metadata)


def check_der_control_responses(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified DERControl's in the resource store have responses that match the check criteria"""

    sent_response_type: int = resolved_parameters["sent_response_type"]  # Mandatory param
    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)

    response_type = ResponseType(sent_response_type)
    resource_store = context.discovered_resources(step)

    total = 0
    for derc in resource_store.get_for_type(CSIPAusResource.DERControl):
        annotations = context.resource_annotations(step, derc.id)
        if annotations.has_tag(AnnotationNamespace.RESPONSES, response_type):
            total += 1

    if minimum_count is not None and total < minimum_count:
        return CheckResult(
            False,
            f"Expected at least {minimum_count} DERControls with a Response {response_type} but found {total}",
        )
    if maximum_count is not None and total > maximum_count:
        return CheckResult(
            False,
            f"Expected at most {maximum_count} DERControls with a Response {response_type} but found {total}",
        )

    return CheckResult(True, f"Found {total} DERControls with a Response {response_type}")
