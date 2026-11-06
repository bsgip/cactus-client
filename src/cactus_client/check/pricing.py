from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.pricing import (
    TariffProfileResponse,
    TimeTariffIntervalResponse,
)

from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import (
    ResourceStore,
    StoredResource,
    get_link_href,
)


def is_received_via_subscription(
    sr: StoredResource, sub_id: str, step: StepExecution, context: ExecutionContext
) -> bool:
    """Returns True if the specified StoredResource has been received via the named subscription"""
    return context.resource_annotations(step, sr.id).has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id)


def evaluate_match_counts(
    resource_name: str,
    total_found: int,
    total_matches: int,
    rejection_details: list[str],
    minimum_count: int | None,
    maximum_count: int | None,
) -> CheckResult:
    """Generates a CheckResult by comparing total_matches against the (optional) minimum/maximum counts"""
    metadata = f"Found {total_found} {resource_name}s, {total_matches} matched criteria"
    rejection_info = f" Rejections: {'; '.join(rejection_details)}" if rejection_details else ""

    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(False, f"{metadata}. Expected at least {minimum_count}.{rejection_info}")

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(False, f"{metadata}. Expected at most {maximum_count}")

    return CheckResult(True, metadata)


def check_tariff_profile(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the discovered TariffProfile(s) in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    primacy: int | None = resolved_parameters.get("primacy", None)
    sub_id: str | None = resolved_parameters.get("sub_id", None)

    tariff_profiles = context.discovered_resources(step).get_for_type(CSIPAusResource.TariffProfile)

    total_matches = 0
    rejection_details: list[str] = []
    for tp_sr in tariff_profiles:
        actual_primacy = cast(TariffProfileResponse, tp_sr.resource).primacyType
        if primacy is not None and actual_primacy != primacy:
            rejection_details.append(f"{tp_sr.id.href()}: primacy {actual_primacy} != expected {primacy}")
            continue

        if sub_id is not None and not is_received_via_subscription(tp_sr, sub_id, step, context):
            rejection_details.append(f"{tp_sr.id.href()}: not received via subscription {sub_id}")
            continue

        total_matches += 1

    return evaluate_match_counts(
        "TariffProfile", len(tariff_profiles), total_matches, rejection_details, minimum_count, maximum_count
    )


def check_rate_component(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the discovered RateComponent(s) in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    sub_id: str | None = resolved_parameters.get("sub_id", None)

    rate_components = context.discovered_resources(step).get_for_type(CSIPAusResource.RateComponent)

    total_matches = 0
    rejection_details: list[str] = []
    for rc_sr in rate_components:
        if sub_id is not None and not is_received_via_subscription(rc_sr, sub_id, step, context):
            rejection_details.append(f"{rc_sr.id.href()}: not received via subscription {sub_id}")
            continue

        total_matches += 1

    return evaluate_match_counts(
        "RateComponent", len(rate_components), total_matches, rejection_details, minimum_count, maximum_count
    )


def rate_component_hrefs_for_tti(store: ResourceStore, tti_sr: StoredResource) -> list[str]:
    """Finds the hrefs of the RateComponents in the RateComponentList of tti_sr's parent TariffProfile.

    A TimeTariffInterval is discovered through its TariffProfile's CombinedTimeTariffIntervalList so it has no
    RateComponent ancestor - the only association is via TimeTariffInterval.RateComponentLink. The store retains the
    order that items were received so this will match the RateComponentList order."""
    tp_sr = store.get_ancestor_of(CSIPAusResource.TariffProfile, tti_sr.id)
    if tp_sr is None:
        return []

    return [rc_sr.id.href() for rc_sr in store.get_descendents_of(CSIPAusResource.RateComponent, tp_sr.id)]


def check_time_tariff_interval(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the discovered TimeTariffInterval(s) in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    sub_id: str | None = resolved_parameters.get("sub_id", None)
    rate_component_index: int | None = resolved_parameters.get("rate_component_index", None)

    resource_store = context.discovered_resources(step)
    ttis = resource_store.get_for_type(CSIPAusResource.TimeTariffInterval)

    total_matches = 0
    rejection_details: list[str] = []
    for tti_sr in ttis:
        if sub_id is not None and not is_received_via_subscription(tti_sr, sub_id, step, context):
            rejection_details.append(f"{tti_sr.id.href()}: not received via subscription {sub_id}")
            continue

        if rate_component_index is not None:
            actual_href = get_link_href(cast(TimeTariffIntervalResponse, tti_sr.resource).RateComponentLink)
            rc_hrefs = rate_component_hrefs_for_tti(resource_store, tti_sr)
            expected_href = rc_hrefs[rate_component_index] if rate_component_index < len(rc_hrefs) else None
            if expected_href is None or actual_href != expected_href:
                rejection_details.append(
                    f"{tti_sr.id.href()}: RateComponentLink {actual_href} != RateComponent at index"
                    f" {rate_component_index} ({expected_href})"
                )
                continue

        total_matches += 1

    return evaluate_match_counts(
        "TimeTariffInterval", len(ttis), total_matches, rejection_details, minimum_count, maximum_count
    )


def find_invalid_tti_references(store: ResourceStore, tti_sr: StoredResource) -> list[str]:
    """Generates a human readable description of every reference on tti_sr that doesn't resolve to a discovered
    resource. Returns an empty list if all references are valid."""
    tti = cast(TimeTariffIntervalResponse, tti_sr.resource)
    invalid_references: list[str] = []

    rc_href = get_link_href(tti.RateComponentLink)
    if rc_href is None or rc_href not in rate_component_hrefs_for_tti(store, tti_sr):
        invalid_references.append(f"{tti_sr.id.href()}: RateComponentLink {rc_href} isn't a discovered RateComponent")

    summary = tti.ConsumptionTariffIntervalListSummary
    summary_ctis = (summary.ConsumptionTariffInterval if summary else None) or []
    if not summary_ctis:
        invalid_references.append(f"{tti_sr.id.href()}: ConsumptionTariffIntervalListSummary has no prices")

    discovered_cti_hrefs = {
        cti_sr.id.href() for cti_sr in store.get_descendents_of(CSIPAusResource.ConsumptionTariffInterval, tti_sr.id)
    }
    for cti in summary_ctis:
        if cti.href not in discovered_cti_hrefs:
            invalid_references.append(
                f"{tti_sr.id.href()}: ConsumptionTariffIntervalListSummary entry {cti.href} isn't a discovered"
                " ConsumptionTariffInterval"
            )

    return invalid_references


def check_time_tariff_interval_valid_references(step: StepExecution, context: ExecutionContext) -> CheckResult:
    """Checks that every discovered TimeTariffInterval has a ConsumptionTariffIntervalListSummary and RateComponentLink
    that point to already discovered ConsumptionTariffIntervals / RateComponents"""

    resource_store = context.discovered_resources(step)
    ttis = resource_store.get_for_type(CSIPAusResource.TimeTariffInterval)

    invalid_references = [ref for tti_sr in ttis for ref in find_invalid_tti_references(resource_store, tti_sr)]
    if invalid_references:
        return CheckResult(False, f"Found {len(invalid_references)} bad references: {'; '.join(invalid_references)}")

    return CheckResult(True, f"All references valid for {len(ttis)} TimeTariffIntervals")
