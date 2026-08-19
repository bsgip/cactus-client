from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.pricing import RateComponentResponse, TimeTariffIntervalResponse

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import CombinedTimeTariffIntervalListResponse


def check_rate_component(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the discovered RateComponent(s) in the resource store match the check criteria"""

    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    active_time_tariff_interval_list_link_present: bool | None = resolved_parameters.get(
        "active_time_tariff_interval_list_link_present", None
    )

    resource_store = context.discovered_resources(step)
    rate_components = resource_store.get_for_type(CSIPAusResource.RateComponent)

    if minimum_count is not None and len(rate_components) < minimum_count:
        return CheckResult(
            False,
            f"Found {len(rate_components)} RateComponents. Expected at least {minimum_count}",
        )

    if maximum_count is not None and len(rate_components) > maximum_count:
        return CheckResult(
            False,
            f"Found {len(rate_components)} RateComponents. Expected at most {maximum_count}",
        )

    if active_time_tariff_interval_list_link_present is not None:
        for rc_sr in rate_components:
            rc = cast(RateComponentResponse, rc_sr.resource)
            is_present = rc.ActiveTimeTariffIntervalListLink is not None
            if is_present != active_time_tariff_interval_list_link_present:
                return CheckResult(
                    False,
                    f"RateComponent {rc_sr.id.href()} ActiveTimeTariffIntervalListLink presence is {is_present}."
                    f" Expected {active_time_tariff_interval_list_link_present}",
                )

    return CheckResult(True, None)


def check_time_tariff_interval(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the discovered TimeTariffInterval entries (sourced from either TimeTariffIntervalList or
    CombinedTimeTariffIntervalList) match the check criteria"""

    resource: CSIPAusResource = resolved_parameters["resource"]
    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    rate_component_link_resolves: bool | None = resolved_parameters.get("rate_component_link_resolves", None)
    distinct_rate_component_link_count: int | None = resolved_parameters.get("distinct_rate_component_link_count", None)
    consumption_tariff_interval_list_summary_present: bool | None = resolved_parameters.get(
        "consumption_tariff_interval_list_summary_present", None
    )

    resource_store = context.discovered_resources(step)

    if resource == CSIPAusResource.CombinedTimeTariffIntervalList:
        # CombinedTimeTariffIntervalList's resource-tree parent is TariffProfile (not a list resource), so
        # discovery doesn't unpack its TimeTariffInterval items into individual store entries. Read them
        # directly off the stored CombinedTimeTariffIntervalListResponse object instead.
        combined_lists = resource_store.get_for_type(CSIPAusResource.CombinedTimeTariffIntervalList)
        ttis = [
            tti
            for combined_sr in combined_lists
            for tti in (cast(CombinedTimeTariffIntervalListResponse, combined_sr.resource).TimeTariffInterval or [])
        ]
    elif resource == CSIPAusResource.TimeTariffIntervalList:
        ttis = [
            cast(TimeTariffIntervalResponse, sr.resource)
            for sr in resource_store.get_for_type(CSIPAusResource.TimeTariffInterval)
        ]
    else:
        return CheckResult(False, f"time-tariff-interval check does not support resource {resource}")

    if minimum_count is not None and len(ttis) < minimum_count:
        return CheckResult(
            False,
            f"Found {len(ttis)} TimeTariffIntervals under {resource}. Expected at least {minimum_count}",
        )

    if maximum_count is not None and len(ttis) > maximum_count:
        return CheckResult(
            False,
            f"Found {len(ttis)} TimeTariffIntervals under {resource}. Expected at most {maximum_count}",
        )

    if rate_component_link_resolves or distinct_rate_component_link_count is not None:
        rate_component_hrefs = {
            rc_sr.resource.href for rc_sr in resource_store.get_for_type(CSIPAusResource.RateComponent)
        }

        if rate_component_link_resolves:
            for tti in ttis:
                href = tti.RateComponentLink.href if tti.RateComponentLink else None
                if not href or href not in rate_component_hrefs:
                    return CheckResult(
                        False,
                        f"TimeTariffInterval {tti.href} RateComponentLink {href} does not resolve to a"
                        " discovered RateComponent",
                    )

        if distinct_rate_component_link_count is not None:
            distinct_hrefs = {tti.RateComponentLink.href for tti in ttis if tti.RateComponentLink}
            if len(distinct_hrefs) != distinct_rate_component_link_count:
                return CheckResult(
                    False,
                    f"Found {len(distinct_hrefs)} distinct RateComponentLink hrefs under {resource}."
                    f" Expected {distinct_rate_component_link_count}",
                )

    if consumption_tariff_interval_list_summary_present:
        for tti in ttis:
            summary = tti.ConsumptionTariffIntervalListSummary
            if summary is None or summary.results < 1:
                return CheckResult(
                    False,
                    f"TimeTariffInterval {tti.href} is missing a ConsumptionTariffIntervalListSummary with"
                    " results >= 1",
                )

    return CheckResult(True, None)
