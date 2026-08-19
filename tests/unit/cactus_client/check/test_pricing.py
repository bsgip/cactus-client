import unittest.mock as mock
from collections.abc import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.identification import Link
from envoy_schema.server.schema.sep2.pricing import (
    ConsumptionTariffIntervalListSummaryResponse,
    RateComponentResponse,
    TimeTariffIntervalResponse,
)

from cactus_client.check.pricing import check_rate_component, check_time_tariff_interval
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import CombinedTimeTariffIntervalListResponse


@pytest.mark.parametrize(
    "rate_component_hrefs, active_links, minimum_count, maximum_count, link_present, expected_result",
    [
        (["rc1"], [None], None, None, None, True),
        ([], [], 1, None, None, False),
        ([], [], 0, None, None, True),
        (["rc1", "rc2"], [None, None], 2, 2, False, True),
        (["rc1", "rc2"], [None, "active1"], 2, 2, False, False),
        (["rc1", "rc2"], ["active1", "active2"], None, None, True, True),
        (["rc1", "rc2"], [None, "active1"], None, None, True, False),
    ],
)
def test_check_rate_component(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    rate_component_hrefs: list[str],
    active_links: list[str | None],
    minimum_count: int | None,
    maximum_count: int | None,
    link_present: bool | None,
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    for idx, (href, active_link) in enumerate(zip(rate_component_hrefs, active_links, strict=True)):
        rc = generate_class_instance(
            RateComponentResponse,
            seed=idx,
            href=href,
            ActiveTimeTariffIntervalListLink=Link(href=active_link) if active_link else None,
        )
        store.append_resource(CSIPAusResource.RateComponent, None, rc)

    resolved_params = {}
    if minimum_count is not None:
        resolved_params["minimum_count"] = minimum_count
    if maximum_count is not None:
        resolved_params["maximum_count"] = maximum_count
    if link_present is not None:
        resolved_params["active_time_tariff_interval_list_link_present"] = link_present

    result = check_rate_component(resolved_params, step, context)
    assert_check_result(result, expected_result)


def make_tti(
    seed: int, href: str, rate_component_href: str | None, summary_results: int | None
) -> TimeTariffIntervalResponse:
    return generate_class_instance(
        TimeTariffIntervalResponse,
        seed=seed,
        href=href,
        RateComponentLink=Link(href=rate_component_href) if rate_component_href else None,
        ConsumptionTariffIntervalListSummary=(
            generate_class_instance(ConsumptionTariffIntervalListSummaryResponse, seed=seed, results=summary_results)
            if summary_results is not None
            else None
        ),
    )


def test_check_time_tariff_interval_combined_list_reads_nested_ttis(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """CombinedTimeTariffIntervalList doesn't get unpacked into individual TimeTariffInterval store entries - the
    check must read .TimeTariffInterval directly off the stored CombinedTimeTariffIntervalListResponse."""
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    store.append_resource(
        CSIPAusResource.RateComponent, None, generate_class_instance(RateComponentResponse, seed=1, href="rc1")
    )
    store.append_resource(
        CSIPAusResource.RateComponent, None, generate_class_instance(RateComponentResponse, seed=2, href="rc2")
    )

    combined = generate_class_instance(
        CombinedTimeTariffIntervalListResponse,
        seed=99,
        href="combined1",
        TimeTariffInterval=[
            make_tti(1, "tti1", "rc1", 1),
            make_tti(2, "tti2", "rc2", 2),
            make_tti(3, "tti3", "rc1", 1),
        ],
    )
    store.append_resource(CSIPAusResource.CombinedTimeTariffIntervalList, None, combined)

    result = check_time_tariff_interval(
        {
            "resource": CSIPAusResource.CombinedTimeTariffIntervalList,
            "minimum_count": 3,
            "maximum_count": 3,
            "rate_component_link_resolves": True,
            "distinct_rate_component_link_count": 2,
            "consumption_tariff_interval_list_summary_present": True,
        },
        step,
        context,
    )
    assert_check_result(result, True)


def test_check_time_tariff_interval_combined_list_no_entries_after_expiry(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    combined = generate_class_instance(
        CombinedTimeTariffIntervalListResponse, seed=99, href="combined1", TimeTariffInterval=[]
    )
    store.append_resource(CSIPAusResource.CombinedTimeTariffIntervalList, None, combined)

    result = check_time_tariff_interval(
        {"resource": CSIPAusResource.CombinedTimeTariffIntervalList, "maximum_count": 0}, step, context
    )
    assert_check_result(result, True)


def test_check_time_tariff_interval_time_tariff_interval_list_uses_store(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    store.append_resource(
        CSIPAusResource.RateComponent, None, generate_class_instance(RateComponentResponse, seed=1, href="rc1")
    )
    store.append_resource(CSIPAusResource.TimeTariffInterval, None, make_tti(1, "tti1", "rc1", 1))
    store.append_resource(CSIPAusResource.TimeTariffInterval, None, make_tti(2, "tti2", "rc1", 1))

    result = check_time_tariff_interval(
        {"resource": CSIPAusResource.TimeTariffIntervalList, "minimum_count": 2, "maximum_count": 2}, step, context
    )
    assert_check_result(result, True)


def test_check_time_tariff_interval_rate_component_link_does_not_resolve(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    store.append_resource(
        CSIPAusResource.RateComponent, None, generate_class_instance(RateComponentResponse, seed=1, href="rc1")
    )

    combined = generate_class_instance(
        CombinedTimeTariffIntervalListResponse,
        seed=99,
        href="combined1",
        TimeTariffInterval=[make_tti(1, "tti1", "unknown-rc", 1)],
    )
    store.append_resource(CSIPAusResource.CombinedTimeTariffIntervalList, None, combined)

    result = check_time_tariff_interval(
        {"resource": CSIPAusResource.CombinedTimeTariffIntervalList, "rate_component_link_resolves": True},
        step,
        context,
    )
    assert_check_result(result, False)


def test_check_time_tariff_interval_missing_summary(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    combined = generate_class_instance(
        CombinedTimeTariffIntervalListResponse,
        seed=99,
        href="combined1",
        TimeTariffInterval=[make_tti(1, "tti1", "rc1", None)],
    )
    store.append_resource(CSIPAusResource.CombinedTimeTariffIntervalList, None, combined)

    result = check_time_tariff_interval(
        {
            "resource": CSIPAusResource.CombinedTimeTariffIntervalList,
            "consumption_tariff_interval_list_summary_present": True,
        },
        step,
        context,
    )
    assert_check_result(result, False)
