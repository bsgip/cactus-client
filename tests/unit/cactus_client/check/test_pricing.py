import unittest.mock as mock
from collections.abc import Callable
from typing import cast

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.identification import Link
from envoy_schema.server.schema.sep2.pricing import (
    ConsumptionTariffIntervalListResponse,
    ConsumptionTariffIntervalListSummaryResponse,
    ConsumptionTariffIntervalResponse,
    RateComponentListResponse,
    RateComponentResponse,
    TariffProfileListResponse,
    TariffProfileResponse,
    TimeTariffIntervalResponse,
)

from cactus_client.check.pricing import (
    check_rate_component,
    check_tariff_profile,
    check_time_tariff_interval,
    check_time_tariff_interval_valid_references,
)
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import (
    CombinedTimeTariffIntervalListResponse,
    ResourceStore,
    StoredResource,
)


def add_tariff_profile(store: ResourceStore, name: str, primacy: int = 0) -> StoredResource:
    tp_list = generate_class_instance(TariffProfileListResponse, href=f"/{name}/list")
    tp_list_sr = store.upsert_resource(CSIPAusResource.TariffProfileList, None, tp_list)
    tp = generate_class_instance(TariffProfileResponse, href=f"/{name}", primacyType=primacy)
    return store.append_resource(CSIPAusResource.TariffProfile, tp_list_sr.id, tp)


def add_rate_components(store: ResourceStore, tp_sr: StoredResource, hrefs: list[str]) -> list[StoredResource]:
    """Adds hrefs (in order) as the RateComponentList of tp_sr"""
    rc_list = generate_class_instance(RateComponentListResponse, href=f"{tp_sr.id.href()}/rc")
    rc_list_sr = store.append_resource(CSIPAusResource.RateComponentList, tp_sr.id, rc_list)
    return [
        store.append_resource(
            CSIPAusResource.RateComponent,
            rc_list_sr.id,
            generate_class_instance(RateComponentResponse, seed=idx, href=href),
        )
        for idx, href in enumerate(hrefs)
    ]


def add_time_tariff_intervals(
    store: ResourceStore, tp_sr: StoredResource, tti_rc_hrefs: list[tuple[str, str | None]]
) -> list[StoredResource]:
    """Adds (tti href, RateComponentLink href) tuples to the CombinedTimeTariffIntervalList of tp_sr"""
    combined = generate_class_instance(CombinedTimeTariffIntervalListResponse, href=f"{tp_sr.id.href()}/ctti")
    combined_sr = store.append_resource(CSIPAusResource.CombinedTimeTariffIntervalList, tp_sr.id, combined)
    return [
        store.append_resource(
            CSIPAusResource.TimeTariffInterval,
            combined_sr.id,
            generate_class_instance(
                TimeTariffIntervalResponse,
                seed=idx,
                href=href,
                RateComponentLink=Link(href=rc_href) if rc_href else None,
            ),
        )
        for idx, (href, rc_href) in enumerate(tti_rc_hrefs)
    ]


@pytest.mark.parametrize(
    "primacies, params, expected_result",
    [
        ([], {}, True),
        ([], {"minimum_count": 0}, True),
        ([], {"minimum_count": 1}, False),
        ([], {"maximum_count": 0}, True),
        ([1, 2], {}, True),
        ([1, 2], {"minimum_count": 2, "maximum_count": 2}, True),
        ([1, 2], {"minimum_count": 3}, False),
        ([1, 2], {"maximum_count": 1}, False),
        ([1, 2, 2], {"primacy": 2, "minimum_count": 2, "maximum_count": 2}, True),
        ([1, 2, 2], {"primacy": 2, "maximum_count": 1}, False),
        ([1, 2, 2], {"primacy": 3, "minimum_count": 1}, False),
        ([1, 2, 2], {"primacy": 3, "maximum_count": 0}, True),
        ([0, 2], {"primacy": 0, "minimum_count": 1, "maximum_count": 1}, True),  # Falsy filter value still filters
    ],
)
def test_check_tariff_profile(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    primacies: list[int],
    params: dict,
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    for idx, primacy in enumerate(primacies):
        add_tariff_profile(store, f"tp{idx}", primacy)

    assert_check_result(check_tariff_profile(params, step, context), expected_result)


def test_check_tariff_profile_sub_id_combined_with_primacy(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    tagged_primacy_1 = add_tariff_profile(store, "tp1", 1)
    tagged_primacy_2 = add_tariff_profile(store, "tp2", 2)
    add_tariff_profile(store, "tp3", 1)  # Untagged
    for sr in (tagged_primacy_1, tagged_primacy_2):
        context.resource_annotations(step, sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    both = {"primacy": 1, "sub_id": "sub1", "minimum_count": 1, "maximum_count": 1}
    assert_check_result(check_tariff_profile(both, step, context), True)

    sub_only = {"sub_id": "sub1", "minimum_count": 2, "maximum_count": 2}
    assert_check_result(check_tariff_profile(sub_only, step, context), True)

    other_sub = {"sub_id": "sub2", "minimum_count": 1}
    assert_check_result(check_tariff_profile(other_sub, step, context), False)


@pytest.mark.parametrize(
    "params, expected_result",
    [
        ({}, True),
        ({"minimum_count": 3, "maximum_count": 3}, True),
        ({"minimum_count": 4}, False),
        ({"maximum_count": 2}, False),
        ({"sub_id": "sub1", "minimum_count": 2, "maximum_count": 2}, True),
        ({"sub_id": "sub1", "minimum_count": 3}, False),
        ({"sub_id": "sub1", "maximum_count": 1}, False),
        ({"sub_id": "sub2", "maximum_count": 0}, True),
        ({"sub_id": "sub2", "minimum_count": 1}, False),
    ],
)
def test_check_rate_component(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    params: dict,
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    rcs = add_rate_components(store, add_tariff_profile(store, "tp1"), ["/rc1", "/rc2", "/rc3"])
    for sr in rcs[:2]:
        context.resource_annotations(step, sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    assert_check_result(check_rate_component(params, step, context), expected_result)


def test_check_rate_component_empty_store(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())

    assert_check_result(check_rate_component({"maximum_count": 0}, step, context), True)
    assert_check_result(check_rate_component({"minimum_count": 1}, step, context), False)


@pytest.mark.parametrize(
    "params, expected_result",
    [
        ({}, True),
        ({"minimum_count": 3, "maximum_count": 3}, True),
        ({"minimum_count": 4}, False),
        ({"maximum_count": 2}, False),
        ({"rate_component_index": 0, "minimum_count": 2, "maximum_count": 2}, True),
        ({"rate_component_index": 0, "maximum_count": 1}, False),
        ({"rate_component_index": 1, "minimum_count": 1, "maximum_count": 1}, True),
        ({"rate_component_index": 1, "minimum_count": 2}, False),
        ({"rate_component_index": 2, "maximum_count": 0}, True),  # Index out of range matches nothing
        ({"rate_component_index": 2, "minimum_count": 1}, False),
        ({"sub_id": "sub1", "minimum_count": 2, "maximum_count": 2}, True),
        ({"sub_id": "sub2", "minimum_count": 1}, False),
        ({"sub_id": "sub1", "rate_component_index": 0, "minimum_count": 1, "maximum_count": 1}, True),
        ({"sub_id": "sub1", "rate_component_index": 1, "minimum_count": 1, "maximum_count": 1}, True),
    ],
)
def test_check_time_tariff_interval(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    params: dict,
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    tp_sr = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp_sr, ["/rc1", "/rc2"])

    # /rc2 is deliberately linked first - the index comes from RateComponentList order, not the order of TTIs
    ttis = add_time_tariff_intervals(store, tp_sr, [("/tti1", "/rc2"), ("/tti2", "/rc1"), ("/tti3", "/rc1")])
    for sr in ttis[:2]:
        context.resource_annotations(step, sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    assert_check_result(check_time_tariff_interval(params, step, context), expected_result)


def test_check_time_tariff_interval_rate_component_index_is_per_tariff_profile(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Each TariffProfile has its own RateComponentList - indexes don't continue across them"""
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    tp1 = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp1, ["/tp1/rc1", "/tp1/rc2"])
    add_time_tariff_intervals(store, tp1, [("/tp1/tti1", "/tp1/rc1"), ("/tp1/tti2", "/tp1/rc2")])

    tp2 = add_tariff_profile(store, "tp2")
    add_rate_components(store, tp2, ["/tp2/rc1"])
    add_time_tariff_intervals(store, tp2, [("/tp2/tti1", "/tp2/rc1")])

    index_0 = {"rate_component_index": 0, "minimum_count": 2, "maximum_count": 2}
    assert_check_result(check_time_tariff_interval(index_0, step, context), True)

    index_1 = {"rate_component_index": 1, "minimum_count": 1, "maximum_count": 1}
    assert_check_result(check_time_tariff_interval(index_1, step, context), True)


def test_check_time_tariff_interval_rate_component_index_unmatched_links(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    tp_sr = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp_sr, ["/rc1"])
    add_time_tariff_intervals(store, tp_sr, [("/tti1", "/unknown"), ("/tti2", None)])

    no_matches = {"rate_component_index": 0, "maximum_count": 0}
    assert_check_result(check_time_tariff_interval(no_matches, step, context), True)
    assert_check_result(check_time_tariff_interval({"minimum_count": 2}, step, context), True)


def test_check_time_tariff_interval_empty_after_expiry(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    tp_sr = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp_sr, ["/rc1"])
    add_time_tariff_intervals(store, tp_sr, [])

    assert_check_result(check_time_tariff_interval({"minimum_count": 0, "maximum_count": 0}, step, context), True)
    assert_check_result(check_time_tariff_interval({"minimum_count": 1}, step, context), False)


def add_tti_with_references(
    store: ResourceStore,
    tp_sr: StoredResource,
    rc_href: str | None,
    summary_cti_hrefs: list[str | None] | None,
    discovered_cti_hrefs: list[str],
) -> StoredResource:
    """Adds a single TTI under tp_sr with a summary of summary_cti_hrefs. The CTIs in discovered_cti_hrefs are added
    as children of the TTI. A summary_cti_hrefs of None will generate no summary"""
    tti_sr = add_time_tariff_intervals(store, tp_sr, [(f"{tp_sr.id.href()}/tti", rc_href)])[0]

    tti = cast(TimeTariffIntervalResponse, tti_sr.resource)
    if summary_cti_hrefs is None:
        tti.ConsumptionTariffIntervalListSummary = None  # type: ignore
    else:
        tti.ConsumptionTariffIntervalListSummary = generate_class_instance(
            ConsumptionTariffIntervalListSummaryResponse,
            results=len(summary_cti_hrefs),
            ConsumptionTariffInterval=[
                generate_class_instance(ConsumptionTariffIntervalResponse, seed=idx, href=href)
                for idx, href in enumerate(summary_cti_hrefs)
            ],
        )

    cti_list = generate_class_instance(ConsumptionTariffIntervalListResponse, href=f"{tti_sr.id.href()}/cti")
    cti_list_sr = store.append_resource(CSIPAusResource.ConsumptionTariffIntervalList, tti_sr.id, cti_list)
    for idx, href in enumerate(discovered_cti_hrefs):
        cti = generate_class_instance(ConsumptionTariffIntervalResponse, seed=idx, href=href)
        store.append_resource(CSIPAusResource.ConsumptionTariffInterval, cti_list_sr.id, cti)

    return tti_sr


@pytest.mark.parametrize(
    "rc_href, summary_cti_hrefs, discovered_cti_hrefs, expected_result",
    [
        ("/rc1", ["/cti1"], ["/cti1"], True),
        ("/rc2", ["/cti1", "/cti2"], ["/cti1", "/cti2"], True),
        ("/rc1", ["/cti1"], ["/cti1", "/cti2"], True),  # Summary is allowed to be a subset
        (None, ["/cti1"], ["/cti1"], False),  # No RateComponentLink
        ("/unknown", ["/cti1"], ["/cti1"], False),  # RateComponentLink doesn't resolve
        ("/rc1", None, ["/cti1"], False),  # No summary
        ("/rc1", [], ["/cti1"], False),  # Summary has no prices
        ("/rc1", ["/cti1", "/unknown"], ["/cti1"], False),  # Partial price match
        ("/rc1", ["/cti1"], [], False),  # Prices were never discovered
        ("/rc1", [None], ["/cti1"], False),  # Summary price has no href
    ],
)
def test_check_time_tariff_interval_valid_references(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    rc_href: str | None,
    summary_cti_hrefs: list[str | None] | None,
    discovered_cti_hrefs: list[str],
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    tp_sr = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp_sr, ["/rc1", "/rc2"])
    add_tti_with_references(store, tp_sr, rc_href, summary_cti_hrefs, discovered_cti_hrefs)

    assert_check_result(check_time_tariff_interval_valid_references(step, context), expected_result)


def test_check_time_tariff_interval_valid_references_no_ttis(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())

    assert_check_result(check_time_tariff_interval_valid_references(step, context), True)


def test_check_time_tariff_interval_valid_references_scoped_to_parents(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """A reference only counts if it resolves within the TTI's own TariffProfile / ConsumptionTariffIntervalList"""
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    tp1 = add_tariff_profile(store, "tp1")
    add_rate_components(store, tp1, ["/tp1/rc1"])
    add_tti_with_references(store, tp1, "/tp1/rc1", ["/tp1/cti1"], ["/tp1/cti1"])
    assert_check_result(check_time_tariff_interval_valid_references(step, context), True)

    tp2 = add_tariff_profile(store, "tp2")
    add_rate_components(store, tp2, ["/tp2/rc1"])
    add_tti_with_references(
        store, tp2, "/tp1/rc1", ["/tp2/cti1"], ["/tp2/cti1"]
    )  # RC belongs to the wrong TariffProfile
    assert_check_result(check_time_tariff_interval_valid_references(step, context), False)

    tp3 = add_tariff_profile(store, "tp3")
    add_rate_components(store, tp3, ["/tp3/rc1"])
    add_tti_with_references(store, tp3, "/tp3/rc1", ["/tp1/cti1"], ["/tp3/cti1"])  # Price belongs to a different TTI
    result = check_time_tariff_interval_valid_references(step, context)
    assert_check_result(result, False)
    assert "/tp1/rc1" in (result.description or "") and "/tp1/cti1" in (result.description or "")
