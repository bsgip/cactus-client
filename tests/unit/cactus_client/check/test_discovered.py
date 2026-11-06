import unittest.mock as mock
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource, CSIPAusVersion
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.pub_sub import Subscription

from cactus_client.check.discovered import check_discovered, check_well_known_discovered
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.wellknown import WellKnown, WellKnownSchemaVersion


@pytest.mark.parametrize(
    "resolved_params, existing_resources, expected",
    [
        ({}, [], True),  # Nothing to check - always a pass
        ({"resources": ["EndDevice"]}, [], False),  # Empty store
        ({"links": ["EndDevice"]}, [], False),  # Empty store
        (
            {"resources": ["EndDevice"]},
            [(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse))],
            True,
        ),
        (
            {"links": ["EndDevice"]},
            [(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse))],
            False,
        ),  # This has the resource - but there is no "link" to it (no parent list)
        (
            {"links": ["EndDevice"]},
            [
                (
                    CSIPAusResource.EndDeviceList,
                    generate_class_instance(EndDeviceListResponse),
                )
            ],
            True,
        ),
        (
            {"links": ["EndDeviceList"]},
            [
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse),
                )
            ],
            False,
        ),  # This has the parent resource - but it won't have the href set
        (
            {"links": ["EndDeviceList"]},
            [
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse, generate_relationships=True),  # include href
                )
            ],
            True,
        ),
        (
            {"links": ["EndDeviceList"], "resources": ["EndDevice"]},
            [
                (
                    CSIPAusResource.Subscription,
                    generate_class_instance(Subscription, seed=1),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(
                        DeviceCapabilityResponse, seed=2, generate_relationships=True
                    ),  # include href
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse, seed=3),
                ),
            ],
            True,
        ),
        (
            {"links": ["EndDeviceList"], "resources": ["EndDevice", "DERSettings"]},
            [
                (
                    CSIPAusResource.Subscription,
                    generate_class_instance(Subscription, seed=1),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(
                        DeviceCapabilityResponse, seed=2, generate_relationships=True
                    ),  # include href
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse, seed=3),
                ),
            ],
            False,
        ),
        (
            {"links": ["EndDeviceList", "Subscription"], "resources": ["EndDevice"]},
            [
                (
                    CSIPAusResource.Subscription,
                    generate_class_instance(Subscription, seed=1),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse, seed=2, generate_relationships=True),
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse, seed=3),
                ),
            ],
            False,
        ),
    ],
)
def test_check_discovered(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    resolved_params: dict[str, Any],
    existing_resources: list[tuple[CSIPAusResource, Resource]],
    expected: bool,
):

    # In case we typo the test parameters - we don't want to let something slip
    assert all(r in CSIPAusResource for r in resolved_params.get("links", [])), "Checking inputs to ensure validity"
    assert all(r in CSIPAusResource for r in resolved_params.get("resources", [])), "Checking to ensure validity"

    context, step = testing_contexts_factory(mock.Mock())

    store = context.discovered_resources(step)
    for resource_type, resource in existing_resources:
        store.append_resource(resource_type, None, resource)

    result = check_discovered(resolved_params, step, context)
    assert_check_result(result, expected)


NS_V12 = "https://csipaus.org/ns"  # Special case - v1.2 has no version in its namespace
NS_V13 = "https://csipaus.org/ns/v1.3"
CONFIGURED_DCAP_URI = "https://server.example:8443/my/dcap"


@pytest.mark.parametrize(
    "entries, params, expected_result",
    [
        # No filters / counts only
        ({}, {}, True),
        ({}, {"minimum_count": 1}, False),
        ({}, {"maximum_count": 0}, True),
        ({NS_V12: [], NS_V13: []}, {"minimum_count": 2, "maximum_count": 2}, True),
        ({NS_V12: [], NS_V13: []}, {"minimum_count": 3}, False),
        ({NS_V12: [], NS_V13: []}, {"maximum_count": 1}, False),
        # Filter by versions
        ({NS_V12: [], NS_V13: []}, {"versions": ["v1.2"], "minimum_count": 1, "maximum_count": 1}, True),
        ({NS_V13: []}, {"versions": ["v1.2"], "minimum_count": 1}, False),
        ({NS_V13: []}, {"versions": ["v1.2"], "maximum_count": 0}, True),
        ({NS_V12: [], NS_V13: []}, {"versions": ["v1.2", "v1.3"], "minimum_count": 2, "maximum_count": 2}, True),
        ({NS_V12: [], NS_V13: []}, {"versions": [CSIPAusVersion.RELEASE_1_3], "minimum_count": 1}, True),
        (
            {"https://csipaus.org/ns/v1.3-beta/storage": []},
            {"versions": ["v1.3-beta/storage"], "minimum_count": 1},
            True,
        ),
        ({"v1.2": []}, {"versions": ["v1.2"], "minimum_count": 1}, False),  # Keys are namespaces, not version labels
        ({"https://csipaus.org/ns/v1.2": []}, {"versions": ["v1.2"], "minimum_count": 1}, False),
        ({NS_V12: []}, {"versions": [], "maximum_count": 0}, True),
        # Filter by dcap
        ({NS_V13: ["/my/dcap"]}, {"matches_dcap": True, "minimum_count": 1}, True),
        ({NS_V13: [CONFIGURED_DCAP_URI]}, {"matches_dcap": True, "minimum_count": 1}, True),
        ({NS_V13: ["/other", "/my/dcap"]}, {"matches_dcap": True, "minimum_count": 1}, True),
        ({NS_V13: ["/other"]}, {"matches_dcap": True, "minimum_count": 1}, False),
        ({NS_V13: ["/my/dcap/extra"]}, {"matches_dcap": True, "minimum_count": 1}, False),
        ({NS_V13: ["https://elsewhere.example/my/dcap"]}, {"matches_dcap": True, "minimum_count": 1}, False),
        ({NS_V13: ["https://server.example/my/dcap"]}, {"matches_dcap": True, "minimum_count": 1}, False),
        ({NS_V13: []}, {"matches_dcap": True, "minimum_count": 1}, False),
        ({NS_V13: ["/other"]}, {"matches_dcap": False, "minimum_count": 1}, True),  # False applies no filter
        # Both filters must match the SAME entry
        (
            {NS_V12: ["/my/dcap"], NS_V13: ["/other"]},
            {"versions": ["v1.2"], "matches_dcap": True, "minimum_count": 1},
            True,
        ),
        (
            {NS_V12: ["/other"], NS_V13: ["/my/dcap"]},
            {"versions": ["v1.2"], "matches_dcap": True, "minimum_count": 1},
            False,
        ),
    ],
)
def test_check_well_known_discovered(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    entries: dict[str, list[str]],
    params: dict[str, Any],
    expected_result: bool,
):
    context, step = testing_contexts_factory(mock.Mock())
    context.server_config = replace(context.server_config, device_capability_uri=CONFIGURED_DCAP_URI)
    context.client_context(step).discovered_well_known = WellKnown(
        supported_schema_versions={version: WellKnownSchemaVersion(dcap=dcap) for version, dcap in entries.items()}
    )

    assert_check_result(check_well_known_discovered(params, step, context), expected_result)


def test_check_well_known_discovered_nothing_discovered(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    context, step = testing_contexts_factory(mock.Mock())

    assert_check_result(check_well_known_discovered({"minimum_count": 1}, step, context), False)
    assert_check_result(check_well_known_discovered({"maximum_count": 0}, step, context), True)
