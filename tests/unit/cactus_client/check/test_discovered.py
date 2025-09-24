import unittest.mock as mock
from typing import Any, Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.pub_sub import Subscription

from cactus_client.check.discovered import check_discovered
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


@pytest.mark.parametrize(
    "resolved_params, existing_resources, expected",
    [
        ({}, [], True),  # Nothing to check - always a pass
        ({"resources": ["EndDevice"]}, [], False),  # Empty store
        ({"links": ["EndDevice"]}, [], False),  # Empty store
        ({"resources": ["EndDevice"]}, [(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse))], True),
        (
            {"links": ["EndDevice"]},
            [(CSIPAusResource.EndDevice, generate_class_instance(EndDeviceResponse))],
            False,
        ),  # This has the resource - but there is no "link" to it (no parent list)
        (
            {"links": ["EndDevice"]},
            [(CSIPAusResource.EndDeviceList, generate_class_instance(EndDeviceListResponse))],
            True,
        ),
        (
            {"links": ["EndDeviceList"]},
            [(CSIPAusResource.DeviceCapability, generate_class_instance(DeviceCapabilityResponse))],
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
                    generate_class_instance(Subscription),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse, generate_relationships=True),  # include href
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse),
                ),
            ],
            True,
        ),
        (
            {"links": ["EndDeviceList"], "resources": ["EndDevice", "DERSettings"]},
            [
                (
                    CSIPAusResource.Subscription,
                    generate_class_instance(Subscription),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse, generate_relationships=True),  # include href
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse),
                ),
            ],
            False,
        ),
        (
            {"links": ["EndDeviceList", "Subscription"], "resources": ["EndDevice"]},
            [
                (
                    CSIPAusResource.Subscription,
                    generate_class_instance(Subscription),
                ),
                (
                    CSIPAusResource.DeviceCapability,
                    generate_class_instance(DeviceCapabilityResponse, generate_relationships=True),  # include href
                ),
                (
                    CSIPAusResource.EndDevice,
                    generate_class_instance(EndDeviceResponse),
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
    assert all((r in CSIPAusResource for r in resolved_params.get("links", []))), "Checking inputs to ensure validity"
    assert all((r in CSIPAusResource for r in resolved_params.get("resources", []))), "Checking to ensure validity"

    context, step = testing_contexts_factory(mock.Mock())

    store = context.discovered_resources(step)
    for resource_type, resource in existing_resources:
        store.append_resource(resource_type, None, resource)

    result = check_discovered(resolved_params, step, context)
    assert_check_result(result, expected)
