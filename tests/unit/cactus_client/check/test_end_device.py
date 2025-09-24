import unittest.mock as mock
from datetime import datetime, timezone
from typing import Any, Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceResponse,
    RegistrationResponse,
)

from cactus_client.check.end_device import check_end_device
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


@pytest.mark.parametrize(
    "resolved_params, edevs_with_registrations, client_lfdi, client_sfdi, client_pin, expected_result, expected_warns",
    [
        # Empty store checks
        ({"matches_client": True}, [], "ABC123", 456, 789, False, False),
        ({"matches_client": True, "matches_pin": True}, [], "ABC123", 456, 789, False, False),
        ({"matches_client": False}, [], "ABC123", 456, 789, True, False),
        ({"matches_client": False, "matches_pin": True}, [], "ABC123", 456, 789, True, False),
        # Store has data
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=789),
                )
            ],
            "ABC123",
            456,
            789,
            True,
            False,
        ),  # Match with a pin
        (
            {"matches_client": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    None,
                )
            ],
            "ABC123",
            456,
            789,
            True,
            False,
        ),  # Match without a pin
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=4567),
                    generate_class_instance(RegistrationResponse, pIN=789),
                )
            ],
            "ABC123",
            456,
            789,
            True,
            True,
        ),  # sfdi mismatch
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="abc123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=789),
                )
            ],
            "ABC123",
            456,
            789,
            True,
            True,
        ),  # Case mismatch
        (
            {"matches_client": False, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=789),
                )
            ],
            "ABC123",
            456,
            789,
            False,
            False,
        ),  # The client exists and matches but matches_client is False
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=7891),
                )
            ],
            "ABC123",
            456,
            789,
            False,
            False,
        ),  # pin mismatch
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    None,
                )
            ],
            "ABC123",
            456,
            789,
            False,
            False,
        ),  # pin doesn't exist
        (
            {"matches_client": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=7891),
                )
            ],
            "ABC123",
            456,
            789,
            True,
            False,
        ),  # pin mismatch is OK if we're not asserting it
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, seed=101, lFDI="def456", sFDI=123),
                    generate_class_instance(RegistrationResponse, seed=101, pIN=123),
                ),
                (
                    generate_class_instance(EndDeviceResponse, seed=202, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, seed=202, pIN=789),
                ),
            ],
            "ABC123",
            456,
            789,
            True,
            False,
        ),  # Match multiple clients
    ],
)
def test_check_end_device(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    resolved_params: dict[str, Any],
    edevs_with_registrations: list[tuple[EndDeviceResponse | None, RegistrationResponse | None]],
    client_lfdi: str,
    client_sfdi: int,
    client_pin: int,
    expected_result: bool,
    expected_warns: bool,
):
    """check_end_device should be able to handle all sorts of resource store configurations / parameters"""

    context, step = testing_contexts_factory(mock.Mock())
    context.clients_by_alias[step.client_alias].client_config = generate_class_instance(
        ClientConfig, lfdi=client_lfdi, sfdi=client_sfdi, pin=client_pin
    )

    store = context.discovered_resources(step)
    for edev, reg in edevs_with_registrations:
        if edev is not None:
            sr_edev = store.append_resource(CSIPAusResource.EndDevice, None, edev)
        else:
            sr_edev = None

        if reg is not None:
            store.append_resource(CSIPAusResource.Registration, sr_edev, reg)

    result = check_end_device(resolved_params, step, context)
    assert_check_result(result, expected_result)

    if expected_warns:
        assert len(context.warnings.warnings) > 0
    else:
        assert len(context.warnings.warnings) == 0
