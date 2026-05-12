import unittest.mock as mock
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import ClientType
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
    RegistrationResponse,
)

from cactus_client.check.end_device import (
    check_end_device,
    check_end_device_list,
    is_checksum_valid,
    match_aggregator_end_device,
    match_end_device_on_lfdi_caseless,
)
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import StoredResource


@pytest.mark.parametrize(
    "pin, expected",
    [
        (123455, True),  # 1+2+3+4+5=15, checksum=5
        (123450, False),  # 1+2+3+4+5=15, checksum should be 5, not 0
        (12345, False),  # 1+2+3+4=10, checksum should be 0, not 5
        (12340, True),  # 1+2+3+4=10, checksum=0
        (11, True),  # 1, checksum=1
        (10, False),  # 1, checksum should be 1, not 0
        (0, True),  # 0, checksum=0
        (99996, True),  # 9+9+9+9=36, checksum=6
        (99999, False),  # 9+9+9+9=36, checksum should be 6, not 9
    ],
)
def test_is_checksum_valid(pin: int, expected: bool):
    assert is_checksum_valid(pin) == expected


@pytest.mark.parametrize(
    "existing_edev_lfdis, lfdi, expected_idx",
    [
        ([], "", None),
        ([], "abc123", None),
        (["abc123"], "abc123", 0),
        (["abc123", "", "123ABC", "DEF456abc"], "DEF", None),
        (["abc123", "", "123ABC", "DEF456abc"], "123abc", 2),
        (["abc123", "", "123ABC", "DEF456abc"], "123ABC", 2),
        (["abc123", "", "123ABC", "DEF456abc"], "def456ABC", 3),
    ],
)
def test_match_end_device_on_lfdi_caseless(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    existing_edev_lfdis: list[str],
    lfdi: str,
    expected_idx: int | None,
):

    # Arrange
    if expected_idx is not None:
        assert expected_idx >= 0 and expected_idx < len(existing_edev_lfdis)

    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    expected_sr: StoredResource | None = None
    for idx, existing_lfdi in enumerate(existing_edev_lfdis):
        new_sr = store.append_resource(
            CSIPAusResource.EndDevice, None, generate_class_instance(EndDeviceResponse, seed=idx, lFDI=existing_lfdi)
        )
        if idx == expected_idx:
            expected_sr = new_sr

    # Act
    actual_sr = match_end_device_on_lfdi_caseless(store, lfdi)

    # Assert
    if expected_sr is not None:
        assert actual_sr is expected_sr
    else:
        assert actual_sr is None


@pytest.mark.parametrize(
    "existing_edev_lfdis, client_type, agg_edev_lfdi, expected_idx",
    [
        ([], ClientType.AGGREGATOR, "", None),
        ([], ClientType.DEVICE, "", None),
        ([], ClientType.AGGREGATOR, "abc123", None),
        ([], ClientType.DEVICE, "abc123", None),
        (["abc123"], ClientType.AGGREGATOR, "abc123", 0),
        (["abc123"], ClientType.DEVICE, "abc123", None),  # Device clients never look for an agg EndDevice
        (["abc123", "", "123ABC", "DEF456abc"], ClientType.AGGREGATOR, "DEF", None),
        (["abc123", "", "123ABC", "DEF456abc"], ClientType.AGGREGATOR, "123abc", 2),
        (["abc123", "", "123ABC", "DEF456abc"], ClientType.AGGREGATOR, "123ABC", 2),
        (["abc123", "", "123ABC", "DEF456abc"], ClientType.AGGREGATOR, "def456ABC", 3),
        (["abc123", "", "123ABC", "DEF456abc"], ClientType.DEVICE, "DEF456abc", None),
    ],
)
@mock.patch("cactus_client.check.end_device.lfdi_from_cert_file")
def test_match_aggregator_end_device(
    mock_lfdi_from_cert_file: mock.MagicMock,
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    existing_edev_lfdis: list[str],
    client_type: ClientType,
    agg_edev_lfdi: str,
    expected_idx: int | None,
):

    # Arrange
    if expected_idx is not None:
        assert expected_idx >= 0 and expected_idx < len(existing_edev_lfdis)

    # Rewrite the client config to have the nominated client type
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    client_config = replace(context.client_config(step), type=client_type)
    context.clients_by_alias[step.client_alias].client_config = client_config

    # Build the resource store
    mock_lfdi_from_cert_file.return_value = agg_edev_lfdi
    expected_sr: StoredResource | None = None
    for idx, existing_lfdi in enumerate(existing_edev_lfdis):
        new_sr = store.append_resource(
            CSIPAusResource.EndDevice, None, generate_class_instance(EndDeviceResponse, seed=idx, lFDI=existing_lfdi)
        )
        if idx == expected_idx:
            expected_sr = new_sr

    # Act
    actual_sr = match_aggregator_end_device(store, context.client_config(step))

    # Assert
    if expected_sr is not None:
        assert actual_sr is expected_sr
    else:
        assert actual_sr is None

    if client_type == ClientType.AGGREGATOR:
        mock_lfdi_from_cert_file.assert_called_once_with(client_config.certificate_file)
    else:
        mock_lfdi_from_cert_file.assert_not_called()


@pytest.mark.parametrize(
    "resolved_params, edevs_with_registrations, client_lfdi, client_sfdi, client_pin, expected_result, expected_warns",
    [
        # Empty store checks
        ({"matches_client": True}, [], "ABC123", 456, 789, False, False),
        (
            {"matches_client": True, "matches_pin": True},
            [],
            "ABC123",
            456,
            789,
            False,
            False,
        ),
        ({"matches_client": False}, [], "ABC123", 456, 789, True, False),
        (
            {"matches_client": False, "matches_pin": True},
            [],
            "ABC123",
            456,
            789,
            True,
            False,
        ),
        # Store has data
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="ABC123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=11),
                )
            ],
            "ABC123",
            456,
            11,
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
                    generate_class_instance(RegistrationResponse, pIN=11),
                )
            ],
            "ABC123",
            456,
            11,
            True,
            True,
        ),  # sfdi mismatch
        (
            {"matches_client": True, "matches_pin": True},
            [
                (
                    generate_class_instance(EndDeviceResponse, lFDI="abc123", sFDI=456),
                    generate_class_instance(RegistrationResponse, pIN=11),
                )
            ],
            "ABC123",
            456,
            11,
            True,
            False,
        ),  # Case mismatch on LFDI is OK
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
                    generate_class_instance(RegistrationResponse, seed=202, pIN=11),
                ),
            ],
            "ABC123",
            456,
            11,
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
            assert sr_edev is not None
            store.append_resource(CSIPAusResource.Registration, sr_edev.id, reg)

    result = check_end_device(resolved_params, step, context)
    assert_check_result(result, expected_result)

    if expected_warns:
        assert len(context.warnings.warnings) > 0
    else:
        assert len(context.warnings.warnings) == 0


@pytest.mark.parametrize(
    "existing_edev_lists_with_tags, min_count, max_count, poll_rate, sub_id, expected_result",
    [
        ([], None, None, None, None, True),
        ([], 0, 10, None, None, True),
        ([], 1, 10, None, None, False),
        (
            [(generate_class_instance(EndDeviceListResponse, pollRate=123), [])],
            1,
            1,
            0,
            None,
            False,
        ),
        (
            [(generate_class_instance(EndDeviceListResponse, pollRate=123), [])],
            1,
            1,
            123,
            None,
            True,
        ),
        (
            [(generate_class_instance(EndDeviceListResponse, pollRate=None), [])],
            1,
            1,
            123,
            None,
            False,
        ),
        (
            [
                (
                    generate_class_instance(EndDeviceListResponse, seed=101, pollRate=None),
                    [],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=202, pollRate=0),
                    [],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=303, pollRate=456),
                    [],
                ),
            ],
            1,
            1,
            456,
            None,
            True,
        ),
        (
            [
                (
                    generate_class_instance(EndDeviceListResponse, seed=101, pollRate=None),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=202, pollRate=0),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=303, pollRate=456),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=404, pollRate=456),
                    [],
                ),
            ],
            1,
            1,
            456,
            None,
            False,
        ),
        (
            [
                (
                    generate_class_instance(EndDeviceListResponse, seed=101, pollRate=None),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=202, pollRate=0),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=303, pollRate=456),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=404, pollRate=456),
                    [],
                ),
            ],
            1,
            1,
            456,
            "sub1",
            True,
        ),
        (
            [
                (
                    generate_class_instance(EndDeviceListResponse, seed=101, pollRate=None),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=202, pollRate=0),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=303, pollRate=456),
                    ["sub1"],
                ),
                (
                    generate_class_instance(EndDeviceListResponse, seed=404, pollRate=456),
                    [],
                ),
            ],
            3,
            3,
            None,
            "sub1",
            True,
        ),
    ],
)
def test_check_end_device_list(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    existing_edev_lists_with_tags: list[tuple[EndDeviceListResponse, list[str]]],
    min_count: int | None,
    max_count: int | None,
    poll_rate: int | None,
    sub_id: str | None,
    expected_result: bool,
):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    for edev_list, tags in existing_edev_lists_with_tags:
        sr = store.append_resource(CSIPAusResource.EndDeviceList, None, edev_list)
        for tag in tags:
            context.resource_annotations(step, sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, tag)

    resolved_params = {}
    if min_count is not None:
        resolved_params["minimum_count"] = min_count
    if max_count is not None:
        resolved_params["maximum_count"] = max_count
    if poll_rate is not None:
        resolved_params["poll_rate"] = poll_rate
    if sub_id is not None:
        resolved_params["sub_id"] = sub_id

    # Act
    result = check_end_device_list(resolved_params, step, context)

    # Assert
    assert_check_result(result, expected_result)
    assert len(context.warnings.warnings) == 0
