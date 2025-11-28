import unittest.mock as mock
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
    FunctionSetAssignmentsResponse,
)

from cactus_client.check.function_set_assignment import check_function_set_assignment
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


@pytest.mark.parametrize(
    "under_client_edev, under_other_edev, minimum_count, maximum_count, matches_client_edev, sub_id, expected_result",
    [
        # Empty edge cases
        (None, None, None, None, None, None, True),
        (None, None, 1, None, None, None, False),
        (None, None, 0, None, None, None, True),
        (None, None, 0, 0, None, None, True),
        (None, None, 0, 0, True, "sub1", True),
        (None, None, 1, None, None, "sub1", False),
        # Single EndDevice parent
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"])],
            ),
            None,
            1,
            1,
            True,
            None,
            True,
        ),
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"])],
            ),
            None,
            0,
            1,
            None,
            None,
            True,
        ),
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), []),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), []),
                ],
            ),
            None,
            1,
            1,
            None,
            None,
            False,
        ),
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), []),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), []),
                ],
            ),
            None,
            0,
            5,
            None,
            None,
            True,
        ),
        # Other edev parent
        (
            None,
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), [])],
            ),
            1,
            1,
            True,
            None,
            False,
        ),
        (
            None,
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), [])],
            ),
            1,
            1,
            None,
            None,
            True,
        ),
        # sub ID
        (
            None,
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), ["sub2", "sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=4), ["sub1"]),
                ],
            ),
            3,
            3,
            None,
            "sub1",
            True,
        ),
        (
            None,
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), ["sub2", "sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=4), ["sub1"]),
                ],
            ),
            3,
            3,
            None,
            "sub2",
            False,
        ),
        (
            None,
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), ["sub2", "sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=4), ["sub1"]),
                ],
            ),
            1,
            1,
            None,
            "sub2",
            True,
        ),
        # Multiple EndDevice parents
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), [])],
            ),
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=3),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=4), []),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=5), []),
                ],
            ),
            1,
            1,
            True,
            None,
            True,
        ),
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [(generate_class_instance(FunctionSetAssignmentsResponse, seed=2), [])],
            ),
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=3),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=4), []),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=5), []),
                ],
            ),
            1,
            1,
            False,
            None,
            False,
        ),
        # combo
        (
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=1),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=2), ["sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=3), []),
                ],
            ),
            (
                generate_class_instance(FunctionSetAssignmentsListResponse, seed=4),
                [
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=5), ["sub1"]),
                    (generate_class_instance(FunctionSetAssignmentsResponse, seed=6), ["sub1"]),
                ],
            ),
            1,
            1,
            True,
            "sub1",
            True,
        ),
    ],
)
def test_check_function_set_assignment(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    under_client_edev: (
        tuple[FunctionSetAssignmentsListResponse, list[tuple[FunctionSetAssignmentsResponse, list[str]]]] | None
    ),
    under_other_edev: (
        tuple[FunctionSetAssignmentsListResponse, list[tuple[FunctionSetAssignmentsResponse, list[str]]]] | None
    ),
    minimum_count: int | None,
    maximum_count: int | None,
    matches_client_edev: bool | None,
    sub_id: str | None,
    expected_result: bool,
):
    """The under_client_edev / under_other_edev are a complicated structure of FSAList and associated child items that
    will then be further nested under specific EndDevice's. Each of the child FSA's is tupled with a set of "sub_id"
    tags that will be preloaded into the annotations store for these resources."""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    # Build the hierarchy of resources in the store
    edev_other = store.append_resource(
        CSIPAusResource.EndDevice,
        None,
        generate_class_instance(EndDeviceResponse, seed=101, lFDI=context.client_config(step).lfdi + "FF"),
    )
    edev_match = store.append_resource(
        CSIPAusResource.EndDevice,
        None,
        generate_class_instance(EndDeviceResponse, seed=202, lFDI=context.client_config(step).lfdi),
    )

    if under_client_edev is not None:
        fsal_match = store.append_resource(
            CSIPAusResource.FunctionSetAssignmentsList, edev_match.id, under_client_edev[0]
        )
        for fsa_with_tags in under_client_edev[1]:
            fsa, fsa_tags = fsa_with_tags
            fsa_sr = store.append_resource(CSIPAusResource.FunctionSetAssignments, fsal_match.id, fsa)
            for tag in fsa_tags or []:
                context.resource_annotations(step, fsa_sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, tag)

    if under_other_edev is not None:
        fsal_other = store.append_resource(
            CSIPAusResource.FunctionSetAssignmentsList, edev_other.id, under_other_edev[0]
        )
        for fsa_with_tags in under_other_edev[1]:
            fsa, fsa_tags = fsa_with_tags
            fsa_sr = store.append_resource(CSIPAusResource.FunctionSetAssignments, fsal_other.id, fsa)
            for tag in fsa_tags or []:
                context.resource_annotations(step, fsa_sr.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, tag)

    resolved_params = {}
    if minimum_count is not None:
        resolved_params["minimum_count"] = minimum_count
    if maximum_count is not None:
        resolved_params["maximum_count"] = maximum_count
    if matches_client_edev is not None:
        resolved_params["matches_client_edev"] = matches_client_edev
    if sub_id is not None:
        resolved_params["sub_id"] = sub_id

    # Act
    result = check_function_set_assignment(resolved_params, step, context)

    # Assert
    assert_check_result(result, expected_result)
    assert len(context.warnings.warnings) == 0
