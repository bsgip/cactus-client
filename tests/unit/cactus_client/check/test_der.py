from typing import Any, Callable
from unittest import mock

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DERProgramListResponse,
    DERProgramResponse,
)
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsResponse,
)

from cactus_client.check.der import check_der_program
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


@pytest.mark.parametrize(
    "stored_programs,check_params,should_pass",
    [
        # No filters - any programs pass
        ([1], {}, True),
        ([1, 2], {}, True),
        # Minimum count
        ([1], {"minimum_count": 1}, True),
        ([1], {"minimum_count": 2}, False),
        ([1, 2], {"minimum_count": 2}, True),
        ([1, 2], {"minimum_count": 3}, False),
        # Maximum count
        ([1], {"maximum_count": 1}, True),
        ([1], {"maximum_count": 0}, False),
        ([1, 2], {"maximum_count": 2}, True),
        ([1, 2, 3], {"maximum_count": 2}, False),
        # Primacy filter
        ([1], {"primacy": 1}, True),
        ([1], {"primacy": 2}, True),  # has no restrictions
        ([1], {"primacy": 2, "minimum_count": 1, "maximum_count": 1}, False),
        ([1, 2], {"primacy": 1}, True),
        ([1, 2, 1], {"primacy": 1}, True),
        # Min and max count together
        ([1, 2], {"minimum_count": 2, "maximum_count": 2}, True),
        ([1, 2], {"minimum_count": 1, "maximum_count": 3}, True),
        ([1], {"minimum_count": 2, "maximum_count": 3}, False),
        ([1, 2, 3, {"primacy": 4}], {"minimum_count": 2, "maximum_count": 3}, False),
        # All filters combined
        ([1, 1, 2], {"primacy": 1, "minimum_count": 2, "maximum_count": 2}, True),
        ([1, 2], {"primacy": 1, "minimum_count": 2, "maximum_count": 3}, False),
    ],
)
def test_check_der_program_combinations_no_fsa(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    stored_programs,
    check_params: dict[str, Any],
    should_pass,
):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    for i, primacy in enumerate(stored_programs):
        derp = generate_class_instance(DERProgramResponse, primacy=primacy, href=f"/derp/{i+1}")
        resource_store.upsert_resource(CSIPAusResource.DERProgram, None, derp)

    # Act
    result = check_der_program(check_params, step, context)

    # Assert
    assert_check_result(result, should_pass)


def test_check_der_program_no_programs_in_store(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resolved_params = {"minimum_count": 1}

    # Act
    result = check_der_program(resolved_params, step, context)

    # Assert
    assert_check_result(result, False)


def test_check_der_program_fsa_index_order_independence(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that fsa_index is consistent regardless of the order programs are added"""
    # Arrange - Create FSAs and DERPrograms
    fsa_data = []
    for i in range(3):
        fsa = generate_class_instance(FunctionSetAssignmentsResponse, href=f"/fsa/{i+1}")
        derp_list = generate_class_instance(DERProgramListResponse, href=f"/fsa/{i+1}/derp")
        derp = generate_class_instance(DERProgramResponse, primacy=1, href=f"/fsa/{i+1}/derp/1")
        fsa_data.append((fsa, derp_list, derp))

    # First context: add in order 0, 1, 2
    context1, step1 = testing_contexts_factory(mock.Mock())
    resource_store1 = context1.discovered_resources(step1)

    for fsa, derp_list, derp in fsa_data:
        fsa_sr = resource_store1.upsert_resource(CSIPAusResource.FunctionSetAssignments, None, fsa)
        derp_list_sr = resource_store1.upsert_resource(CSIPAusResource.DERProgramList, fsa_sr.id, derp_list)
        resource_store1.upsert_resource(CSIPAusResource.DERProgram, derp_list_sr.id, derp)

    # Second context: add in reverse order 2, 1, 0
    context2: ExecutionContext
    context2, step2 = testing_contexts_factory(mock.Mock())
    resource_store2 = context2.discovered_resources(step2)

    for fsa, derp_list, derp in reversed(fsa_data):
        fsa_sr = resource_store2.upsert_resource(CSIPAusResource.FunctionSetAssignments, None, fsa)
        derp_list_sr = resource_store2.upsert_resource(CSIPAusResource.DERProgramList, fsa_sr.id, derp_list)
        resource_store2.upsert_resource(CSIPAusResource.DERProgram, derp_list_sr.id, derp)

    # Act & Assert - Check each fsa_index in both contexts
    for fsa_idx in range(3):
        result1 = check_der_program({"fsa_index": fsa_idx}, step1, context1)
        result2 = check_der_program({"fsa_index": fsa_idx}, step2, context2)

        assert_check_result(result1, True)
        assert_check_result(result2, True)


def test_check_der_program_fsa_index_negatives(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that fsa_index supports negative values (i.e. referencing end of the list)"""

    # Arrange - Create FSAs and DERPrograms
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)

    # They will look like this:
    # FSA 1 -> DERP List 1
    #              - DERP A (primacy 11)
    #              - DERP B (primacy 12)
    #              - DERP C (primacy 13)
    # FSA 2 -> DERP List 2
    #              - DERP D (primacy 21)
    #              - DERP E (primacy 22)
    #              - DERP F (primacy 23)
    # FSA 3 -> DERP List 3
    #              - DERP G (primacy 31)
    #              - DERP H (primacy 32)
    #              - DERP I (primacy 33)
    for i in range(3):
        fsa = generate_class_instance(FunctionSetAssignmentsResponse, href=f"/fsa/{i+1}")
        derp_list = generate_class_instance(DERProgramListResponse, href=f"/fsa/{i+1}/derp")
        derp1 = generate_class_instance(DERProgramResponse, primacy=((i + 1) * 10) + 1, href=f"/fsa/{i+1}/derp/1")
        derp2 = generate_class_instance(DERProgramResponse, primacy=((i + 1) * 10) + 2, href=f"/fsa/{i+1}/derp/2")
        derp3 = generate_class_instance(DERProgramResponse, primacy=((i + 1) * 10) + 3, href=f"/fsa/{i+1}/derp/3")

        fsa_sr = store.append_resource(CSIPAusResource.FunctionSetAssignments, None, fsa)
        derp_list_sr = store.append_resource(CSIPAusResource.DERProgramList, fsa_sr.id, derp_list)
        store.append_resource(CSIPAusResource.DERProgram, derp_list_sr.id, derp1)
        store.append_resource(CSIPAusResource.DERProgram, derp_list_sr.id, derp2)
        store.append_resource(CSIPAusResource.DERProgram, derp_list_sr.id, derp3)

    # Sanity check on fsa_index positive values
    assert_check_result(
        check_der_program({"fsa_index": 0, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_der_program({"fsa_index": 1, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context), False
    )
    assert_check_result(
        check_der_program({"fsa_index": 1, "primacy": 22, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_der_program({"fsa_index": 2, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context), False
    )

    # Do the negatives correctly pick the correct FSA's (and associated child DERPs) to search through
    assert_check_result(
        check_der_program({"fsa_index": -1, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context),
        False,
    )
    assert_check_result(
        check_der_program({"fsa_index": -2, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context),
        False,
    )
    assert_check_result(
        check_der_program({"fsa_index": -3, "primacy": 11, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_der_program({"fsa_index": -3, "primacy": 21, "minimum_count": 1, "maximum_count": 1}, step, context),
        False,
    )
    assert_check_result(
        check_der_program({"fsa_index": -2, "primacy": 21, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_der_program({"fsa_index": -1, "primacy": 21, "minimum_count": 1, "maximum_count": 1}, step, context),
        False,
    )
    assert_check_result(
        check_der_program({"fsa_index": -1, "primacy": 31, "minimum_count": 1, "maximum_count": 1}, step, context),
        True,
    )


def test_check_der_program_sub_id(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that sub_id filtering works"""

    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Setup store an initial sub tags
    derp1 = resource_store.upsert_resource(
        CSIPAusResource.DERProgram, None, generate_class_instance(DERProgramResponse, seed=1)
    )
    derp2 = resource_store.upsert_resource(
        CSIPAusResource.DERProgram, None, generate_class_instance(DERProgramResponse, seed=2)
    )
    resource_store.upsert_resource(
        CSIPAusResource.DERProgram, None, generate_class_instance(DERProgramResponse, seed=3)
    )
    derp4 = resource_store.upsert_resource(
        CSIPAusResource.DERProgram, None, generate_class_instance(DERProgramResponse, seed=4)
    )

    context.resource_annotations(step, derp1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")
    context.resource_annotations(step, derp1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub2")

    context.resource_annotations(step, derp2.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    context.resource_annotations(step, derp4.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    # Perform queries
    assert_check_result(
        check_der_program({"minimum_count": 3, "maximum_count": 3, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_der_program({"minimum_count": 0, "maximum_count": 5, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_der_program({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub1"}, step, context), False
    )
    assert_check_result(
        check_der_program({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub2"}, step, context), True
    )
    assert_check_result(
        check_der_program({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub3"}, step, context), False
    )
    assert_check_result(
        check_der_program({"minimum_count": 0, "maximum_count": 0, "sub_id": "sub3"}, step, context), True
    )
