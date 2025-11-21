from unittest import mock
import pytest
from cactus_client.check.der import check_der_program
from assertical.fake.generator import generate_class_instance
from envoy_schema.server.schema.sep2.der import DERProgramResponse, DERProgramListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import FunctionSetAssignmentsResponse
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_client.model.execution import CheckResult


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
        ([1], {"primacy": 2}, False),
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
def test_check_der_program_combinations_no_fsa(testing_contexts_factory, stored_programs, check_params, should_pass):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    for i, primacy in enumerate(stored_programs):
        derp = generate_class_instance(DERProgramResponse, primacy=primacy, href=f"/derp/{i+1}")
        resource_store.upsert_resource(CSIPAusResource.DERProgram, None, derp)

    # Act
    result = check_der_program(check_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is should_pass


def test_check_der_program_no_programs_in_store(testing_contexts_factory):
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resolved_params = {"minimum_count": 1}

    # Act
    result = check_der_program(resolved_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is False


def test_check_der_program_fsa_index_order_independence(testing_contexts_factory):
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
        derp_list_sr = resource_store1.upsert_resource(CSIPAusResource.DERProgramList, fsa_sr, derp_list)
        resource_store1.upsert_resource(CSIPAusResource.DERProgram, derp_list_sr, derp)

    # Second context: add in reverse order 2, 1, 0
    context2, step2 = testing_contexts_factory(mock.Mock())
    resource_store2 = context2.discovered_resources(step2)

    for fsa, derp_list, derp in reversed(fsa_data):
        fsa_sr = resource_store2.upsert_resource(CSIPAusResource.FunctionSetAssignments, None, fsa)
        derp_list_sr = resource_store2.upsert_resource(CSIPAusResource.DERProgramList, fsa_sr, derp_list)
        resource_store2.upsert_resource(CSIPAusResource.DERProgram, derp_list_sr, derp)

    # Act & Assert - Check each fsa_index in both contexts
    for fsa_idx in range(3):
        result1 = check_der_program({"fsa_index": fsa_idx}, step1, context1)
        result2 = check_der_program({"fsa_index": fsa_idx}, step2, context2)

        assert result1.passed
        assert result2.passed
