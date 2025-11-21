from unittest import mock
import pytest
from cactus_client.check.der import check_der_program, check_default_der_control
from assertical.fake.generator import generate_class_instance
from envoy_schema.server.schema.sep2.der import DefaultDERControl, DERControlBase, DERProgramResponse
from envoy_schema.server.schema.sep2.der_control_types import ActivePower
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_client.model.execution import CheckResult


@pytest.mark.parametrize(
    "stored_values,check_params,should_match",
    [
        # No criteria
        ({}, {}, True),
        # Single criteria
        ({"opModExpLimW": 5000.0}, {"opModExpLimW": 5000.0}, True),
        ({"opModExpLimW": 5000.0}, {"opModExpLimW": 3000.0}, False),
        ({"opModLoadLimW": 3000.0}, {"opModLoadLimW": 3000.0}, True),
        ({"opModLoadLimW": 3000.0}, {"opModLoadLimW": 9999.0}, False),
        ({"opModGenLimW": 4000.0}, {"opModGenLimW": 4000.0}, True),
        ({"opModGenLimW": 4000.0}, {"opModGenLimW": 2000.0}, False),
        ({"setGradW": 100}, {"setGradW": 100}, True),
        ({"setGradW": 100}, {"setGradW": 200}, False),
        # All criteria - all match
        (
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 4000.0, "setGradW": 100},
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 4000.0, "setGradW": 100},
            True,
        ),
        # All criteria - one mismatch
        (
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 4000.0, "setGradW": 100},
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 9999.0, "setGradW": 100},
            False,
        ),
        # With multipliers
        ({"opModExpLimW": (50, 2)}, {"opModExpLimW": 5000.0}, True),
        ({"opModExpLimW": (50, 2)}, {"opModExpLimW": 500.0}, False),
        ({"opModExpLimW": (50, 2), "opModGenLimW": (40, 2)}, {"opModExpLimW": 5000.0, "opModGenLimW": 4000.0}, True),
    ],
)
def test_check_default_der_control_combinations(testing_contexts_factory, stored_values, check_params, should_match):
    """Test check_default_der_control with various combinations of criteria"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Build the DERControl and base to insert to store
    control_base_kwargs = {}
    set_grad_w = None

    for key, value in stored_values.items():
        if key == "setGradW":
            set_grad_w = value
        else:
            # Handle tuples as (value, multiplier) or plain floats
            if isinstance(value, tuple):
                control_base_kwargs[key] = ActivePower(value=value[0], multiplier=value[1])
            else:
                control_base_kwargs[key] = ActivePower(value=int(value), multiplier=0)

    control_base = generate_class_instance(DERControlBase, **control_base_kwargs)
    dderc_kwargs = {"DERControlBase_": control_base, "href": "/dderc/1"}
    if set_grad_w is not None:
        dderc_kwargs["setGradW"] = set_grad_w

    dderc = generate_class_instance(DefaultDERControl, **dderc_kwargs)
    resource_store.upsert_resource(CSIPAusResource.DefaultDERControl, None, dderc)

    # Act
    result = check_default_der_control(check_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is should_match


def test_check_default_der_control_no_controls_in_store(testing_contexts_factory):
    """Test check_default_der_control when no controls exist"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resolved_params = {"opModExpLimW": 5000.0}

    # Act
    result = check_default_der_control(resolved_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is False


def test_check_default_der_control_multiple_controls(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # First control - matches
    export_limit1 = ActivePower(value=5000, multiplier=0)
    control_base1 = generate_class_instance(DERControlBase, opModExpLimW=export_limit1)
    dderc1 = generate_class_instance(DefaultDERControl, DERControlBase_=control_base1, href="/dderc/1")

    # Second control - doesn't match
    export_limit2 = ActivePower(value=3000, multiplier=0)
    control_base2 = generate_class_instance(DERControlBase, opModExpLimW=export_limit2)
    dderc2 = generate_class_instance(DefaultDERControl, DERControlBase_=control_base2, href="/dderc/2")

    resource_store.upsert_resource(CSIPAusResource.DefaultDERControl, None, dderc1)
    resource_store.upsert_resource(CSIPAusResource.DefaultDERControl, None, dderc2)

    resolved_params = {"opModExpLimW": 5000.0}

    # Act
    result = check_default_der_control(resolved_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is True


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
def test_check_der_program_combinations(testing_contexts_factory, stored_programs, check_params, should_pass):
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
