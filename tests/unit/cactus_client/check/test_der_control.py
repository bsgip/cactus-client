from typing import Callable
from unittest import mock

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DefaultDERControl,
    DERControlBase,
    DERControlResponse,
    DERProgramResponse,
)
from envoy_schema.server.schema.sep2.der_control_types import ActivePower
from cactus_client.check.der_controls import check_default_der_control, check_der_control
from envoy_schema.server.schema.sep2.types import DateTimeIntervalType
from cactus_client.model.context import AnnotationNamespace, ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from envoy_schema.server.schema.sep2.event import EventStatus

from cactus_client.schema.validator import to_hex_binary


@pytest.mark.parametrize(
    "stored_values,check_params,should_match",
    [
        # No criteria
        ({}, {}, True),
        ({}, {"minimum_count": 1, "maximum_count": 1}, True),
        ({}, {"minimum_count": 2, "maximum_count": 3}, False),
        ({}, {"minimum_count": 0, "maximum_count": 0}, False),
        ({}, {"minimum_count": 0, "maximum_count": 99}, True),
        # Single criteria
        ({"opModExpLimW": 5000.0}, {"opModExpLimW": 5000.0, "minimum_count": 1, "maximum_count": 1}, True),
        ({"opModExpLimW": 5000.0}, {"opModExpLimW": 3000.0, "minimum_count": 1, "maximum_count": 1}, False),
        ({"opModLoadLimW": 3000.0}, {"opModLoadLimW": 3000.0, "minimum_count": 1, "maximum_count": 1}, True),
        ({"opModLoadLimW": 3000.0}, {"opModLoadLimW": 9999.0, "minimum_count": 1, "maximum_count": 1}, False),
        ({"opModGenLimW": 4000.0}, {"opModGenLimW": 4000.0, "minimum_count": 1, "maximum_count": 1}, True),
        ({"opModGenLimW": 4000.0}, {"opModGenLimW": 2000.0, "minimum_count": 1, "maximum_count": 1}, False),
        ({"setGradW": 100}, {"setGradW": 100, "minimum_count": 1, "maximum_count": 1}, True),
        ({"setGradW": 100}, {"setGradW": 200, "minimum_count": 1, "maximum_count": 1}, False),
        # All criteria - all match
        (
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 4000.0, "setGradW": 100},
            {
                "opModExpLimW": 5000.0,
                "opModLoadLimW": 3000.0,
                "opModGenLimW": 4000.0,
                "setGradW": 100,
                "minimum_count": 1,
                "maximum_count": 1,
            },
            True,
        ),
        # All criteria - one mismatch
        (
            {"opModExpLimW": 5000.0, "opModLoadLimW": 3000.0, "opModGenLimW": 4000.0, "setGradW": 100},
            {
                "opModExpLimW": 5000.0,
                "opModLoadLimW": 3000.0,
                "opModGenLimW": 9999.0,
                "setGradW": 100,
                "minimum_count": 1,
                "maximum_count": 1,
            },
            False,
        ),
        # With multipliers
        ({"opModExpLimW": (50, 2)}, {"opModExpLimW": 5000.0, "minimum_count": 1, "maximum_count": 1}, True),
        ({"opModExpLimW": (50, 2)}, {"opModExpLimW": 500.0, "minimum_count": 1, "maximum_count": 1}, False),
        (
            {"opModExpLimW": (50, 2), "opModGenLimW": (40, 2)},
            {"opModExpLimW": 5000.0, "opModGenLimW": 4000.0, "minimum_count": 1, "maximum_count": 1},
            True,
        ),
    ],
)
def test_check_default_der_control_combinations(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    stored_values,
    check_params,
    should_match,
):
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
    assert_check_result(result, should_match)


def test_check_default_der_control_no_controls_in_store(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Test check_default_der_control when no controls exist"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resolved_params = {"opModExpLimW": 5000.0}

    # Act
    result = check_default_der_control(resolved_params, step, context)

    # Assert
    assert isinstance(result, CheckResult)
    assert result.passed is False


def test_check_default_der_control_multiple_controls(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):

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


def test_check_default_der_control_sub_id(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that sub_id filtering works"""

    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Setup store an initial sub tags
    dderc1 = resource_store.upsert_resource(
        CSIPAusResource.DefaultDERControl, None, generate_class_instance(DefaultDERControl, seed=1)
    )
    dderc2 = resource_store.upsert_resource(
        CSIPAusResource.DefaultDERControl, None, generate_class_instance(DefaultDERControl, seed=2)
    )
    resource_store.upsert_resource(
        CSIPAusResource.DefaultDERControl, None, generate_class_instance(DefaultDERControl, seed=3)
    )
    dderc4 = resource_store.upsert_resource(
        CSIPAusResource.DefaultDERControl, None, generate_class_instance(DefaultDERControl, seed=4)
    )

    context.resource_annotations(step, dderc1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")
    context.resource_annotations(step, dderc1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub2")

    context.resource_annotations(step, dderc2.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    context.resource_annotations(step, dderc4.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    # Perform queries
    assert_check_result(
        check_default_der_control({"minimum_count": 3, "maximum_count": 3, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_default_der_control({"minimum_count": 0, "maximum_count": 5, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_default_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub1"}, step, context), False
    )
    assert_check_result(
        check_default_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub2"}, step, context), True
    )
    assert_check_result(
        check_default_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub3"}, step, context), False
    )
    assert_check_result(
        check_default_der_control({"minimum_count": 0, "maximum_count": 0, "sub_id": "sub3"}, step, context), True
    )


def test_check_der_control_sub_id(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that sub_id filtering works"""

    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Setup store an initial sub tags
    dderc1 = resource_store.upsert_resource(
        CSIPAusResource.DERControl, None, generate_class_instance(DERControlResponse, seed=1)
    )
    dderc2 = resource_store.upsert_resource(
        CSIPAusResource.DERControl, None, generate_class_instance(DERControlResponse, seed=2)
    )
    resource_store.upsert_resource(
        CSIPAusResource.DERControl, None, generate_class_instance(DERControlResponse, seed=3)
    )
    dderc4 = resource_store.upsert_resource(
        CSIPAusResource.DERControl, None, generate_class_instance(DERControlResponse, seed=4)
    )

    context.resource_annotations(step, dderc1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")
    context.resource_annotations(step, dderc1.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub2")

    context.resource_annotations(step, dderc2.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    context.resource_annotations(step, dderc4.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, "sub1")

    # Perform queries
    assert_check_result(
        check_der_control({"minimum_count": 3, "maximum_count": 3, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_der_control({"minimum_count": 0, "maximum_count": 5, "sub_id": "sub1"}, step, context), True
    )
    assert_check_result(
        check_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub1"}, step, context), False
    )
    assert_check_result(
        check_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub2"}, step, context), True
    )
    assert_check_result(
        check_der_control({"minimum_count": 1, "maximum_count": 1, "sub_id": "sub3"}, step, context), False
    )
    assert_check_result(
        check_der_control({"minimum_count": 0, "maximum_count": 0, "sub_id": "sub3"}, step, context), True
    )


def test_check_default_der_control_with_derp_primacy(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
):
    """Test that derp_primacy filtering works"""
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create DERProgram with primacy
    derp1 = generate_class_instance(DERProgramResponse, primacy=1, seed=1)
    derp1_sr = resource_store.upsert_resource(CSIPAusResource.DERProgram, None, derp1)

    derp2 = generate_class_instance(DERProgramResponse, primacy=2, seed=2)
    derp2_sr = resource_store.upsert_resource(CSIPAusResource.DERProgram, None, derp2)

    # Create DefaultDERControls under each program
    dderc1 = generate_class_instance(DefaultDERControl, seed=1)
    resource_store.upsert_resource(CSIPAusResource.DefaultDERControl, derp1_sr.id, dderc1)

    dderc2 = generate_class_instance(DefaultDERControl, seed=2)
    resource_store.upsert_resource(CSIPAusResource.DefaultDERControl, derp2_sr.id, dderc2)

    # Test filtering by primacy
    assert_check_result(
        check_default_der_control({"derp_primacy": 1, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_default_der_control({"derp_primacy": 2, "minimum_count": 1, "maximum_count": 1}, step, context), True
    )
    assert_check_result(
        check_default_der_control({"derp_primacy": 3, "minimum_count": 0, "maximum_count": 0}, step, context), True
    )


@pytest.mark.parametrize(
    "stored_values,check_params,should_match",
    [
        # Happy case - all parameters match
        (
            {
                "opModImpLimW": 6000.0,
                "opModExpLimW": 5000.0,
                "opModLoadLimW": 3000.0,
                "opModGenLimW": 4000.0,
                "opModEnergize": True,
                "opModConnect": False,
                "opModFixedW": 2000.0,
                "rampTms": 300,
                "randomizeStart": 60,
                "event_status": 1,
                "response_required": 1,
                "derp_primacy": 10,
                "duration": 3600,
            },
            {
                "opModImpLimW": 6000.0,
                "opModExpLimW": 5000.0,
                "opModLoadLimW": 3000.0,
                "opModGenLimW": 4000.0,
                "opModEnergize": True,
                "opModConnect": False,
                "opModFixedW": 2000.0,
                "rampTms": 300,
                "randomizeStart": 60,
                "event_status": 1,
                "response_required": 1,
                "derp_primacy": 10,
                "duration": 3600,
                "minimum_count": 1,
                "maximum_count": 1,
            },
            True,
        ),
    ],
)
def test_check_der_control_all_parameters(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
    assert_check_result: Callable[[CheckResult, bool], None],
    stored_values,
    check_params,
    should_match,
):
    """Test check_der_control with all parameters (except sub_id which has dedicated tests)"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create DERProgram for derp_primacy testing
    derp = generate_class_instance(DERProgramResponse, primacy=stored_values["derp_primacy"])
    derp_sr = resource_store.upsert_resource(CSIPAusResource.DERProgram, None, derp)

    # Build DERControlBase
    control_base = generate_class_instance(
        DERControlBase,
        opModImpLimW=ActivePower(value=int(stored_values["opModImpLimW"]), multiplier=0),
        opModExpLimW=ActivePower(value=int(stored_values["opModExpLimW"]), multiplier=0),
        opModLoadLimW=ActivePower(value=int(stored_values["opModLoadLimW"]), multiplier=0),
        opModGenLimW=ActivePower(value=int(stored_values["opModGenLimW"]), multiplier=0),
        opModEnergize=stored_values["opModEnergize"],
        opModConnect=stored_values["opModConnect"],
        opModFixedW=stored_values["opModFixedW"],
        rampTms=stored_values["rampTms"],
    )

    # Build other components
    interval = generate_class_instance(DateTimeIntervalType, duration=stored_values["duration"])
    event_status = generate_class_instance(EventStatus, currentStatus=stored_values["event_status"])

    # Build DERControl
    derc = generate_class_instance(
        DERControlResponse,
        DERControlBase_=control_base,
        interval=interval,
        EventStatus_=event_status,
        randomizeStart=stored_values["randomizeStart"],
        responseRequired=to_hex_binary(stored_values["response_required"]),
        href="/derc/1",
    )

    resource_store.upsert_resource(CSIPAusResource.DERControl, derp_sr.id, derc)

    # Act
    result = check_der_control(check_params, step, context)

    # Assert
    assert_check_result(result, should_match)
