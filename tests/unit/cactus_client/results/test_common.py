from datetime import datetime, timedelta
from pathlib import Path

import pytest
from assertical.fake.generator import generate_class_instance, generate_value
from cactus_test_definitions.server.actions import Action
from cactus_test_definitions.server.test_procedures import (
    Step,
    TestProcedure,
    TestProcedureId,
)
from multidict import CIMultiDict

from cactus_client.model.config import ServerConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import (
    ActionResult,
    CheckResult,
    ExecutionResult,
    StepExecution,
    StepExecutionList,
)
from cactus_client.model.http import (
    NotificationEndpoint,
    NotificationRequest,
    ServerRequest,
    ServerResponse,
)
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import RESOURCE_SEP2_TYPES, CSIPAusResourceTree
from cactus_client.results.common import ResultsEvaluation, skipped_steps


def generate_step(seed: int) -> Step:
    """Generate a Step with an explicit Action to avoid assertical trying to generate dict[str, Any]."""
    return generate_class_instance(Step, seed=seed, action=Action(type="dummy"))


def generate_server_response(seed: int, xsd_errors: list[str] | None) -> ServerResponse:
    return ServerResponse(
        url=generate_value(str, seed + 1),
        body=generate_value(str, seed + 2),
        content_type=generate_value(str, seed + 3),
        location=None,
        method=generate_value(str, seed + 4),
        request=ServerRequest(
            generate_value(str, seed + 5),
            generate_value(str, seed + 6),
            generate_value(str, seed + 7),
            {},
        ),
        headers=CIMultiDict(),
        status=200,
        xsd_errors=xsd_errors,
    )


def generate_notification(seed: int, xsd_errors: list[str] | None) -> NotificationRequest:
    return NotificationRequest(
        method=generate_value(str, seed + 1),
        body=generate_value(str, seed + 2),
        content_type=generate_value(str, seed + 3),
        headers=CIMultiDict(),
        received_at=generate_value(datetime, seed + 4),
        remote=generate_value(str, seed + 5),
        sub_id=generate_value(str, seed + 6),
        xsd_errors=xsd_errors,
        source=generate_class_instance(NotificationEndpoint, seed + 7),
    )


def generate_empty_context(steps: list[Step]) -> ExecutionContext:
    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure, steps=steps),
        test_procedures_version="vtest",
        output_directory=Path("."),
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={},
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=StepExecutionList(),
    )

    return context


@pytest.mark.asyncio
async def test_ResultsEvaluation_passed(assertical_extensions):
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))
    context.responses.responses.append(generate_notification(3, xsd_errors=None))
    context.responses.responses.append(generate_notification(4, xsd_errors=[]))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert actual.has_passed()
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 2
    assert actual.total_warnings == 0
    assert actual.total_xsd_errors == 0


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_missing_result(assertical_extensions):
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))
    context.responses.responses.append(generate_notification(3, xsd_errors=None))
    context.responses.responses.append(generate_notification(4, xsd_errors=[]))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert not actual.all_steps_evaluated
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 1
    assert actual.total_warnings == 0
    assert actual.total_xsd_errors == 0


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_xsd_errors(assertical_extensions):
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=["has error"]))
    context.responses.responses.append(generate_notification(3, xsd_errors=None))
    context.responses.responses.append(generate_notification(4, xsd_errors=[]))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert not actual.no_xsd_errors
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 2
    assert actual.total_warnings == 0
    assert actual.total_xsd_errors == 1


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_xsd_errors_notifications(
    assertical_extensions,
):
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))
    context.responses.responses.append(generate_notification(3, xsd_errors=None))
    context.responses.responses.append(generate_notification(4, xsd_errors=["has error"]))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert not actual.no_xsd_errors
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 2
    assert actual.total_warnings == 0
    assert actual.total_xsd_errors == 1


@pytest.mark.asyncio
async def test_ResultsEvaluation_passed_with_warnings(assertical_extensions):
    """Warnings are reported but do not cause the test to fail."""
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))

    context.warnings.log_step_warning(step_execution_2, "Added a warning")

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert actual.has_passed()
    assert not actual.no_warnings
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 2
    assert actual.total_warnings == 1
    assert actual.total_xsd_errors == 0


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_failing_step(assertical_extensions):
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1,
        ActionResult(completed=True, repeat=True, not_before=None),
        CheckResult(True, None),
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(False, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert actual.all_steps_evaluated
    assert not actual.all_steps_passed
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 1
    assert actual.total_warnings == 0
    assert actual.total_xsd_errors == 0


def test_RESOURCE_SEP2_TYPES_typos():
    """Just check we havent made a typo in the resource defs"""
    for resource_enum, response_type in RESOURCE_SEP2_TYPES.items():
        expected = response_type.__name__.replace("Response", "")
        assert resource_enum.value == expected


@pytest.mark.asyncio
async def test_ResultsEvaluation_passed_with_skips(assertical_extensions):
    """A skipped step doesn't count as passed but doesn't block the run from passing"""
    step_1 = generate_step(1)
    step_2 = generate_step(2)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(
        step_execution_2,
        ActionResult.done(),
        CheckResult(False, None),
        skip_reason="cant set that up here",
    )

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert actual.has_passed()
    assert actual.all_steps_evaluated
    assert actual.all_steps_passed
    assert actual.skips_applied
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 1
    assert actual.total_steps_skipped == 1

    assert [(sr.step.id, sr.skip_reason) for sr in skipped_steps(context)] == [(step_2.id, "cant set that up here")]


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_step_with_skips(assertical_extensions):
    """A skip doesn't rescue a genuinely failing step"""
    step_1 = generate_step(1)
    step_2 = generate_step(2)
    step_3 = generate_step(3)

    context = generate_empty_context([step_1, step_2, step_3])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)
    step_execution_3 = generate_class_instance(StepExecution, seed=303, source=step_3)

    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(
        step_execution_2, ActionResult.done(), CheckResult(True, None), skip_reason="waived"
    )
    await context.progress.set_step_result(step_execution_3, ActionResult.done(), CheckResult(False, None))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert not actual.all_steps_passed
    assert actual.skips_applied
    assert actual.total_steps_passed == 1
    assert actual.total_steps_skipped == 1
    assert actual.total_steps == 3


@pytest.mark.asyncio
async def test_ResultsEvaluation_no_skips(assertical_extensions):
    """A clean run reports no skips"""
    step_1 = generate_step(1)
    context = generate_empty_context([step_1])
    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)

    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert actual.has_passed()
    assert not actual.skips_applied
    assert actual.total_steps_skipped == 0
    assert skipped_steps(context) == []


@pytest.mark.asyncio
async def test_ResultsEvaluation_strict_warnings_fail(assertical_extensions):
    """A strict run treats warnings as failures"""
    step_1 = generate_step(1)
    context = generate_empty_context([step_1])
    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)

    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    context.warnings.log_step_warning(step_execution_1, "Added a warning")

    actual = ResultsEvaluation(context, ExecutionResult(True))

    assert actual.has_passed()
    assert not actual.has_passed(strict=True)
