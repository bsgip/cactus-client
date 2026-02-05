from datetime import datetime, timedelta
from pathlib import Path

import pytest
from assertical.fake.generator import generate_class_instance, generate_value
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
from cactus_client.results.common import ResultsEvaluation


def generate_server_response(seed: int, xsd_errors: list[str] | None) -> ServerResponse:
    return ServerResponse(
        url=generate_value(str, seed + 1),
        body=generate_value(str, seed + 2),
        content_type=generate_value(str, seed + 3),
        location=None,
        method=generate_value(str, seed + 4),
        request=ServerRequest(
            generate_value(str, seed + 5), generate_value(str, seed + 6), generate_value(str, seed + 7), {}
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
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
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
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
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
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
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
async def test_ResultsEvaluation_failing_xsd_errors_notifications(assertical_extensions):
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
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
async def test_ResultsEvaluation_failing_warning(assertical_extensions):
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
    )
    await context.progress.add_step_execution_completion(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.add_step_execution_completion(step_execution_2, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_1, ActionResult.done(), CheckResult(True, None))
    await context.progress.set_step_result(step_execution_2, ActionResult.done(), CheckResult(True, None))

    context.responses.responses.append(generate_server_response(1, xsd_errors=None))
    context.responses.responses.append(generate_server_response(2, xsd_errors=[]))

    context.warnings.log_step_warning(step_execution_2, "Added a warning")

    actual = ResultsEvaluation(context, ExecutionResult(True))
    assert not actual.has_passed()
    assert not actual.no_warnings
    assert actual.total_steps == 2
    assert actual.total_steps_passed == 2
    assert actual.total_warnings == 1
    assert actual.total_xsd_errors == 0


@pytest.mark.asyncio
async def test_ResultsEvaluation_failing_failing_step(assertical_extensions):
    step_1 = generate_class_instance(Step, seed=1, generate_relationships=True)
    step_2 = generate_class_instance(Step, seed=2, generate_relationships=True)

    context = generate_empty_context([step_1, step_2])

    step_execution_1 = generate_class_instance(StepExecution, seed=101, source=step_1)
    step_execution_2 = generate_class_instance(StepExecution, seed=202, source=step_2)

    await context.progress.add_step_execution_completion(
        step_execution_1, ActionResult(completed=True, repeat=True, not_before=None), CheckResult(True, None)
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
