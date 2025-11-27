import unittest.mock as mock
from datetime import timedelta
from pathlib import Path

import pytest
from assertical.asserts.time import assert_nowish
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.server.test_procedures import (
    Action,
    Check,
    Step,
    TestProcedure,
    TestProcedureId,
)

from cactus_client.error import CactusClientException
from cactus_client.execution.execute import execute_for_context
from cactus_client.model.config import ClientConfig, ServerConfig
from cactus_client.model.context import ClientContext, ExecutionContext
from cactus_client.model.execution import (
    ActionResult,
    CheckResult,
    ExecutionResult,
    StepExecution,
    StepExecutionList,
)
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import (
    CSIPAusResourceTree,
    ResourceStore,
)
from cactus_client.time import utc_now

ACTION_DONE = "action-done"
ACTION_REPEAT_ONCE = "action-repeat-once"
ACTION_REPEAT_ONCE_DELAY = "action-repeat-once-delay"
ACTION_EXCEPTION = "action-exception"

CHECK_FAIL_ONCE = "check-fail-once"
CHECK_PASS = "check-pass"
CHECK_FAIL = "check-fail"
CHECK_EXCEPTION = "check-exception"


DELAY_TIME = timedelta(seconds=2)


def handle_mock_execute_action(current_step: StepExecution, context: ExecutionContext) -> ActionResult:
    assert isinstance(current_step, StepExecution)
    assert isinstance(context, ExecutionContext)

    action_type = current_step.source.action.type
    if action_type == ACTION_DONE:
        return ActionResult.done()
    elif action_type == ACTION_REPEAT_ONCE:
        if current_step.repeat_number == 0:
            return ActionResult(True, None)
        else:
            return ActionResult.done()
    elif action_type == ACTION_REPEAT_ONCE_DELAY:
        if current_step.repeat_number == 0:
            return ActionResult(True, utc_now() + DELAY_TIME)
        else:
            return ActionResult.done()
    elif action_type == ACTION_EXCEPTION:
        raise CactusClientException("mocked exception")
    else:
        raise NotImplementedError(f"Unsupported action type {action_type}")


def handle_mock_execute_checks(current_step: StepExecution, context: ExecutionContext) -> CheckResult:
    assert isinstance(current_step, StepExecution)
    assert isinstance(context, ExecutionContext)

    check_type = current_step.source.checks[0].type if current_step.source.checks else "DNE"
    if check_type == CHECK_PASS:
        return CheckResult(True, None)
    elif check_type == CHECK_FAIL:
        return CheckResult(False, None)
    elif check_type == CHECK_FAIL_ONCE:
        if current_step.attempts == 0:
            return CheckResult(False, f"Failure #{current_step.attempts}")
        else:
            return CheckResult(True, None)
    elif check_type == CHECK_EXCEPTION:
        raise CactusClientException("mocked exception")
    else:
        raise NotImplementedError(f"Unsupported check type {check_type}")


def assert_step_result(tracker: ProgressTracker, step_id: str, passed: bool | None) -> None:
    """Asserts the step result - if None, assert that no result is logged"""
    if passed is None:
        progress = tracker.progress_by_step_id.get(step_id, None)
        if progress is not None:
            assert progress.result is None
    else:
        assert step_id in tracker.progress_by_step_id
        progress = tracker.progress_by_step_id[step_id]
        assert progress.result is not None
        assert progress.result.is_passed() is passed


@mock.patch("cactus_client.execution.execute.execute_action")
@mock.patch("cactus_client.execution.execute.execute_checks")
@pytest.mark.asyncio
async def test_execute_for_context_success_cases_with_repeats(
    mock_execute_checks: mock.MagicMock,
    mock_execute_action: mock.MagicMock,
):
    """Can execute handle cases where some steps repeat due to failure or by request"""
    # Arrange
    step_list = StepExecutionList()
    step_list.add(
        StepExecution(
            Step(id="1", action=Action(ACTION_DONE), checks=[Check(CHECK_FAIL_ONCE)], repeat_until_pass=True),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=0,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="2", action=Action(ACTION_REPEAT_ONCE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=1,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="3", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=2,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )

    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure),
        test_procedures_version="vtest",
        output_directory=Path("."),  # Shouldn't be used to do anything in this test
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={
            "client-test": ClientContext(
                "client-test", generate_class_instance(ClientConfig), ResourceStore(tree), {}, mock.Mock(), None
            )
        },
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=step_list,
    )

    mock_execute_checks.side_effect = handle_mock_execute_checks
    mock_execute_action.side_effect = handle_mock_execute_action

    # Act
    start_time = utc_now()
    result = await execute_for_context(context)
    duration = utc_now() - start_time

    # Assert
    assert isinstance(result, ExecutionResult)
    assert result.completed
    assert duration <= DELAY_TIME, "We expect the test to operate with no delays"

    assert mock_execute_checks.call_count == 5
    assert mock_execute_action.call_count == 5

    assert len(context.warnings.warnings) == 0

    assert [se.step_execution.source.id for se in context.progress.all_completions] == ["1", "1", "2", "2", "3"]
    assert [p.is_success() for p in context.progress.all_completions] == [False, True, True, True, True]
    assert_step_result(context.progress, "1", True)
    assert_step_result(context.progress, "2", True)
    assert_step_result(context.progress, "3", True)

    for p in context.progress.all_completions:
        assert_nowish(p.created_at)
        assert p.exc is None


@mock.patch("cactus_client.execution.execute.execute_action")
@mock.patch("cactus_client.execution.execute.execute_checks")
@pytest.mark.asyncio
async def test_execute_for_context_failure_stops_early(
    mock_execute_checks: mock.MagicMock,
    mock_execute_action: mock.MagicMock,
):
    """Can execute handle check results that report failure and then stop future steps from running"""
    # Arrange
    step_list = StepExecutionList()
    step_list.add(
        StepExecution(
            Step(id="1", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=0,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="2", action=Action(ACTION_DONE), checks=[Check(CHECK_FAIL_ONCE)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=1,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="3", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=2,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )

    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure),
        test_procedures_version="vtest",
        output_directory=Path("."),  # Shouldn't be used to do anything in this test
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={
            "client-test": ClientContext(
                "client-test", generate_class_instance(ClientConfig), ResourceStore(tree), {}, mock.Mock(), None
            )
        },
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=step_list,
    )

    mock_execute_checks.side_effect = handle_mock_execute_checks
    mock_execute_action.side_effect = handle_mock_execute_action

    # Act
    result = await execute_for_context(context)

    # Assert
    assert isinstance(result, ExecutionResult)
    assert result.completed

    assert mock_execute_checks.call_count == 2, "Only the first two steps should execute due to step 2 failing"
    assert mock_execute_action.call_count == 2, "Only the first two steps should execute due to step 2 failing"

    assert [se.step_execution.source.id for se in context.progress.all_completions] == ["1", "2"]
    assert [p.is_success() for p in context.progress.all_completions] == [True, False]
    assert_step_result(context.progress, "1", True)
    assert_step_result(context.progress, "2", False)
    assert_step_result(context.progress, "3", None)

    assert len(context.warnings.warnings) == 0


@mock.patch("cactus_client.execution.execute.execute_action")
@mock.patch("cactus_client.execution.execute.execute_checks")
@pytest.mark.asyncio
async def test_execute_for_context_action_exception(
    mock_execute_checks: mock.MagicMock,
    mock_execute_action: mock.MagicMock,
):
    """Can execute handle exceptions during action resolution"""
    # Arrange
    step_list = StepExecutionList()
    step_list.add(
        StepExecution(
            Step(id="1", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=0,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="2", action=Action(ACTION_EXCEPTION), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=1,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="3", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=2,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )

    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure),
        test_procedures_version="vtest",
        output_directory=Path("."),  # Shouldn't be used to do anything in this test
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={
            "client-test": ClientContext(
                "client-test", generate_class_instance(ClientConfig), ResourceStore(tree), {}, mock.Mock(), None
            )
        },
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=step_list,
    )

    mock_execute_checks.side_effect = handle_mock_execute_checks
    mock_execute_action.side_effect = handle_mock_execute_action

    # Act
    result = await execute_for_context(context)

    # Assert
    assert isinstance(result, ExecutionResult)
    assert not result.completed

    assert mock_execute_checks.call_count == 1, "Test is aborted at step 2 (during action execution)"
    assert mock_execute_action.call_count == 2, "Test is aborted at step 2"

    assert len(context.warnings.warnings) == 0
    assert [se.step_execution.source.id for se in context.progress.all_completions] == ["1", "2"]
    assert [p.is_success() for p in context.progress.all_completions] == [True, False]
    assert [p.exc is None for p in context.progress.all_completions] == [True, False]
    assert_step_result(context.progress, "1", True)
    assert_step_result(context.progress, "2", False)
    assert_step_result(context.progress, "3", None)

    assert context.progress.progress_by_step_id["1"].result.exc is None
    assert context.progress.progress_by_step_id["2"].result.exc is not None


@mock.patch("cactus_client.execution.execute.execute_action")
@mock.patch("cactus_client.execution.execute.execute_checks")
@pytest.mark.asyncio
async def test_execute_for_context_check_exception(
    mock_execute_checks: mock.MagicMock,
    mock_execute_action: mock.MagicMock,
):
    """Can execute handle exceptions during check resolution"""
    # Arrange
    step_list = StepExecutionList()
    step_list.add(
        StepExecution(
            Step(id="1", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=0,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="2", action=Action(ACTION_DONE), checks=[Check(CHECK_EXCEPTION)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=1,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="3", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=2,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )

    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure),
        test_procedures_version="vtest",
        output_directory=Path("."),  # Shouldn't be used to do anything in this test
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={
            "client-test": ClientContext(
                "client-test", generate_class_instance(ClientConfig), ResourceStore(tree), {}, mock.Mock(), None
            )
        },
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=step_list,
    )

    mock_execute_checks.side_effect = handle_mock_execute_checks
    mock_execute_action.side_effect = handle_mock_execute_action

    # Act
    result = await execute_for_context(context)

    # Assert
    assert isinstance(result, ExecutionResult)
    assert not result.completed

    assert [se.step_execution.source.id for se in context.progress.all_completions] == ["1", "2"]
    assert [p.is_success() for p in context.progress.all_completions] == [True, False]
    assert_step_result(context.progress, "1", True)
    assert_step_result(context.progress, "2", False)
    assert_step_result(context.progress, "3", None)
    assert len(context.warnings.warnings) == 0


@mock.patch("cactus_client.execution.execute.execute_action")
@mock.patch("cactus_client.execution.execute.execute_checks")
@pytest.mark.asyncio
async def test_execute_for_context_success_cases_with_delays(
    mock_execute_checks: mock.MagicMock,
    mock_execute_action: mock.MagicMock,
):
    """Can execute handle cases where some steps repeat due to failure or by request"""
    # Arrange
    step_list = StepExecutionList()
    step_list.add(
        StepExecution(
            Step(id="1", action=Action(ACTION_REPEAT_ONCE_DELAY), checks=[Check(CHECK_PASS)], repeat_until_pass=True),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=0,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )
    step_list.add(
        StepExecution(
            Step(id="2", action=Action(ACTION_DONE), checks=[Check(CHECK_PASS)]),
            client_alias="client-test",
            client_resources_alias="client-test",
            primacy=1,
            repeat_number=0,
            not_before=None,
            attempts=0,
        )
    )

    tree = CSIPAusResourceTree()
    context = ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure),
        test_procedures_version="vtest",
        output_directory=Path("."),  # Shouldn't be used to do anything in this test
        dcap_path="/dcap/path",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias={
            "client-test": ClientContext(
                "client-test", generate_class_instance(ClientConfig), ResourceStore(tree), {}, mock.Mock(), None
            )
        },
        resource_tree=tree,
        repeat_delay=timedelta(0),
        responses=ResponseTracker(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        steps=step_list,
    )

    mock_execute_checks.side_effect = handle_mock_execute_checks
    mock_execute_action.side_effect = handle_mock_execute_action

    # Act
    start_time = utc_now()
    result = await execute_for_context(context)
    duration = utc_now() - start_time

    # Assert
    assert isinstance(result, ExecutionResult)
    assert result.completed

    assert duration >= DELAY_TIME, "We expect the test to perform a single wait"

    # We are expecting the execution to look like this:
    #  Step 1 - Runs and then asks for a repeat in 2 seconds
    #  Step 2 - Runs (as it is the lowest primacy that isn't on a delay)
    #  Step 1 - Repeats later
    assert [se.step_execution.source.id for se in context.progress.all_completions] == ["1", "2", "1"]
    assert [p.is_success() for p in context.progress.all_completions] == [True, True, True]
    assert_step_result(context.progress, "1", True)
    assert_step_result(context.progress, "2", True)

    assert mock_execute_checks.call_count == 3
    assert mock_execute_action.call_count == 3
    assert len(context.warnings.warnings) == 0
