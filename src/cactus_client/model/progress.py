import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from cactus_test_definitions.server.test_procedures import Step

from cactus_client.model.execution import ActionResult, CheckResult, StepExecution
from cactus_client.model.http import ServerRequest, ServerResponse
from cactus_client.time import relative_time, utc_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LogEntry:
    message: str  # The log entry
    step_execution: StepExecution  # The step execution that generated this log entry
    created_at: datetime = field(default_factory=utc_now, init=False)


class WarningTracker:
    """A warning represents some form of (minor) failure of a test that doesn't block the execution but should be
    reported at the end. Example warnings could include a non critical XSD error."""

    warnings: list[LogEntry]

    def __init__(self) -> None:
        self.warnings = []

    # def log_resource_warning(self, type: CSIPAusResource, message: str) -> None:
    #     """Log an warning about a specific type of CSIPAusResource"""
    #     warning = f"Resource {type}: {message}"
    #     self.warnings.append(warning)
    #     logger.warning(warning)

    def log_step_warning(self, step_execution: StepExecution, message: str) -> None:
        """Log a warning about a specific execution step"""
        self.warnings.append(LogEntry(message, step_execution))

        logger.warning(
            f"{step_execution.source.id}[{step_execution.repeat_number}] Attempt {step_execution.attempts}: {message}"
        )


@dataclass(frozen=True)
class StepExecutionCompletion:
    """Represents the completion of a StepExecution (successful or otherwise)"""

    step_execution: StepExecution
    action_result: ActionResult | None  # None if aborted due to exception
    check_result: CheckResult | None  # None if aborted due to exception

    exc: Exception | None  # Set to the exception that was raised during action/check calculation

    created_at: datetime = field(default_factory=utc_now, init=False)

    def is_success(self) -> bool:
        """True if this execution represents a successful result (no exceptions and a passing check result)"""
        return (
            self.exc is None
            and self.action_result is not None
            and self.check_result is not None
            and self.check_result.passed
        )


@dataclass(frozen=True)
class StepResult:
    """Represents the pass/failure result of an entire step"""

    step: Step
    failure_result: CheckResult | None
    exc: Exception | None

    created_at: datetime = field(default_factory=utc_now, init=False)

    def is_passed(self) -> bool:
        return self.failure_result is None and self.exc is None


@dataclass
class StepProgress:
    """Captures the progress of a top level Step as it undergoes execution. Will be created at first progress"""

    step: Step  # The step definition
    result: StepResult | None  # The result of this step (if fully completed)
    step_execution_completions: list[StepExecutionCompletion]  # Populated when StepExecutions complete (pass or fail)
    log_entries: list[LogEntry]  # general log entries associated with StepExecutions occurring under this step

    created_at: datetime = field(default_factory=utc_now, init=False)

    @staticmethod
    def empty(step: Step) -> "StepProgress":
        return StepProgress(step, None, [], [])


class ProgressTracker:
    """A utility for allowing step execution operations to update the user facing progress of those operations"""

    current_step_execution: StepExecution | None  # What step is currently undergoing execution / waiting
    progress_by_step_id: dict[str, StepProgress]

    all_completions: list[StepExecutionCompletion]  # Every completion (across steps) sorted by their insertion time
    all_results: list[StepResult]  # Every result logged, sorted by their insertion time

    def __init__(self) -> None:
        self.current_step_execution = None
        self.progress_by_step_id = {}
        self.all_completions = []
        self.all_results = []

    def _update_progress(self, step_execution: StepExecution, update: Callable[[StepProgress], Any]) -> None:
        step_id = step_execution.source.id
        progress = self.progress_by_step_id.get(step_id, None)
        if progress is None:
            self.progress_by_step_id[step_id] = progress = StepProgress.empty(step_execution.source)

        update(progress)

    async def add_log(self, step_execution: StepExecution, message: str) -> None:
        """Adds a log entry for a specific StepExecution"""
        log = LogEntry(message, step_execution)
        step_id = step_execution.source.id
        logger.info(f"{step_id}[{step_execution.repeat_number}] Attempt {step_execution.attempts}: {message}")

        self._update_progress(step_execution, lambda p: p.log_entries.append(log))

    async def update_current_step(self, step_execution: StepExecution, delay: timedelta | None) -> None:
        """Updates the tracker with what the currently executing StepExecution is"""
        self.current_step_execution = step_execution
        if delay:
            await self.add_log(step_execution, f"Waiting {relative_time(delay)} for start.")
        else:
            await self.add_log(step_execution, "Beginning execution.")

    async def add_step_execution_exception(self, step_execution: StepExecution, exc: Exception) -> None:
        """Logs that a step action/check raised an unhandled exception - this will also mark the step as failed"""

        completion = StepExecutionCompletion(
            step_execution=step_execution, action_result=None, check_result=None, exc=exc
        )
        result = StepResult(step=step_execution.source, failure_result=None, exc=exc)

        await self.add_log(step_execution, f"Exception raised during action/check: {exc}")
        self.all_completions.append(completion)
        self.all_results.append(result)

        def do_update(progress: StepProgress) -> None:
            progress.step_execution_completions.append(completion)
            progress.result = result

        self._update_progress(step_execution, do_update)

    async def add_step_execution_completion(
        self, step_execution: StepExecution, action_result: ActionResult, check_result: CheckResult
    ) -> None:
        """Logs that a step and its checks have completed without an exception (either pass or fail)"""

        completion = StepExecutionCompletion(
            step_execution=step_execution, action_result=action_result, check_result=check_result, exc=None
        )
        self.all_completions.append(completion)
        self._update_progress(step_execution, lambda p: p.step_execution_completions.append(completion))
        if check_result.passed:
            await self.add_log(step_execution, "Success with all checks passing.")
        else:
            await self.add_log(step_execution, f"Check Failure: {check_result.description}")

    async def set_step_result(self, step_execution: StepExecution, check_result: CheckResult) -> None:
        """Logs that a step execution is that LAST time the underlying step will run."""

        result = StepResult(
            step=step_execution.source, failure_result=None if check_result.passed else check_result, exc=None
        )
        self.all_results.append(result)

        def do_update(progress: StepProgress) -> None:
            progress.result = result

        self._update_progress(step_execution, do_update)

        if check_result.passed:
            await self.add_log(step_execution, f"{step_execution.source.id} has been marked as successful")
        else:
            await self.add_log(
                step_execution, f"{step_execution.source.id} has been marked as failed: {check_result.description}"
            )


class ResponseTracker:
    """A utility for tracking raw responses received from the utility server and their validity"""

    responses: list[ServerResponse]
    active_request: ServerRequest | None

    def __init__(self) -> None:
        self.responses = []
        self.active_request = None

    async def set_active_request(
        self, method: str, url: str, body: str | None, headers: dict[str, str]
    ) -> ServerRequest:
        self.active_request = ServerRequest(url=url, method=method, body=body, headers=headers)
        return self.active_request

    async def clear_active_request(self) -> None:
        self.active_request = None

    async def log_response_body(self, r: ServerResponse) -> None:
        self.responses.append(r)
        logger.info(f"{r.method} {r.url} Yielded {r.status}: Received body of length {len(r.body)}.")
