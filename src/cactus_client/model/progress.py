import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import Step

from cactus_client.model.execution import ActionResult, CheckResult, StepExecution
from cactus_client.model.http import ServerResponse
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


class WarningTracker:
    """A warning represents some form of (minor) failure of a test that doesn't block the execution but should be
    reported at the end. Example warnings could include a non critical XSD error."""

    warnings: list[str]

    def __init__(self) -> None:
        self.warnings = []

    def log_resource_warning(self, type: CSIPAusResource, message: str) -> None:
        """Log an warning about a specific type of CSIPAusResource"""
        warning = f"Resource {type}: {message}"
        self.warnings.append(warning)
        logger.warning(warning)

    def log_step_warning(self, step: StepExecution, message: str) -> None:
        """Log a warning about a specific execution step"""
        warning = f"Step {step.source.id}[{step.repeat_number}]: {message}"
        self.warnings.append(warning)
        logger.warning(warning)


@dataclass(frozen=True)
class StepExecutionProgress:
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
    step: Step
    failure_result: CheckResult | None
    exc: Exception | None

    created_at: datetime = field(default_factory=utc_now, init=False)

    def is_passed(self) -> bool:
        return self.failure_result is None and self.exc is None


class ProgressTracker:
    """A utility for allowing step execution operations to update the user facing progress of those operations"""

    current_step_execution: StepExecution | None  # What step is currently undergoing execution / waiting

    step_execution_progress: list[StepExecutionProgress]
    step_results: list[StepResult]

    def __init__(self) -> None:
        self.step_execution_progress = []
        self.step_results = []

    async def log_current_step_execution(self, step: StepExecution, delay: timedelta | None) -> None:
        self.current_step_execution = step
        if delay:
            logger.info(
                f"{step.source.id}[{step.repeat_number}] Attempt {step.attempts}: Waiting {delay.seconds}s for start."
            )
        else:
            logger.info(f"{step.source.id}[{step.repeat_number}] Attempt {step.attempts}: Beginning Execution.")

    async def log_step_execution_progress(self, step: StepExecution, message: str) -> None:
        """Updates the progress information for a specific step"""
        logger.info(f"{step.source.id}[{step.repeat_number}] Attempt {step.attempts}: {message}")

    async def log_step_execution_completed(
        self, s: StepExecution, action_result: ActionResult, check_result: CheckResult
    ) -> None:
        """Logs that a step and its checks have completed without an exception (either pass or fail)"""
        self.current_step_execution = None

        self.step_execution_progress.append(
            StepExecutionProgress(step_execution=s, action_result=action_result, check_result=check_result, exc=None)
        )
        if check_result.passed:
            logger.info(
                f"{s.source.id}[{s.repeat_number}] Attempt {s.attempts}: Success, repeat: {action_result.repeat}"
            )
        else:
            logger.info(f"{s.source.id}[{s.repeat_number}] Attempt {s.attempts}: Failed: {check_result.description}")

    async def log_step_execution_exception(self, step: StepExecution, exc: Exception) -> None:
        """Logs that a step action/check raised an unhandled exception - this will likely be the end of this test run"""
        self.step_execution_progress.append(
            StepExecutionProgress(step=step, action_result=None, check_result=None, exc=exc)
        )
        self.step_results.append(StepResult(step=step.source, failure_result=None, exc=exc))
        logger.info(f"{step.source.id}[{step.repeat_number}] Attempt {step.attempts}: Exception", exc_info=exc)

    async def log_final_step_execution(self, step: StepExecution, check_result: CheckResult) -> None:
        """Logs that a step execution is that LAST time the underlying step will run."""
        self.step_results.append(
            StepResult(step=step.source, failure_result=None if check_result.passed else check_result, exc=None)
        )
        if check_result.passed:
            logger.info(f"{step.source.id} has been marked as successful")
        else:
            logger.info(f"{step.source.id} has been marked as failed: {check_result.description}")


class ResponseTracker:
    """A utility for tracking raw responses received from the utility server and their validity"""

    responses: list[ServerResponse]

    def __init__(self) -> None:
        self.responses = []

    async def log_response_body(self, r: ServerResponse) -> None:
        self.responses.append(r)
        logger.info(f"{r.method} {r.url} Yielded {r.status}: Received body of length {len(r.body)}.")
