import logging
from datetime import timedelta

from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import Step

from cactus_client.model.execution import ActionResult, CheckResult, StepExecution
from cactus_client.model.http import ServerResponse

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


class ProgressTracker:
    """A utility for allowing step execution operations to update the user facing progress of those operations"""

    current_step_execution: StepExecution | None  # What step is currently undergoing execution / waiting

    success_step_executions: list[
        tuple[StepExecution, ActionResult]
    ]  # Any StepExecution that completed with all checks reporting OK
    failed_step_executions: list[
        tuple[StepExecution, ActionResult, CheckResult]
    ]  # Any StepExecution that completed but a check reported failure
    aborted_step_executions: list[tuple[StepExecution, Exception]]  # Any StepExecution that raised an exception

    passed_steps: list[Step]
    failed_steps: list[tuple[Step, CheckResult]]

    def __init__(self) -> None:
        self.success_step_executions = []
        self.failed_step_executions = []
        self.aborted_step_executions = []
        self.current_step_execution = None
        self.passed_steps = []
        self.failed_steps = []

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
        if check_result.passed:
            logger.info(
                f"{s.source.id}[{s.repeat_number}] Attempt {s.attempts}: Success, repeat: {action_result.repeat}"
            )
            self.success_step_executions.append((s, action_result))
        else:
            logger.info(f"{s.source.id}[{s.repeat_number}] Attempt {s.attempts}: Failed: {check_result.description}")
            self.failed_step_executions.append((s, action_result, check_result))

    async def log_step_execution_exception(self, step: StepExecution, exc: Exception) -> None:
        """Logs that a step action/check raised an unhandled exception - this will likely be the end of this test run"""
        logger.info(f"{step.source.id}[{step.repeat_number}] Attempt {step.attempts}: Exception", exc_info=exc)
        self.aborted_step_executions.append((step, exc))

    async def log_final_step_execution(self, step: StepExecution, check_result: CheckResult) -> None:
        """Logs that a step execution is that LAST time the underlying step will run."""
        if check_result.passed:
            logger.info(f"{step.source.id} has been marked as successful")
            self.passed_steps.append(step.source)
        else:
            logger.info(f"{step.source.id} has been marked as failed: {check_result.description}")
            self.failed_steps.append((step.source, check_result))


class ResponseTracker:
    """A utility for tracking raw responses received from the utility server and their validity"""

    responses: list[ServerResponse]

    def __init__(self) -> None:
        self.responses = []

    async def log_response_body(self, r: ServerResponse) -> None:
        self.responses.append(r)
        logger.info(f"{r.method} {r.url} Yielded {r.status}: Received body of length {len(r.body)}.")
