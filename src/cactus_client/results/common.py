from dataclasses import dataclass
from datetime import datetime

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult
from cactus_client.time import relative_time, utc_now


@dataclass
class ResultsEvaluation:
    """Captures a pretty granular breakdown of why an execution is passing/failing"""

    all_steps_evaluated: bool
    all_steps_passed: bool
    no_warnings: bool
    no_xsd_errors: bool
    execution_complete: bool

    # Some basic metadata
    total_warnings: int
    total_xsd_errors: int
    total_steps_passed: int
    total_steps: int

    created_at: datetime

    def __init__(self, context: ExecutionContext, execute_result: ExecutionResult) -> None:
        self.total_warnings = len(context.warnings.warnings)
        self.total_xsd_errors = sum((bool(r.xsd_errors) for r in context.responses.responses))
        self.total_steps_passed = sum((sr.is_passed() for sr in context.progress.all_results))
        self.total_steps = len(context.test_procedure.steps)

        self.all_steps_evaluated = len(context.progress.all_results) == self.total_steps
        self.all_steps_passed = self.total_steps_passed == self.total_steps
        self.no_warnings = self.total_warnings == 0
        self.no_xsd_errors = self.total_xsd_errors == 0
        self.execution_complete = execute_result.completed

        self.created_at = utc_now()

    def has_passed(self) -> bool:
        """Returns True if EVERYTHING about the execution appears to be passing"""
        return (
            self.all_steps_evaluated
            and self.all_steps_passed
            and self.no_warnings
            and self.no_xsd_errors
            and self.execution_complete
        )


def context_relative_time(context: ExecutionContext, dt: datetime) -> str:
    """Returns the time relative to context as a human readable string"""
    return relative_time(dt - context.created_at)
