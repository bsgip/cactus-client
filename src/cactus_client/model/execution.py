from dataclasses import dataclass, field
from datetime import datetime, timedelta

from cactus_test_definitions.server.test_procedures import Step

from cactus_client.time import utc_now

MAX_PRIMACY = 0xEFFFFFFF  # If we're dealing with primacies bigger than this - something has gone wrong


@dataclass
class CheckResult:
    """Represents the results of a running a single check"""

    passed: bool  # True if the check is considered passed or successful. False otherwise
    description: str | None  # Human readable description of what the check "considered" or wants to elaborate about


@dataclass
class ActionResult:
    repeat: bool  # If true - this will trigger the action to retrigger again (with a higher repeat number)
    not_before: datetime | None  # If repeat is true - this will be the new value for StepExecution.not_before

    @staticmethod
    def done() -> "ActionResult":
        """Shorthand for generating a "completed" StepExecution"""
        return ActionResult(False, None)


@dataclass
class ExecutionResult:
    """Represents the final result from a full execution of a test procedure"""

    completed: bool  # True if the execution completed without an exception being raised (successful or not)
    created_at: datetime = field(default_factory=utc_now, init=False)


@dataclass
class StepExecution:
    """Represents a planned execution of a Step's actions/checks."""

    source: Step  # What step is the parent for this execution
    client_alias: str  # What client will executing this step?
    client_resources_alias: str  # What client will be supplying a ResourceStore for this step (usually client_alias)
    primacy: int  # Lower primacy = higher priority - usually based from the position in the step list
    repeat_number: int  # Some Steps might repeat a number of times - this is how many prior executions have occurred
    not_before: datetime | None  # If set - this step cannot start execution until after this point in time

    attempts: int  # How many times has this step been attempted

    def executable_delay_required(self, now: datetime) -> timedelta:
        """Can this step be executed at this exact moment based on not_before? If so - return timedelta(0) otherwise
        return the delay required"""
        if self.not_before is None or self.not_before <= now:
            return timedelta(0)

        return self.not_before - now


class StepExecutionList:
    """Really simply "priority queue" of StepExecution elements"""

    # This could be optimised for lookup speed but realistically we aren't going to have more than ~10 items in here
    # so doing everything in O(n) time will be more than sufficient
    _items: list[StepExecution]

    def __init__(self) -> None:
        self._items = []

    def __len__(self) -> int:
        return len(self._items)

    def peek(self, now: datetime) -> StepExecution | None:
        """Returns the lowest primacy StepExecution whose not_before is either None or <= now. Does NOT remove the
        step from the list."""
        lowest_primacy = 0xEFFFFFFF  # If we're dealing with primacies bigger than this - something has gone wrong
        lowest: StepExecution | None = None
        for se in self._items:
            if se.not_before is not None and se.not_before > now:
                continue

            if se.primacy < lowest_primacy:
                lowest = se
                lowest_primacy = se.primacy

        return lowest

    def peek_next_no_wait(self, now: datetime) -> StepExecution | None:
        """Similar to peek - but if nothing is immediately available - picks the item with the soonest not_before"""
        next_step = self.peek(now)
        if next_step is not None:
            return next_step

        # Time to search for the lowest not_before
        earliest_not_before = datetime(9999, 1, 1, tzinfo=now.tzinfo)
        earliest: StepExecution | None = None
        for se in self._items:
            # Should technically not happen due to the earlier call to peek but just in case
            if se.not_before is None:
                return se

            if se.not_before < earliest_not_before:
                earliest_not_before = se.not_before
                earliest = se

        return earliest

    def time_until_next(self, now: datetime) -> timedelta | None:
        """Calculates the time until the next item from pop() will be available. Returns timedelta(0) if something is
        available now. Returns None if there is nothing left in the list"""

        if len(self._items) == 0:
            return None

        next_item = self.peek_next_no_wait(now)
        if next_item is None:
            return None

        if next_item.not_before is None or next_item.not_before <= now:
            return timedelta(0)

        return next_item.not_before - now

    def pop(self, now: datetime) -> StepExecution | None:
        """Finds the lowest primacy StepExecution (whose not_before is <= now), removes it from the list and returns
        it"""
        next_step = self.peek(now)
        if next_step is not None:
            self._items.remove(next_step)
        return next_step

    def add(self, se: StepExecution) -> None:
        self._items.append(se)
