from dataclasses import dataclass
from datetime import datetime, timedelta
from ssl import SSLContext

from aiohttp import ClientSession
from cactus_test_definitions.server.test_procedures import Step


@dataclass
class ActionResult:
    repeat: bool  # If true - this will trigger the action to retrigger again (with a higher repeat number)
    not_before: datetime | None  # If repeat is true - this will be the new value for StepExecution.not_before

    @staticmethod
    def done() -> "ActionResult":
        """Shorthand for generating a "completed" StepExecution"""
        return ActionResult(False, None)


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


class StepExecutionList:
    """Really simply "priority queue" of StepExecution elements"""

    # This could be optimised for lookup speed but realistically we aren't going to have more than ~10 items in here
    # so doing everything in O(n) time will be more than sufficient
    _items: list[StepExecution]

    def __init__(self) -> None:
        self._items = []

    def __len__(self) -> int:
        return len(self._items)

    def time_until_next(self, now: datetime) -> timedelta | None:
        """Calculates the time until the next item from pop() will be available. Returns timedelta(0) if something is
        available now. Returns None if there is nothing left in the list"""

        if len(self._items) == 0:
            return None

        earliest_not_before = datetime(9999, 1, 1, tzinfo=now.tzinfo)
        for se in self._items:
            if se.not_before is None:
                return timedelta(0)
            elif se.not_before < earliest_not_before:
                earliest_not_before = se.not_before

        # Don't serve a negative delta
        if earliest_not_before <= now:
            return timedelta(0)

        return earliest_not_before - now

    def pop(self, now: datetime) -> StepExecution | None:
        """Finds the highest priority StepExecution in the queue (whose not_before is <= now)"""
        lowest_primacy = 0xEFFFFFFF  # If we're dealing with primacies bigger than this - something has gone wrong
        lowest: StepExecution | None = None
        for se in self._items:
            if se.not_before is not None and se.not_before > now:
                continue

            if se.primacy < lowest_primacy:
                lowest = se
                lowest_primacy = se.primacy

        # If we ever start dealing with 100s of steps - this method will need to be improved
        if lowest is not None:
            self._items.remove(lowest)
        return lowest

    def add(self, se: StepExecution) -> None:
        self._items.append(se)
