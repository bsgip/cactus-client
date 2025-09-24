import asyncio

from cactus_client.action import execute_action
from cactus_client.check import execute_checks
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult
from cactus_client.time import utc_now


async def execute(context: ExecutionContext) -> ExecutionResult:
    """Does the actual execution work - will operate until the context's step list is fully drained. Will also
    handle precondition management"""

    now = utc_now()
    while (upcoming_step := context.steps.peek_next_no_wait(now)) is not None:

        # Sometimes the next step will have a "not before" time - in which case we delay until that time has passed
        delay_required = upcoming_step.executable_delay_required(now)
        if delay_required:
            await context.progress.log_step_progress(
                upcoming_step, f"Delaying execution for{int(delay_required.seconds)}s"
            )
            await asyncio.sleep(delay_required.seconds)
            now = utc_now()
            continue

        # We're ready to commit to running the next step
        next_step = context.steps.pop(now())
        if next_step is None:
            continue  # Shouldn't happen due to our earlier wait

        # At this point - we're free to execute the step
        try:
            action_result = await execute_action(next_step, context)
        except CactusClientException as exc:
            await context.progress.log_step_progress(upcoming_step, f"Exception raised while executing step {exc}")
            return ExecutionResult(completed=False)
        
        try:
            check_result = await execute_checks(next_step, context)
        except CactusClientException as exc:
            await context.progress.log_step_progress(upcoming_step, f"Exception raised while checking step {exc}")
            return ExecutionResult(completed=False)
        
        if check_result.passed:

    return ExecutionResult(completed=True)
