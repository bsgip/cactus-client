import asyncio
import logging
from dataclasses import replace

from cactus_client.action import execute_action
from cactus_client.check import execute_checks
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


async def execute_for_context(context: ExecutionContext) -> ExecutionResult:
    """Does the actual execution work - will operate until the context's step list is fully drained. Will also
    handle updating trackers as the steps execute.

    If any step reports failure - execution will be stopped"""

    while (upcoming_step := context.steps.peek_next_no_wait(now := utc_now())) is not None:

        # Sometimes the next step will have a "not before" time - in which case we delay until that time has passed
        # We do this via peeking so we can log the delay against that upcoming step without popping it off the queue
        delay_required = upcoming_step.executable_delay_required(now)
        if delay_required:
            await context.progress.update_current_step(upcoming_step, delay=delay_required)
            await asyncio.sleep(delay_required.seconds)
            continue

        # We're ready to commit to running the next step
        current_step = context.steps.pop(now)
        if current_step is None:
            continue  # Shouldn't happen due to our earlier wait

        # Start the step execution and checking
        await context.progress.update_current_step(current_step, delay=None)
        try:
            action_result = await execute_action(current_step, context)
        except Exception as exc:
            logger.error("Action exception", exc_info=exc)
            await context.progress.add_step_execution_exception(current_step, exc)
            return ExecutionResult(completed=False)

        try:
            check_result = await execute_checks(current_step, context)
        except Exception as exc:
            logger.error("Check exception", exc_info=exc)
            await context.progress.add_step_execution_exception(current_step, exc)
            return ExecutionResult(completed=False)

        await context.progress.add_step_execution_completion(current_step, action_result, check_result)

        # Depending on how the step ran - we may need to add a repeat or requeue
        if check_result.passed and action_result.repeat:
            # The step was successful, but asked for a repeat
            repeat_step = replace(
                current_step,
                repeat_number=current_step.repeat_number + 1,
                attempts=0,
                not_before=action_result.not_before,
            )
            context.steps.add(repeat_step)
        elif not check_result.passed and current_step.source.repeat_until_pass:
            # The step failed - but it might be marked as repeat_until_pass
            repeat_step = replace(current_step, attempts=current_step.attempts + 1, not_before=None)
            context.steps.add(repeat_step)

            # This can potentially result in a tight loop - so we add a delay
            await context.progress.update_current_step(repeat_step, delay=context.repeat_delay)
            await asyncio.sleep(context.repeat_delay.seconds)
        else:
            # At this point - we aren't re-queuing a repeat, therefore this step is now "done" (pass or fail)
            await context.progress.set_step_result(current_step, check_result)

            # If this step failed - no point continuing, it's likely downstream steps will also fail
            if not check_result.passed:
                break

    return ExecutionResult(completed=True)
