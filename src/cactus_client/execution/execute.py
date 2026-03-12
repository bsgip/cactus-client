import asyncio
import logging
from dataclasses import replace
from contextlib import asynccontextmanager

from cactus_client.action import execute_action, ActionResult
from cactus_client.check import execute_checks
from cactus_client.check.sep2 import is_invalid_resource
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult, StepExecution, CheckResult
from cactus_client.time import utc_now

from typing import AsyncIterator

logger = logging.getLogger(__name__)


def validate_all_resources(context: ExecutionContext) -> None:
    """Enumerates the resource store - running validation on all stored resources and appending errors to the context
    warnings"""
    server_pen = context.server_config.pen
    for client_context in context.clients_by_alias.values():
        for sr in client_context.discovered_resources.resources():
            error = is_invalid_resource(sr, server_pen)
            if error:
                context.warnings.log_stored_resource_warning(sr, error)


@asynccontextmanager
async def setup_and_teardown(context: ExecutionContext) -> AsyncIterator[ActionResult]:
    """Executes plugin admin setup and teardown functions as an async context manager."""
    dummy_check_result = CheckResult(passed=True, description="No checks provided")

    # Setup
    setup_step = StepExecution.admin_setup()
    logger.debug("====== Running admin setup ======")

    try:
        action_result = await execute_action(setup_step, context)
        await context.progress.set_step_result(setup_step, action_result, dummy_check_result)
        logger.debug("===== Admin setup complete =====")
        # Yield the action result
        yield action_result

    except Exception as exc:
        logger.error("Admin setup exception", exc_info=exc)
        await context.progress.add_step_execution_exception(setup_step, exc)
        raise

    finally:
        # Teardown
        logger.debug("====== Running admin teardown ======")
        teardown_step = StepExecution.admin_teardown()
        try:
            action_result = await execute_action(teardown_step, context)
            await context.progress.set_step_result(teardown_step, action_result, dummy_check_result)
        except Exception as exc:
            logger.error("Admin teardown exception", exc_info=exc)
            await context.progress.add_step_execution_exception(teardown_step, exc)

        logger.debug("====== Admin teardown complete ======")


async def execute_for_context(context: ExecutionContext) -> ExecutionResult:
    """Does the actual execution work - will operate until the context's step list is fully drained. Will also
    handle updating trackers as the steps execute.

    If any step reports failure - execution will be stopped.

    Args:
        context: Contains all elements necessary for execution state

    Returns:
        Overall result of the test execution
    """
    # Context manager with the setup and teardown functionality
    # async with setup_and_teardown(context) as setup_result:
    #    if not setup_result.completed:
    #        return ExecutionResult(completed=False)

    if True:  # TODO remove temporary bridge
        while (upcoming_step := context.steps.peek_next_no_wait(now := utc_now())) is not None:

            # Sometimes the next step will have a "not before" time - in which case we delay until that time has passed
            # We do this via peeking so we can log the delay against that upcoming step without popping it off the queue
            delay_required = upcoming_step.executable_delay_required(now)
            if delay_required:
                await context.progress.update_current_step(upcoming_step, delay=delay_required)
                await asyncio.sleep(delay_required.total_seconds())
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

            # Combine action and check results - step passes only if both pass
            step_passed = action_result.completed and check_result.passed

            # Depending on how the step ran - we may need to add a repeat or requeue
            if step_passed and action_result.repeat:
                # The step was successful, but asked for a repeat
                repeat_step = replace(
                    current_step,
                    repeat_number=current_step.repeat_number + 1,
                    attempts=0,
                    not_before=action_result.not_before,
                )
                context.steps.add(repeat_step)
            elif not step_passed and current_step.source.repeat_until_pass:
                # The step failed (action or check) - but it might be marked as repeat_until_pass
                repeat_step = replace(current_step, attempts=current_step.attempts + 1, not_before=None)
                context.steps.add(repeat_step)

                # This can potentially result in a tight loop - so we add a delay
                await context.progress.update_current_step(repeat_step, delay=context.repeat_delay)
                await asyncio.sleep(context.repeat_delay.seconds)
            else:
                # At this point - we aren't re-queuing a repeat, therefore this step is now "done" (pass or fail)
                await context.progress.set_step_result(current_step, action_result, check_result)

                # If this step failed - no point continuing, it's likely downstream steps will also fail
                if not step_passed:
                    break

    # We do resource validation at the very end - it's easier than trying to identify resource changes after each step
    validate_all_resources(context)

    return ExecutionResult(completed=True)
