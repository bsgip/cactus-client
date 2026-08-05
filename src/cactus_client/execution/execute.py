import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace

from cactus_client.action import execute_action
from cactus_client.admin import get_plugin_manager
from cactus_client.check import execute_checks
from cactus_client.check.end_device import match_aggregator_end_device
from cactus_client.check.sep2 import is_invalid_resource
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, ExecutionResult, StepExecution
from cactus_client.model.parameter import resolve_variable_expressions_from_parameters
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def validate_all_resources(context: ExecutionContext) -> None:
    """Enumerates the resource store - running validation on all stored resources and appending errors to the context
    warnings"""
    server_pen = context.server_config.pen
    for client_context in context.clients_by_alias.values():
        aggregator_edev = match_aggregator_end_device(client_context.discovered_resources, client_context.client_config)
        for sr in client_context.discovered_resources.resources():
            error = is_invalid_resource(sr, server_pen, aggregator_edev)
            if error:
                context.warnings.log_stored_resource_warning(sr, error)


@asynccontextmanager
async def setup_and_teardown(context: ExecutionContext) -> AsyncIterator[ActionResult]:
    """Calls admin plugin setup before yielding, and guarantees teardown runs afterwards.

    Yields the ActionResult from setup so the caller can bail early on failure.
    Teardown exceptions are caught and logged rather than propagated.
    """
    pm = get_plugin_manager()

    admin_context = context.to_admin_context()

    logger.debug("Running admin setup")
    setup_results = await pm.ahook.admin_setup(context=admin_context)
    setup_result: ActionResult = next((r for r in setup_results if not r.completed), ActionResult.done())
    if not setup_result.completed:
        logger.error("Admin setup failed: %s", setup_result.description)
    else:
        logger.debug("Admin setup complete")

    try:
        yield setup_result
    finally:
        logger.debug("Running admin teardown")
        try:
            await pm.ahook.admin_teardown(context=admin_context)
        except Exception as exc:
            logger.error("Admin teardown error", exc_info=exc)
        logger.debug("Admin teardown complete")


async def _handle_step_exception(
    context: ExecutionContext,
    current_step: StepExecution,
    exc: Exception,
    label: str,
) -> ExecutionResult:
    """Log a step action/check exception, update progress, and return a failed result."""
    logger.error("%s exception", label, exc_info=exc)
    await context.progress.add_step_execution_exception(current_step, exc)
    return ExecutionResult(completed=False)


async def execute_for_context(context: ExecutionContext) -> ExecutionResult:
    """Does the actual execution work - will operate until the context's step list is fully drained. Will also
    handle updating trackers as the steps execute.

    If any step reports failure - execution will be stopped.
    Caller is responsible for wrapping this in setup_and_teardown."""

    logger.info("[admin-instruction] test=%s started", context.test_procedure_id)
    result = await _execute_steps(context)
    logger.info(
        "[admin-instruction] test=%s finished completed=%s",
        context.test_procedure_id,
        result.completed,
    )
    return result


async def _fire_admin_instructions(context: ExecutionContext, current_step: StepExecution) -> str | None:
    """Fire each admin instruction for the step via the plugin manager, logging unhandled instructions.

    Returns the reason from the first plugin that requested a skip of this step (if any)."""
    pm = get_plugin_manager()
    admin_context = context.to_admin_context()
    skip_reason: str | None = None
    for instr in current_step.source.admin_instructions or []:
        client_config = admin_context.client_config_for(instr.client)
        resolved_params = await resolve_variable_expressions_from_parameters(client_config, instr.parameters)
        instr = replace(instr, parameters=resolved_params)
        logger.info(
            "[admin-instruction] step=%s type=%s params=%s",
            current_step.source.id,
            instr.type,
            instr.parameters,
        )
        results = await pm.ahook.admin_instruction(instruction=instr, step=current_step, context=admin_context)
        if not any(r is not None for r in results):
            logger.info(
                "[admin-instruction] no plugin handled type=%s in step=%s",
                instr.type,
                current_step.source.id,
            )
            continue

        requested_skip = next((r.skip_reason for r in results if r is not None and r.skip_reason is not None), None)
        if requested_skip is not None and skip_reason is None:
            logger.warning(
                "[admin-instruction] skip of step=%s requested: %s",
                current_step.source.id,
                requested_skip,
            )
            skip_reason = requested_skip

    return skip_reason


async def _execute_steps(context: ExecutionContext) -> ExecutionResult:
    """Inner execution loop extracted from execute_for_context to allow CancelledError handling at the top level."""
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

        # Fire admin instructions before the first attempt at this step
        skip_reason: str | None = None
        if current_step.attempts == 0 and current_step.source.admin_instructions:
            try:
                skip_reason = await _fire_admin_instructions(context, current_step)
            except Exception as exc:
                return await _handle_step_exception(context, current_step, exc, "Admin instruction")

        try:
            action_result = await execute_action(current_step, context)
        except Exception as exc:
            return await _handle_step_exception(context, current_step, exc, "Action")

        try:
            check_result = await execute_checks(current_step, context)
        except Exception as exc:
            return await _handle_step_exception(context, current_step, exc, "Check")

        await context.progress.add_step_execution_completion(current_step, action_result, check_result)

        # Combine action and check results - step passes only if both pass
        step_passed = action_result.completed and check_result.passed

        # Depending on how the step ran - we may need to add a repeat or requeue
        if action_result.repeat and (step_passed or skip_reason is not None):
            # The action is still mid-flight and asked for a repeat (honoured for skipped steps too)
            repeat_step = replace(
                current_step,
                repeat_number=current_step.repeat_number + 1,
                attempts=0,
                not_before=action_result.not_before,
            )
            context.steps.add(repeat_step)
        elif skip_reason is not None:
            if context.allow_skips:
                # The step ran but its pass/fail judgement is waived.
                # It is deliberately never re-queued via repeat_until_pass - it would re-queue forever.
                await context.progress.set_step_result(
                    current_step,
                    action_result,
                    check_result,
                    skip_reason=skip_reason,
                )
            else:
                failure = ActionResult.failed(f"skip requested: {skip_reason}; skips are not enabled")
                await context.progress.set_step_result(current_step, failure, check_result)
                break
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
