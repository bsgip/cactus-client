import logging

from cactus_client.action.discovery import action_discovery
from cactus_client.action.end_device import (
    action_insert_end_device,
    action_upsert_connection_point,
)
from cactus_client.action.mup import action_insert_readings, action_upsert_mup
from cactus_client.action.noop import action_noop
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.parameter import resolve_variable_expressions_from_parameters

logger = logging.getLogger(__name__)


async def execute_action(step: StepExecution, context: ExecutionContext) -> ActionResult:
    """Given a step and context - execute the appropriate action for that step (or raise a CactusClientException)"""

    action_info = step.source.action

    client_config = context.client_config(step)

    try:
        resolved_params = await resolve_variable_expressions_from_parameters(client_config, action_info.parameters)
    except Exception as exc:
        logger.error(f"Exception resolving parameters for action in step: {step.source.id}", exc_info=exc)
        raise CactusClientException(
            f"There was an error parsing parameters for the action in step: {step.source.id}."
            + " This is a problem with the test definition itself."
        )

    match (action_info.type):
        case "no-op":
            return await action_noop()
        case "discovery":
            return await action_discovery(resolved_params, step, context)
        case "insert-end-device":
            return await action_insert_end_device(resolved_params, step, context)
        case "upsert-connection-point":
            return await action_upsert_connection_point(resolved_params, step, context)
        case "upsert-mup":
            return await action_upsert_mup(resolved_params, step, context)
        case "insert-readings":
            return await action_insert_readings(resolved_params, step, context)
        case _:
            logger.error(f"Unrecognised action type {action_info.type} in step {step.source.id}")
            raise CactusClientException(
                f"Unrecognised action type {action_info.type} in step {step.source.id}."
                + " This is a problem with the test definition itself."
            )
