import logging

from cactus_client.action.der import (
    action_send_malformed_der_settings,
    action_upsert_der_capability,
    action_upsert_der_settings,
    action_upsert_der_status,
)
from cactus_client.action.der_controls import action_respond_der_controls, action_send_malformed_response
from cactus_client.action.discovery import action_discovery
from cactus_client.action.end_device import (
    action_insert_end_device,
    action_upsert_connection_point,
)
from cactus_client.action.mup import action_insert_readings, action_upsert_mup
from cactus_client.action.noop import action_noop
from cactus_client.action.refresh_resource import action_refresh_resource
from cactus_client.action.subscription import (
    action_create_subscription,
    action_delete_subscription,
    action_notifications,
)
from cactus_client.action.wait import action_wait
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
        case "notifications":
            return await action_notifications(resolved_params, step, context)
        case "insert-end-device":
            return await action_insert_end_device(resolved_params, step, context)
        case "upsert-connection-point":
            return await action_upsert_connection_point(resolved_params, step, context)
        case "upsert-mup":
            return await action_upsert_mup(resolved_params, step, context)
        case "insert-readings":
            return await action_insert_readings(resolved_params, step, context)
        case "upsert-der-capability":
            return await action_upsert_der_capability(resolved_params, step, context)
        case "upsert-der-settings":
            return await action_upsert_der_settings(resolved_params, step, context)
        case "upsert-der-status":
            return await action_upsert_der_status(resolved_params, step, context)
        case "send-malformed-der-settings":
            return await action_send_malformed_der_settings(resolved_params, step, context)
        case "respond-der-controls":
            return await action_respond_der_controls(step, context)
        case "send-malformed-response":
            return await action_send_malformed_response(resolved_params, step, context)
        case "refresh-resource":
            return await action_refresh_resource(resolved_params, step, context)
        case "create-subscription":
            return await action_create_subscription(resolved_params, step, context)
        case "delete-subscription":
            return await action_delete_subscription(resolved_params, step, context)
        case "wait":
            return await action_wait(resolved_params)
        case _:
            logger.error(f"Unrecognised action type {action_info.type} in step {step.source.id}")
            raise CactusClientException(
                f"Unrecognised action type {action_info.type} in step {step.source.id}."
                + " This is a problem with the test definition itself."
            )
