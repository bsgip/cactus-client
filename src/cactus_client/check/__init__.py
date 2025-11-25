import logging

from cactus_client.check.der import check_der_program
from cactus_client.check.der_controls import (
    check_default_der_control,
    check_der_control,
)
from cactus_client.check.discovered import check_discovered
from cactus_client.check.end_device import check_end_device, check_end_device_list
from cactus_client.check.function_set_assignment import check_function_set_assignment
from cactus_client.check.mup import check_mirror_usage_point
from cactus_client.check.time import check_poll_rate, check_time_synced
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.parameter import resolve_variable_expressions_from_parameters

logger = logging.getLogger(__name__)


async def execute_checks(step: StepExecution, context: ExecutionContext) -> CheckResult:
    """Given a step and context - execute all post action checks - returning the first failure (or a passing result)"""

    if not step.source.checks:
        return CheckResult(True, None)  # No checks to run

    client_config = context.client_config(step)
    for check in step.source.checks:
        try:
            resolved_params = await resolve_variable_expressions_from_parameters(client_config, check.parameters)
        except Exception as exc:
            logger.error(f"Exception resolving parameters for check {check.type} in {step.source.id}", exc_info=exc)
            raise CactusClientException(
                f"There was an error parsing parameters for check {check.type} in {step.source.id}."
                + " This is a problem with the test definition itself."
            )

        last_result: CheckResult | None = None
        match (check.type):
            case "discovered":
                last_result = check_discovered(resolved_params, step, context)
            case "time-synced":
                last_result = check_time_synced(step, context)
            case "end-device":
                last_result = check_end_device(resolved_params, step, context)
            case "end-device-list":
                last_result = check_end_device_list(resolved_params, step, context)
            case "function-set-assignment":
                last_result = check_function_set_assignment(resolved_params, step, context)
            case "mirror-usage-point":
                last_result = check_mirror_usage_point(resolved_params, step, context)
            case "der-control":
                last_result = check_der_control(resolved_params, step, context)
            case "default-der-control":
                last_result = check_default_der_control(resolved_params, step, context)
            case "der-program":
                last_result = check_der_program(resolved_params, step, context)
            case "poll-rate":
                last_result = check_poll_rate(resolved_params, step, context)
            case _:
                logger.error(f"Unrecognised check type {check.type} in step {step.source.id}")
                raise CactusClientException(
                    f"Unrecognised check type {check.type} in step {step.source.id}."
                    + " This is a problem with the test definition itself."
                )

        if not last_result.passed:
            return last_result

    return CheckResult(True, None)
