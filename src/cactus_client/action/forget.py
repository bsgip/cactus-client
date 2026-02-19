from typing import Any

from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution


async def action_forget(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    resources: list[str] = resolved_parameters["resources"]  # Mandatory param
    store = context.discovered_resources(step)

    for resource in resources:
        store.clear_resource(CSIPAusResource(resource))

    return ActionResult.done()
