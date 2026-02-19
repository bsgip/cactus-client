import logging
from http import HTTPMethod
from typing import Any, cast

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource

from cactus_client.action.server import (
    client_error_or_empty_list_request_for_step,
    client_error_request_for_step,
    get_resource_for_step,
)
from cactus_client.error import CactusClientException, RequestException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import StoredResource

logger = logging.getLogger(__name__)


async def action_refresh_resource(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Refresh a resource from the server using the resources href and update the resource store"""

    # Retrieve params
    resource_type: CSIPAusResource = CSIPAusResource(resolved_parameters["resource"])
    expect_rejection: bool | None = resolved_parameters.get("expect_rejection", None)
    expect_rejection_or_empty: bool = resolved_parameters.get("expect_rejection_or_empty", False)

    resource_store = context.discovered_resources(step)
    matching_resources: list[StoredResource] = resource_store.get_for_type(resource_type)

    if len(matching_resources) == 0:
        raise CactusClientException(f"Expected matching resources to refresh for resource {resource_type}. None found.")

    for sr in matching_resources:
        href = sr.resource.href

        if href is None:  # Skip resources without a href
            continue

        try:
            if expect_rejection:
                await client_error_request_for_step(step, context, href, HTTPMethod.GET)
            elif expect_rejection_or_empty:
                if is_list_resource(resource_type):
                    await client_error_or_empty_list_request_for_step(
                        cast(Any, type(sr.resource)), step, context, href, HTTPMethod.GET
                    )
                else:
                    await client_error_request_for_step(step, context, href, HTTPMethod.GET)
            else:
                # If not expected to fail, actually request the resource and upsert in the resource store
                fetched_resource = await get_resource_for_step(type(sr.resource), step, context, href)
                resource_store.upsert_resource(resource_type, sr.id.parent_id(), fetched_resource)

        except RequestException as exc:
            # We will bundle up RequestException as a "retryable" failure
            logger.error(f"Request error refreshing {href}", exc_info=exc)
            return ActionResult.failed(f"Request error: {exc}")

    return ActionResult.done()
