import logging
from http import HTTPMethod
from typing import Any

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource

from cactus_client.action.server import (
    client_error_request_for_step,
    get_resource_for_step,
    request_for_step,
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

    for resource in matching_resources:
        href = resource.resource.href

        if href is None:  # Skip resources without a href
            continue

        if expect_rejection is True:
            await client_error_request_for_step(step, context, href, HTTPMethod.GET)

        elif expect_rejection_or_empty is True:
            result = await _handle_expected_rejection_or_empty(step, context, href, resource_type, resource)
            if not result.completed:
                return result

        else:
            # If not expected to fail, actually request the resource and upsert in the resource store
            try:
                fetched_resource = await get_resource_for_step(type(resource.resource), step, context, href)
            except RequestException as exc:
                logger.error(f"Error refreshing {href}", exc_info=exc)
                if expect_rejection is False:
                    return ActionResult.failed(f"Error: {exc}")
                raise
            resource_store.upsert_resource(resource_type, resource.id.parent_id(), fetched_resource)

    return ActionResult.done()


async def _handle_expected_rejection_or_empty(
    step: StepExecution, context: ExecutionContext, href: str, resource_type: CSIPAusResource, resource_instance: Any
) -> ActionResult:
    """Verify that a request is either rejected OR returns an empty list.

    Returns ActionResult.failed() for retriable failures (e.g., list not empty yet),
    raises CactusClientException for fatal errors."""

    response = await request_for_step(step, context, href, HTTPMethod.GET)

    # Case 1: Expected rejection
    if response.is_client_error():
        await client_error_request_for_step(step, context, href, HTTPMethod.GET)
        return ActionResult.done()

    # Case 2: Success (must be an empty list resource)
    if response.is_success():
        if not is_list_resource(resource_type):
            raise CactusClientException(
                f"Expected rejection or empty for {resource_type} at {href}, "
                f"but got {response.status} for non-list resource"
            )

        fetched_resource = await get_resource_for_step(type(resource_instance.resource), step, context, href)

        # Check if list is empty - this is a retriable failure
        if not (hasattr(fetched_resource, "all_") and fetched_resource.all_ == 0):
            return ActionResult.failed(
                f"Expected rejection or empty list for {resource_type} at {href}, but got non-empty list."
            )
        return ActionResult.done()

    # Any other status code is unexpected
    raise CactusClientException(f"Unexpected status {response.status} for {href} in expect_rejection_or_empty")
