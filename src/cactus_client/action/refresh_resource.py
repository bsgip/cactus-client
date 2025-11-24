from http import HTTPMethod
from typing import Any

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource

from cactus_client.action.server import (
    client_error_request_for_step,
    get_resource_for_step,
    request_for_step,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import StoredResource


async def action_refresh_resource(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Refresh a resource from the server using the resources href and update the resource store"""

    # Retrieve params
    resource_type: CSIPAusResource = CSIPAusResource(resolved_parameters["resource"])
    expect_rejection: bool = resolved_parameters.get("expect_rejection", False)
    expect_rejection_or_empty: bool = resolved_parameters.get("expect_rejection_or_empty", False)

    resource_store = context.discovered_resources(step)
    matching_resources: list[StoredResource] = resource_store.get_for_type(resource_type)

    if len(matching_resources) == 0:
        raise CactusClientException(f"Expected matching resources to refresh for resource {resource_type}. None found.")

    for resource in matching_resources:
        href = resource.resource.href

        if href is None:  # Skip resources without a href
            continue

        if expect_rejection:
            await client_error_request_for_step(step, context, href, HTTPMethod.GET)

        elif expect_rejection_or_empty:
            await _handle_expected_rejection_or_empty(step, context, href, resource_type, resource)

        # If not expected to fail, actually request the resource and upsert in the resource store
        else:
            fetched_resource = await get_resource_for_step(type(resource.resource), step, context, href)
            resource_store.upsert_resource(resource_type, resource.id.parent_id(), fetched_resource)

    return ActionResult.done()


async def _handle_expected_rejection_or_empty(
    step: StepExecution, context: ExecutionContext, href: str, resource_type: CSIPAusResource, resource_instance: Any
) -> None:
    """Verify that a request is either rejected OR returns an empty list."""

    response = await request_for_step(step, context, href, HTTPMethod.GET)

    # Case 1: Expected rejection
    if response.is_client_error():
        await client_error_request_for_step(step, context, href, HTTPMethod.GET)
        return

    # Case 2: Success (must be an empty list resource)
    if response.is_success():
        if not is_list_resource(resource_type):
            raise CactusClientException(
                f"Expected rejection or empty for {resource_type} at {href}, "
                f"but got {response.status} for non-list resource"
            )

        fetched_resource = await get_resource_for_step(type(resource_instance.resource), step, context, href)

        # Check if list is empty
        if not (hasattr(fetched_resource, "all_") and fetched_resource.all_ == 0):
            raise CactusClientException(
                f"Expected rejection or empty list for {resource_type} at {href}, but got non-empty list."
            )
        return

    # Any other status code is unexpected
    raise CactusClientException(f"Unexpected status {response.status} for {href} in expect_rejection_or_empty")
