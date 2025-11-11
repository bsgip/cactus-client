import asyncio
from datetime import datetime
from typing import Any, Callable, cast

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.der import DERControlListResponse, DERListResponse, DERProgramListResponse
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import FunctionSetAssignmentsListResponse
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePointListResponse
from envoy_schema.server.schema.sep2.pub_sub import SubscriptionListResponse

from cactus_client.action.server import get_resource_for_step, paginate_list_resource_items
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import RESOURCE_SEP2_TYPES, ResourceStore
from cactus_client.time import utc_now

DISCOVERY_LIST_PAGE_SIZE = 3  # We want something suitably small (to ensure pagination is tested)


def calculate_wait_next_polling_window(now: datetime, discovered_resources: ResourceStore) -> int:
    """Calculates the wait until the next whole minute(s) based on DeviceCapability poll rate (defaults to 60 seconds).

    Returns the delay in seconds.
    """

    dcaps = discovered_resources.get(CSIPAusResource.DeviceCapability)
    if len(dcaps) == 0:
        poll_rate_seconds = 60
    else:
        poll_rate_seconds = cast(DeviceCapabilityResponse, dcaps[0].resource).pollRate or 60

    now_seconds = int(now.timestamp())
    return poll_rate_seconds - (now_seconds % poll_rate_seconds)


def check_item_for_href(step: StepExecution, context: ExecutionContext, href: str, item: Resource) -> Resource:
    if not item.href:
        context.warnings.log_step_warning(step, f"Entity at {href} was returned with no href.")
    return item


async def discover_resource(resource: CSIPAusResource, step: StepExecution, context: ExecutionContext) -> None:
    """Performs discovery for the particular resource - it is assumed that all parent resources have been previously
    fetched."""

    resource_store = context.discovered_resources(step)
    resource_store.clear_resource(resource)

    # Find the link / parent list that we will be querying
    # We need to check if there is a direct Link.href reference to this resource (from a parent)
    # We need to also check if the parent resource is a list type and this resource is a member of that list
    parent_resource = context.resource_tree.parent_resource(resource)
    if parent_resource is None:
        # We have device capability - this is a special case
        resource_store.append_resource(
            CSIPAusResource.DeviceCapability,
            None,
            check_item_for_href(
                step,
                context,
                context.dcap_path,
                await get_resource_for_step(DeviceCapabilityResponse, step, context, context.dcap_path),
            ),
        )
        return

    if is_list_resource(parent_resource):
        # If this is a member of a list (eg resource is EndDevice and parent_resource is EndDeviceList)

        # We need to know how to decompose a parent list to get at the child items
        get_list_items: Callable[[Resource], list[Resource] | None] | None = None
        match (parent_resource):
            case CSIPAusResource.MirrorUsagePointList:
                get_list_items = lambda list_: cast(MirrorUsagePointListResponse, list_).mirrorUsagePoints  # type: ignore # noqa: E731
            case CSIPAusResource.EndDeviceList:
                get_list_items = lambda list_: cast(EndDeviceListResponse, list_).EndDevice  # type: ignore # noqa: E731
            case CSIPAusResource.DERList:
                get_list_items = lambda list_: cast(DERListResponse, list_).DER_  # type: ignore # noqa: E731
            case CSIPAusResource.DERProgramList:
                get_list_items = lambda list_: cast(DERProgramListResponse, list_).DERProgram  # type: ignore # noqa: E731
            case CSIPAusResource.DERControlList:
                get_list_items = lambda list_: cast(DERControlListResponse, list_).DERControl  # type: ignore # noqa: E731
            case CSIPAusResource.FunctionSetAssignmentsList:
                get_list_items = lambda list_: cast(FunctionSetAssignmentsListResponse, list_).FunctionSetAssignments  # type: ignore # noqa: E731
            case CSIPAusResource.SubscriptionList:
                get_list_items = lambda list_: cast(SubscriptionListResponse, list_).subscriptions  # type: ignore # noqa: E731

        if get_list_items is None:
            raise CactusClientException(f"resource {parent_resource} has no registered get_list_items function.")

        # Each of our parent resources will be a List - time to paginate through them
        for parent_sr in resource_store.get(parent_resource):
            list_href = parent_sr.resource.href
            if not list_href:
                continue

            # Paginate through each of the lists - each of those items are the things we want to store
            list_items = await paginate_list_resource_items(
                RESOURCE_SEP2_TYPES[parent_resource], step, context, list_href, DISCOVERY_LIST_PAGE_SIZE, get_list_items
            )
            for item in list_items:
                resource_store.append_resource(resource, parent_sr, check_item_for_href(step, context, list_href, item))
    else:
        # Not a list item - look for direct links from parent (eg an EndDevice.ConnectionPointLink -> ConnectionPoint)
        for parent_sr in resource_store.get(parent_resource):
            href = parent_sr.resource_link_hrefs.get(resource, None)
            if href:
                resource_store.append_resource(
                    resource,
                    parent_sr,
                    check_item_for_href(
                        step,
                        context,
                        href,
                        await get_resource_for_step(RESOURCE_SEP2_TYPES[resource], step, context, href),
                    ),
                )


async def action_discovery(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    resources: list[CSIPAusResource] = resolved_parameters["resources"]  # Mandatory param
    next_polling_window: bool = resolved_parameters.get("next_polling_window", False)
    now = utc_now()
    discovered_resources = context.discovered_resources(step)

    # We may hold up execution waiting for the next polling window
    if next_polling_window:
        delay_seconds = calculate_wait_next_polling_window(now, discovered_resources)
        await context.progress.add_log(step, f"Delaying {delay_seconds}s until next polling window.")
        await asyncio.sleep(delay_seconds)

    # Start making requests for resources
    for resource in context.resource_tree.discover_resource_plan(resources):
        await discover_resource(resource, step, context)

    return ActionResult.done()
