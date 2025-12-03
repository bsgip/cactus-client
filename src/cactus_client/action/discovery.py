import asyncio
from datetime import datetime
from typing import Any, Callable, cast

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.der import (
    DERControlListResponse,
    DERListResponse,
    DERProgramListResponse,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
)
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePointListResponse
from envoy_schema.server.schema.sep2.pub_sub import SubscriptionListResponse

from cactus_client.action.server import (
    fetch_list_page,
    get_resource_for_step,
    paginate_list_resource_items,
)
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

    dcaps = discovered_resources.get_for_type(CSIPAusResource.DeviceCapability)
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


def get_list_item_callback(
    list_resource: CSIPAusResource,
) -> tuple[Callable[[Resource], list[Resource] | None], CSIPAusResource]:
    """Generates a callback that when executed (with a Resource) will generate the list of child items that belong
    to that resource.

    list_resource: Should be a list type CSIPAusResource

    raises CactusClientException if list_resource is unsupported

    returns a tuple:
        callback: A callable that takes a Resource and returns a list of child Resources (or None)
        list_item_type: A CSIPAusResource matching the type of the child list items"""
    get_list_items: Callable[[Resource], list[Resource] | None] | None = None
    list_item_type: CSIPAusResource | None = None
    match (list_resource):
        case CSIPAusResource.MirrorUsagePointList:
            get_list_items = lambda list_: cast(MirrorUsagePointListResponse, list_).mirrorUsagePoints  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.MirrorUsagePoint
        case CSIPAusResource.EndDeviceList:
            get_list_items = lambda list_: cast(EndDeviceListResponse, list_).EndDevice  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.EndDevice
        case CSIPAusResource.DERList:
            get_list_items = lambda list_: cast(DERListResponse, list_).DER_  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.DER
        case CSIPAusResource.DERProgramList:
            get_list_items = lambda list_: cast(DERProgramListResponse, list_).DERProgram  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.DERProgram
        case CSIPAusResource.DERControlList:
            get_list_items = lambda list_: cast(DERControlListResponse, list_).DERControl  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.DERControl
        case CSIPAusResource.FunctionSetAssignmentsList:
            get_list_items = lambda list_: cast(FunctionSetAssignmentsListResponse, list_).FunctionSetAssignments  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.FunctionSetAssignments
        case CSIPAusResource.SubscriptionList:
            get_list_items = lambda list_: cast(SubscriptionListResponse, list_).subscriptions  # type: ignore # noqa: E731
            list_item_type = CSIPAusResource.Subscription

    if get_list_items is None or list_item_type is None:
        raise CactusClientException(f"resource {list_resource} has no registered get_list_items function.")

    return (get_list_items, list_item_type)


async def discover_resource(
    resource: CSIPAusResource, step: StepExecution, context: ExecutionContext, list_limit: int | None
) -> None:
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
        get_list_items, _ = get_list_item_callback(parent_resource)

        # Each of our parent resources will be a List - time to paginate through them
        for parent_sr in resource_store.get_for_type(parent_resource):
            list_href = parent_sr.resource.href
            if not list_href:
                continue

            # If list limit exists, make a single query of this length
            if list_limit is not None:
                list_items, _ = await fetch_list_page(
                    RESOURCE_SEP2_TYPES[parent_resource], step, context, list_href, 0, list_limit, get_list_items
                )
            else:
                # Paginate through each of the lists - each of those items are the things we want to store
                list_items = await paginate_list_resource_items(
                    RESOURCE_SEP2_TYPES[parent_resource],
                    step,
                    context,
                    list_href,
                    DISCOVERY_LIST_PAGE_SIZE,
                    get_list_items,
                )

            for item in list_items:
                resource_store.append_resource(
                    resource, parent_sr.id, check_item_for_href(step, context, list_href, item)
                )
    else:
        # Not a list item - look for direct links from parent (eg an EndDevice.ConnectionPointLink -> ConnectionPoint)
        for parent_sr in resource_store.get_for_type(parent_resource):
            href = parent_sr.resource_link_hrefs.get(resource, None)
            if href:
                resource_store.append_resource(
                    resource,
                    parent_sr.id,
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
    resources: list[str] = resolved_parameters["resources"]  # Mandatory param
    next_polling_window: bool = resolved_parameters.get("next_polling_window", False)
    list_limit: int | None = resolved_parameters.get("list_limit", None)
    now = utc_now()
    discovered_resources = context.discovered_resources(step)

    # We may hold up execution waiting for the next polling window
    if next_polling_window:
        delay_seconds = calculate_wait_next_polling_window(now, discovered_resources)
        await context.progress.add_log(step, f"Delaying {delay_seconds}s until next polling window.")
        await asyncio.sleep(delay_seconds)

    # Start making requests for resources
    for resource in context.resource_tree.discover_resource_plan([CSIPAusResource(r) for r in resources]):
        await discover_resource(resource, step, context, list_limit)

    return ActionResult.done()
