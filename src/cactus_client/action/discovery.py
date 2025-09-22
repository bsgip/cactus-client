import asyncio
from datetime import datetime
from typing import Any, Callable, cast

from aiohttp import ClientSession
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.csip_aus.connection_point import ConnectionPointResponse
from envoy_schema.server.schema.sep2.der import (
    DER,
    DefaultDERControl,
    DERCapability,
    DERControlListResponse,
    DERControlResponse,
    DERListResponse,
    DERProgramListResponse,
    DERProgramResponse,
    DERSettings,
    DERStatus,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
    RegistrationResponse,
)
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
    FunctionSetAssignmentsResponse,
)
from envoy_schema.server.schema.sep2.identification import Link, Resource
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorMeterReading,
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
)
from envoy_schema.server.schema.sep2.time import TimeResponse
from treelib import Tree

from cactus_client.action.server import (
    get_resource_for_step,
    paginate_list_resource_items,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext, ResourceStore, StoredResource
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.time import utc_now

DISCOVERY_LIST_PAGE_SIZE = 3  # We want something suitably small (to ensure pagination is tested)


def get_resource_tree() -> Tree:
    """Returns the tree of CSIPAusResource parent/child relationships. DeviceCapability is forming the root"""

    tree = Tree()
    tree.create_node(identifier=CSIPAusResource.DeviceCapability, parent=None)
    tree.create_node(identifier=CSIPAusResource.Time, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePointList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.EndDeviceList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePoint, parent=CSIPAusResource.MirrorUsagePointList)
    tree.create_node(identifier=CSIPAusResource.EndDevice, parent=CSIPAusResource.EndDeviceList)
    tree.create_node(identifier=CSIPAusResource.ConnectionPoint, parent=CSIPAusResource.EndDevice)
    tree.create_node(identifier=CSIPAusResource.Registration, parent=CSIPAusResource.EndDevice)
    tree.create_node(identifier=CSIPAusResource.FunctionSetAssignmentsList, parent=CSIPAusResource.EndDevice)
    tree.create_node(
        identifier=CSIPAusResource.FunctionSetAssignments, parent=CSIPAusResource.FunctionSetAssignmentsList
    )
    tree.create_node(identifier=CSIPAusResource.DERProgramList, parent=CSIPAusResource.FunctionSetAssignments)
    tree.create_node(identifier=CSIPAusResource.DERProgram, parent=CSIPAusResource.DERProgramList)
    tree.create_node(identifier=CSIPAusResource.DefaultDERControl, parent=CSIPAusResource.DERProgram)
    tree.create_node(identifier=CSIPAusResource.DERControlList, parent=CSIPAusResource.DERProgram)
    tree.create_node(identifier=CSIPAusResource.DERControl, parent=CSIPAusResource.DERControlList)
    tree.create_node(identifier=CSIPAusResource.DERList, parent=CSIPAusResource.EndDevice)
    tree.create_node(identifier=CSIPAusResource.DER, parent=CSIPAusResource.DERList)
    tree.create_node(identifier=CSIPAusResource.DERCapability, parent=CSIPAusResource.DER)
    tree.create_node(identifier=CSIPAusResource.DERSettings, parent=CSIPAusResource.DER)
    tree.create_node(identifier=CSIPAusResource.DERStatus, parent=CSIPAusResource.DER)

    return tree


def discover_resource_plan(tree: Tree, target_resources: list[CSIPAusResource]) -> list[CSIPAusResource]:
    """Given a list of resource targets and their hierarchy - calculate the ordered sequence of requests required
    to "walk" the tree such that all target_resources are hit (and nothing is double fetched)"""

    visit_order: list[CSIPAusResource] = []
    visited_nodes: set[CSIPAusResource] = set()
    for target in target_resources:
        for step in reversed(list(tree.rsearch(target))):
            if step in visited_nodes:
                continue
            visited_nodes.add(step)
            visit_order.append(step)

    return visit_order


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


def get_link_href(link: Link | None) -> str | None:
    """Convenience function to reduce boilerplate - returns the href (if available) or None"""
    if link is None:
        return None
    return link.href


async def do_discovery_singular(
    target_resource: CSIPAusResource,
    target_type: type[Resource],
    parent_resource: CSIPAusResource,
    get_href: Callable[[StoredResource], str | None],
    step: StepExecution,
    context: ExecutionContext,
) -> None:
    """Enumerates all parent_resources - extracts all child hrefs for target and then makes requests for those target.

    This is for requesting resources that return a SINGLE instance per request"""
    resource_store = context.discovered_resources(step)
    resource_store.clear_resource(target_resource)

    # For every parent resource - make a request for a single entity of target_type
    for sr, href in resource_store.get_resource_hrefs(parent_resource, get_href):
        target_entity = await get_resource_for_step(target_type, step, context, href)
        resource_store.append_resource(target_resource, sr, target_entity)


async def do_discovery_list_items(
    target_resource: CSIPAusResource,
    list_resource: CSIPAusResource,
    list_type: type[Resource],
    get_list_href: Callable[[StoredResource], str | None],
    get_list_items: Callable[[Resource], list[Any] | None],
    step: StepExecution,
    context: ExecutionContext,
) -> None:
    """Enumerates all parent_resources - extracts all child hrefs for target and then makes requests for those targets.

    This is for requesting resources that site underneath a list type"""
    resource_store = context.discovered_resources(step)
    resource_store.clear_resource(target_resource)

    # Find EVERY parent list that exists (there might be multiple)
    for sr, href in resource_store.get_resource_hrefs(list_resource, get_list_href):

        # Paginate through each of the lists - each of those items are the things we want to store
        list_items = await paginate_list_resource_items(
            list_type, step, context, href, DISCOVERY_LIST_PAGE_SIZE, get_list_items
        )
        for item in list_items:
            resource_store.append_resource(target_resource, sr, item)


async def discover_resource(resource: CSIPAusResource, step: StepExecution, context: ExecutionContext) -> None:
    """Performs discovery for the particular resource - it is assumed that all parent resources have been previously
    fetched."""
    match resource:
        case CSIPAusResource.DeviceCapability:
            context.discovered_resources(step).set_resource(
                resource,
                None,
                await get_resource_for_step(DeviceCapabilityResponse, step, context, context.dcap_path),
            )

        case CSIPAusResource.Time:
            await do_discovery_singular(
                target_resource=resource,
                target_type=TimeResponse,
                parent_resource=CSIPAusResource.DeviceCapability,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DeviceCapabilityResponse, sr.resource).TimeLink),
            )

        case CSIPAusResource.MirrorUsagePointList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=MirrorUsagePointListResponse,
                parent_resource=CSIPAusResource.DeviceCapability,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DeviceCapabilityResponse, sr.resource).MirrorUsagePointListLink),
            )

        case CSIPAusResource.MirrorUsagePoint:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.MirrorUsagePointList,
                list_type=MirrorUsagePointListResponse,
                get_list_href=lambda sr: cast(MirrorUsagePointListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(MirrorUsagePointListResponse, list_).mirrorUsagePoints,
                step=step,
                context=context,
            )

        case CSIPAusResource.EndDeviceList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=EndDeviceListResponse,
                parent_resource=CSIPAusResource.DeviceCapability,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DeviceCapabilityResponse, sr.resource).EndDeviceListLink),
            )

        case CSIPAusResource.EndDevice:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.EndDeviceList,
                list_type=EndDeviceListResponse,
                get_list_href=lambda sr: cast(EndDeviceListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(EndDeviceListResponse, list_).EndDevice,
                step=step,
                context=context,
            )

        case CSIPAusResource.ConnectionPoint:
            await do_discovery_singular(
                target_resource=resource,
                target_type=ConnectionPointResponse,
                parent_resource=CSIPAusResource.EndDevice,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(EndDeviceResponse, sr.resource).ConnectionPointLink),
            )

        case CSIPAusResource.Registration:
            await do_discovery_singular(
                target_resource=resource,
                target_type=RegistrationResponse,
                parent_resource=CSIPAusResource.EndDevice,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(EndDeviceResponse, sr.resource).RegistrationLink),
            )

        case CSIPAusResource.FunctionSetAssignmentsList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=FunctionSetAssignmentsListResponse,
                parent_resource=CSIPAusResource.EndDevice,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(EndDeviceResponse, sr.resource).FunctionSetAssignmentsListLink),
            )

        case CSIPAusResource.FunctionSetAssignments:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.FunctionSetAssignmentsList,
                list_type=FunctionSetAssignmentsListResponse,
                get_list_href=lambda sr: cast(FunctionSetAssignmentsListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(FunctionSetAssignmentsListResponse, list_).FunctionSetAssignments,
                step=step,
                context=context,
            )

        case CSIPAusResource.DERProgramList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERProgramResponse,
                parent_resource=CSIPAusResource.FunctionSetAssignments,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(FunctionSetAssignmentsResponse, sr.resource).DERProgramListLink),
            )

        case CSIPAusResource.DERProgram:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.DERProgramList,
                list_type=DERProgramListResponse,
                get_list_href=lambda sr: cast(DERProgramListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(DERProgramListResponse, list_).DERProgram,
                step=step,
                context=context,
            )

        case CSIPAusResource.DefaultDERControl:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DefaultDERControl,
                parent_resource=CSIPAusResource.DERProgram,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DERProgramResponse, sr.resource).DefaultDERControlLink),
            )

        case CSIPAusResource.DefaultDERControl:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DefaultDERControl,
                parent_resource=CSIPAusResource.DERProgram,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DERProgramResponse, sr.resource).DefaultDERControlLink),
            )

        case CSIPAusResource.DERControlList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERControlListResponse,
                parent_resource=CSIPAusResource.DERProgram,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DERProgramResponse, sr.resource).DERControlListLink),
            )

        case CSIPAusResource.DERControl:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.DERControlList,
                list_type=DERControlListResponse,
                get_list_href=lambda sr: cast(DERControlListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(DERControlListResponse, list_).DERControl,
                step=step,
                context=context,
            )

        case CSIPAusResource.DERList:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERListResponse,
                parent_resource=CSIPAusResource.EndDevice,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(EndDeviceResponse, sr.resource).DERListLink),
            )

        case CSIPAusResource.DER:
            await do_discovery_list_items(
                target_resource=resource,
                list_resource=CSIPAusResource.DERList,
                list_type=DERListResponse,
                get_list_href=lambda sr: cast(DERListResponse, sr.resource).href,
                get_list_items=lambda list_: cast(DERListResponse, list_).DER_,
                step=step,
                context=context,
            )

        case CSIPAusResource.DERCapability:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERCapability,
                parent_resource=CSIPAusResource.DER,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DER, sr.resource).DERCapabilityLink),
            )

        case CSIPAusResource.DERSettings:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERSettings,
                parent_resource=CSIPAusResource.DER,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DER, sr.resource).DERSettingsLink),
            )

        case CSIPAusResource.DERStatus:
            await do_discovery_singular(
                target_resource=resource,
                target_type=DERStatus,
                parent_resource=CSIPAusResource.DER,
                step=step,
                context=context,
                get_href=lambda sr: get_link_href(cast(DER, sr.resource).DERStatusLink),
            )

        case _:
            raise CactusClientException(f"Resource {resource} is not supported in this version of cactus.")


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
        await context.progress.log_step_progress(step, f"Delaying {delay_seconds}s until next polling window.")
        await asyncio.sleep(delay_seconds)

    # Start making requests for resources
    for resource in discover_resource_plan(RESOURCE_TREE, resources):
        await discover_resource(resource, step, context)

    return ActionResult.done()


RESOURCE_TREE = get_resource_tree()  # This shouldn't be changing during execution
