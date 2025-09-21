import asyncio
from datetime import datetime
from typing import Any, cast

from aiohttp import ClientSession
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from treelib import Tree

from cactus_client.model.context import ExecutionContext, ResourceStore
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.time import utc_now


def get_resource_tree() -> Tree:
    """Returns the tree of CSIPAusResource relationships with DeviceCapability forming the root"""

    tree = Tree()
    tree.create_node(identifier=CSIPAusResource.DeviceCapability, parent=None)
    tree.create_node(identifier=CSIPAusResource.Time, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePointList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.EndDeviceList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePoint, parent=CSIPAusResource.MirrorUsagePointList)
    tree.create_node(identifier=CSIPAusResource.MirrorMeterReadingList, parent=CSIPAusResource.MirrorUsagePoint)
    tree.create_node(identifier=CSIPAusResource.MirrorMeterReading, parent=CSIPAusResource.MirrorMeterReadingList)
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


async def fetch_dcap(dcap_path: str, session: ClientSession, discovered_resources: ResourceStore) -> None:
    async with session.get(dcap_path) as response:
        response.status


async def discover_resource(
    resource: CSIPAusResource, dcap_path: str, session: ClientSession, discovered_resources: ResourceStore
) -> None:
    match resource:
        case CSIPAusResource.DeviceCapability:
            raise NotImplementedError()
        case CSIPAusResource.EndDeviceList:
            for dcap in discovered_resources.get(CSIPAusResource.DeviceCapability):
                cast(DeviceCapabilityResponse, dcap.resource).EndDeviceListLink.href
                raise NotImplementedError()


async def action_discovery(
    session: ClientSession, resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    resources: list[CSIPAusResource] = resolved_parameters["resources"]  # Mandatory param
    next_polling_window: bool = resolved_parameters.get("next_polling_window", False)
    now = utc_now()
    discovered_resources = context.discovered_resources(step)

    # We may hold up execution waiting for the next polling window
    if next_polling_window:
        delay_seconds = calculate_wait_next_polling_window(now, discovered_resources)
        context.progress.log_step_progress(step, f"Delaying {delay_seconds}s until next polling window.")
        await asyncio.sleep(delay_seconds)

    # Start making requests for resources
    for resource in discover_resource_plan(RESOURCE_TREE, resources):
        await session.get(context.dcap_path)

    return ActionResult.done()


# "discovery": {
#         "resources": ParameterSchema(True, ParameterType.ListCSIPAusResource),  # What resources to try and resolve?
#         "next_polling_window": ParameterSchema(
#             False, ParameterType.Boolean
#         ),  # If set - delay this until the upcoming polling window (eg- wait for the next whole minute)
#     }

RESOURCE_TREE = get_resource_tree()  # This shouldn't be changing during execution
