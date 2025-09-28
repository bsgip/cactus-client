from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional, TypeVar, cast

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
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
)
from envoy_schema.server.schema.sep2.pub_sub import (
    Subscription,
    SubscriptionListResponse,
)
from envoy_schema.server.schema.sep2.time import TimeResponse
from treelib import Tree

from cactus_client.time import utc_now

AnyType = TypeVar("AnyType")


RESOURCE_SEP2_TYPES: dict[CSIPAusResource, type[Resource]] = {
    CSIPAusResource.DeviceCapability: DeviceCapabilityResponse,
    CSIPAusResource.Time: TimeResponse,
    CSIPAusResource.MirrorUsagePointList: MirrorUsagePointListResponse,
    CSIPAusResource.EndDeviceList: EndDeviceListResponse,
    CSIPAusResource.MirrorUsagePoint: MirrorUsagePoint,
    CSIPAusResource.EndDevice: EndDeviceResponse,
    CSIPAusResource.SubscriptionList: SubscriptionListResponse,
    CSIPAusResource.Subscription: Subscription,
    CSIPAusResource.ConnectionPoint: ConnectionPointResponse,
    CSIPAusResource.Registration: RegistrationResponse,
    CSIPAusResource.FunctionSetAssignmentsList: FunctionSetAssignmentsListResponse,
    CSIPAusResource.FunctionSetAssignments: FunctionSetAssignmentsResponse,
    CSIPAusResource.DERProgramList: DERProgramListResponse,
    CSIPAusResource.DERProgram: DERProgramResponse,
    CSIPAusResource.DefaultDERControl: DefaultDERControl,
    CSIPAusResource.DERControlList: DERControlListResponse,
    CSIPAusResource.DERControl: DERControlResponse,
    CSIPAusResource.DERList: DERListResponse,
    CSIPAusResource.DER: DER,
    CSIPAusResource.DERCapability: DERCapability,
    CSIPAusResource.DERSettings: DERSettings,
    CSIPAusResource.DERStatus: DERStatus,
}


class CSIPAusResourceTree:
    """Represents CSIPAus Resources as a hierarchy"""

    tree: Tree

    def __init__(self) -> None:
        self.tree = Tree()
        self.tree.create_node(identifier=CSIPAusResource.DeviceCapability, parent=None)
        self.tree.create_node(identifier=CSIPAusResource.Time, parent=CSIPAusResource.DeviceCapability)
        self.tree.create_node(identifier=CSIPAusResource.MirrorUsagePointList, parent=CSIPAusResource.DeviceCapability)
        self.tree.create_node(identifier=CSIPAusResource.EndDeviceList, parent=CSIPAusResource.DeviceCapability)
        self.tree.create_node(identifier=CSIPAusResource.MirrorUsagePoint, parent=CSIPAusResource.MirrorUsagePointList)
        self.tree.create_node(identifier=CSIPAusResource.EndDevice, parent=CSIPAusResource.EndDeviceList)
        self.tree.create_node(identifier=CSIPAusResource.ConnectionPoint, parent=CSIPAusResource.EndDevice)
        self.tree.create_node(identifier=CSIPAusResource.Registration, parent=CSIPAusResource.EndDevice)
        self.tree.create_node(identifier=CSIPAusResource.SubscriptionList, parent=CSIPAusResource.EndDevice)
        self.tree.create_node(identifier=CSIPAusResource.Subscription, parent=CSIPAusResource.SubscriptionList)
        self.tree.create_node(identifier=CSIPAusResource.FunctionSetAssignmentsList, parent=CSIPAusResource.EndDevice)
        self.tree.create_node(
            identifier=CSIPAusResource.FunctionSetAssignments, parent=CSIPAusResource.FunctionSetAssignmentsList
        )
        self.tree.create_node(identifier=CSIPAusResource.DERProgramList, parent=CSIPAusResource.FunctionSetAssignments)
        self.tree.create_node(identifier=CSIPAusResource.DERProgram, parent=CSIPAusResource.DERProgramList)
        self.tree.create_node(identifier=CSIPAusResource.DefaultDERControl, parent=CSIPAusResource.DERProgram)
        self.tree.create_node(identifier=CSIPAusResource.DERControlList, parent=CSIPAusResource.DERProgram)
        self.tree.create_node(identifier=CSIPAusResource.DERControl, parent=CSIPAusResource.DERControlList)
        self.tree.create_node(identifier=CSIPAusResource.DERList, parent=CSIPAusResource.EndDevice)
        self.tree.create_node(identifier=CSIPAusResource.DER, parent=CSIPAusResource.DERList)
        self.tree.create_node(identifier=CSIPAusResource.DERCapability, parent=CSIPAusResource.DER)
        self.tree.create_node(identifier=CSIPAusResource.DERSettings, parent=CSIPAusResource.DER)
        self.tree.create_node(identifier=CSIPAusResource.DERStatus, parent=CSIPAusResource.DER)

    def discover_resource_plan(self, target_resources: list[CSIPAusResource]) -> list[CSIPAusResource]:
        """Given a list of resource targets - calculate the ordered sequence of requests required
        to "walk" the tree such that all target_resources are hit (and nothing is double fetched)"""

        visit_order: list[CSIPAusResource] = []
        visited_nodes: set[CSIPAusResource] = set()
        for target in target_resources:
            for step in reversed(list(self.tree.rsearch(target))):
                if step in visited_nodes:
                    continue
                visited_nodes.add(step)
                visit_order.append(step)

        return visit_order

    def parent_resource(self, target: CSIPAusResource) -> CSIPAusResource | None:
        """Find the (immediate) parent resource for a specific target resource (or None if this is the root)"""
        return self.tree.ancestor(target)  # type: ignore


@dataclass(frozen=True, eq=True)
class StoredResource:
    created_at: datetime  # When did this resource get created/stored
    resource_type: CSIPAusResource
    parent: Optional["StoredResource"]  # The parent of this resource (at the time of discovery)
    resource_link_hrefs: dict[
        CSIPAusResource, str
    ]  # hrefs from Link.href values found in this resource, keyed by the resource type they point to.
    member_of_list: CSIPAusResource | None  # If specified - this resource is a member of a List of this type

    resource: Resource  # The common 2030.5 Resource that is being stored. List items "may" have some children populated
    alias: str | None = field(
        compare=False
    )  # Can be set by the test definition marking specific resources - is NOT used in equality checks.

    def __hash__(self) -> int:
        return hash(
            (
                self.created_at,
                self.resource_type,
                self.parent,
                tuple(self.resource_link_hrefs.items()),
                self.member_of_list,
                id(self.resource),
                # We are deliberately NOT including alias in the hash
            )
        )

    @staticmethod
    def from_resource(
        tree: CSIPAusResourceTree,
        type: CSIPAusResource,
        parent: Optional["StoredResource"],
        resource: Resource,
        alias: str | None,
    ) -> "StoredResource":
        parent_type = tree.parent_resource(type)
        if parent_type and is_list_resource(parent_type):
            member_of_list = parent_type
        else:
            member_of_list = None
        return StoredResource(
            created_at=utc_now(),
            resource_type=type,
            parent=parent,
            resource=resource,
            resource_link_hrefs=generate_resource_link_hrefs(type, resource),
            member_of_list=member_of_list,
            alias=alias,
        )


class ResourceStore:
    """Top level "database" of CSIP Aus resources that have been seen by the client"""

    store: dict[CSIPAusResource, list[StoredResource]]
    tree: CSIPAusResourceTree

    def __init__(self, tree: CSIPAusResourceTree) -> None:
        self.store = {}
        self.tree = tree

    def clear(self) -> None:
        """Fully resets this store to its initial state"""
        self.store.clear()

    def clear_resource(self, type: CSIPAusResource) -> None:
        """Updates the store so that future calls to get (for type) will return an empty list."""
        if type in self.store:
            del self.store[type]

    def set_resource(
        self, type: CSIPAusResource, parent: StoredResource | None, resource: Resource, alias: str | None = None
    ) -> StoredResource:
        """Updates the store so that future calls to get (for type) will return ONLY resource. Any existing resources
        of this type will be deleted. Alias can be used to mark this resource for future identification (is not used in
        comparisons).

        Returns the StoredResource that was inserted."""
        new_resource = StoredResource.from_resource(self.tree, type, parent, resource, alias)
        self.store[type] = [new_resource]
        return new_resource

    def append_resource(
        self, type: CSIPAusResource, parent: StoredResource | None, resource: Resource, alias: str | None = None
    ) -> StoredResource:
        """Updates the store so that future calls to get (for type) will return their current value(s) PLUS this new
        value. Alias can be used to mark this resource for future identification (is not used in comparisons).

        Returns the StoredResource that was inserted"""
        new_resource = StoredResource.from_resource(self.tree, type, parent, resource, alias)
        existing = self.store.get(type, None)
        if existing is None:
            self.store[type] = [new_resource]
        else:
            existing.append(new_resource)

        return new_resource

    def upsert_resource(
        self, type: CSIPAusResource, parent: StoredResource | None, resource: Resource, alias: str | None = None
    ) -> StoredResource:
        """Similar to append_resource but if a resource with the same href+parent already exists, it will be
        replaced. Alias can be used to mark this resource for future identification (is not used in comparisons)."""
        new_resource = StoredResource.from_resource(self.tree, type, parent, resource, alias)
        existing = self.store.get(type, None)
        if existing is None:
            self.store[type] = [new_resource]
            return new_resource

        # Look for a conflict - replacing it if found
        for idx, potential_match in enumerate(existing):
            if potential_match.parent == parent and potential_match.resource.href == resource.href:
                existing[idx] = new_resource
                return new_resource

        # Otherwise just append
        existing.append(new_resource)
        return new_resource

    def get(self, type: CSIPAusResource) -> list[StoredResource]:
        """Finds all StoredResources of the specified resource type. Returns empty list if none are found"""
        return self.store.get(type, [])

    def get_descendents_of(self, type: CSIPAusResource, parent: StoredResource) -> list[StoredResource]:
        """Finds all StoredResources of the specified resource type that ALSO list parent in the their chain of parents
        (at any level). Returns empty list if none are found."""
        matches: list[StoredResource] = []

        for potential_match in self.get(type):
            visited_parents: set[StoredResource] = {potential_match}  # Stop infinite loops
            current_ancestor = potential_match.parent
            while current_ancestor is not None:
                if current_ancestor in visited_parents:
                    break  # No match - we've looped back around somehow (this is bad)
                if current_ancestor is parent:
                    matches.append(potential_match)
                    break  # We found a match - stop walking the parents

                visited_parents.add(current_ancestor)
                current_ancestor = current_ancestor.parent  # Keep searching up the parents

        return matches


def get_link_href(link: Link | None) -> str | None:
    """Convenience function to reduce boilerplate - returns the href (if available) or None"""
    if link is None:
        return None
    return link.href


def resource_link_hrefs_from_links(links: Iterable[tuple[CSIPAusResource, Link | None]]) -> dict[CSIPAusResource, str]:
    """Convenience function to reduce boilerplate - Returns a dict where ONLY the populated hrefs are included"""
    return dict(((type, link.href) for type, link in links if link and link.href))


def generate_resource_link_hrefs(type: CSIPAusResource, resource: Resource) -> dict[CSIPAusResource, str]:
    """Given a raw XML resource and its type - extract all the subordinate Link resources found in that resource. Any
    optional / missing Links will NOT be encoded."""
    match (type):
        case CSIPAusResource.DeviceCapability:
            dcap = cast(DeviceCapabilityResponse, resource)
            return resource_link_hrefs_from_links(
                [
                    (CSIPAusResource.Time, dcap.TimeLink),
                    (CSIPAusResource.EndDeviceList, dcap.EndDeviceListLink),
                    (CSIPAusResource.MirrorUsagePointList, dcap.MirrorUsagePointListLink),
                ]
            )
        case CSIPAusResource.EndDevice:
            edev = cast(EndDeviceResponse, resource)
            return resource_link_hrefs_from_links(
                [
                    (CSIPAusResource.ConnectionPoint, edev.ConnectionPointLink),
                    (CSIPAusResource.Registration, edev.RegistrationLink),
                    (CSIPAusResource.FunctionSetAssignmentsList, edev.FunctionSetAssignmentsListLink),
                    (CSIPAusResource.DERList, edev.DERListLink),
                    (CSIPAusResource.SubscriptionList, edev.SubscriptionListLink),
                ]
            )
        case CSIPAusResource.FunctionSetAssignments:
            fsa = cast(FunctionSetAssignmentsResponse, resource)
            return resource_link_hrefs_from_links(
                [
                    (CSIPAusResource.DERProgramList, fsa.DERProgramListLink),
                ]
            )
        case CSIPAusResource.DERProgram:
            derp = cast(DERProgramResponse, resource)
            return resource_link_hrefs_from_links(
                [
                    (CSIPAusResource.DefaultDERControl, derp.DefaultDERControlLink),
                    (CSIPAusResource.DERControlList, derp.DERControlListLink),
                ]
            )
        case CSIPAusResource.DER:
            der = cast(DER, resource)
            return resource_link_hrefs_from_links(
                [
                    (CSIPAusResource.DERCapability, der.DERCapabilityLink),
                    (CSIPAusResource.DERSettings, der.DERSettingsLink),
                    (CSIPAusResource.DERStatus, der.DERStatusLink),
                ]
            )
        case _:
            return {}  # This will match any type that doesn't have subordinate Link resources
