import logging
from dataclasses import dataclass
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
    Notification,
    Subscription,
    SubscriptionListResponse,
)
from envoy_schema.server.schema.sep2.time import TimeResponse
from treelib import Tree

from cactus_client.error import CactusClientException
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)

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
    CSIPAusResource.Notification: Notification,  # Not in the resource tree
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
class StoredResourceId:
    """Represents a unique ID for a single Resource that's based on the chain of parent hrefs and the href for this
    node"""

    hrefs: tuple[str, ...]

    def href(self) -> str:
        """Fetches the href associated with this specific ID"""
        return self.hrefs[0]

    def parent_id(self) -> Optional["StoredResourceId"]:
        """Generates a new ResourceId that's equivalent to the parent that created this ResourceId"""
        if len(self.hrefs) == 1:
            return None
        return StoredResourceId(hrefs=self.hrefs[1:])

    def is_descendent_of(self, ancestor: "StoredResourceId") -> bool:
        """Returns True if this StoredResource is a descendent of the supplied ancestor. Self is NOT a descendent of
        itself."""
        ancestor_depth = len(ancestor.hrefs)
        self_depth = len(self.hrefs)

        if self_depth <= ancestor_depth:
            return False

        # If the self ID "endswith" ancestor's ID - then it's descended from it
        return self.hrefs[-ancestor_depth:] == ancestor.hrefs

    def is_ancestor_of(self, descendent: "StoredResourceId") -> bool:
        """Returns True if this StoredResource is an ancestor of the supplied descendent. Self is NOT a ancestor of
        itself."""
        descendent_depth = len(descendent.hrefs)
        self_depth = len(self.hrefs)

        if self_depth >= descendent_depth:
            return False

        # If the descendent ID "endswith" self's ID - then it's descended from it
        return self.hrefs == descendent.hrefs[-self_depth:]

    @staticmethod
    def from_parent(parent: Optional["StoredResourceId"], href: str) -> "StoredResourceId":
        """Creates a new descendent ResourceId with the specified href"""
        if parent is None:
            return StoredResourceId(hrefs=(href,))
        else:
            return StoredResourceId(hrefs=tuple((href, *parent.hrefs)))


@dataclass(frozen=True)
class StoredResource:
    id: StoredResourceId  # Uniquely identifies this resource based on what parents discovered it
    created_at: datetime  # When did this resource get created/stored
    resource_type: CSIPAusResource
    resource_link_hrefs: dict[
        CSIPAusResource, str
    ]  # hrefs from Link.href values found in this resource, keyed by the resource type they point to.
    member_of_list: CSIPAusResource | None  # If specified - this resource is a member of a List of this type
    resource: Resource  # The common 2030.5 Resource that is being stored. List items "may" have some children populated

    @staticmethod
    def from_resource(
        tree: CSIPAusResourceTree,
        resource_type: CSIPAusResource,
        parent: StoredResourceId | None,
        resource: Resource,
    ) -> "StoredResource":

        parent_type = tree.parent_resource(resource_type)
        if parent_type and is_list_resource(parent_type):
            member_of_list = parent_type
        else:
            member_of_list = None

        if not resource.href:
            raise CactusClientException(f"Received a {resource_type} under {parent} with no href.")

        return StoredResource(
            id=StoredResourceId.from_parent(parent, resource.href),
            created_at=utc_now(),
            resource_type=resource_type,
            resource=resource,
            resource_link_hrefs=generate_resource_link_hrefs(resource_type, resource),
            member_of_list=member_of_list,
        )


class ResourceStore:
    """Top level "database" of CSIP Aus resources that have been seen by the client"""

    resource_store: dict[CSIPAusResource, list[StoredResource]]
    id_store: dict[StoredResourceId, StoredResource]
    tree: CSIPAusResourceTree

    def __init__(self, tree: CSIPAusResourceTree) -> None:
        self.resource_store = {}
        self.id_store = {}
        self.tree = tree

    def clear(self) -> None:
        """Fully resets this store to its initial state"""
        self.resource_store.clear()
        self.id_store.clear()

    def clear_resource(self, type: CSIPAusResource) -> None:
        """Updates the store so that future calls to get (for type) will return an empty list. Also unlinks ALL
        of the ID entries that are removed"""
        existing_srs = self.resource_store.get(type)
        if existing_srs is not None:
            for sr in existing_srs:
                del self.id_store[sr.id]
            del self.resource_store[type]

    def append_resource(
        self, type: CSIPAusResource, parent: StoredResourceId | None, resource: Resource
    ) -> StoredResource:
        """Updates the store so that future calls to get (for type) will return their current value(s) PLUS this new
        value.

        raises a CactusClientException if resource is missing a href
        raises a CactusClientException if a resource with the same unique ID is already stored.

        Returns the StoredResource that was inserted"""
        new_resource = StoredResource.from_resource(self.tree, type, parent, resource)

        duplicate = self.id_store.get(new_resource.id, None)
        if duplicate is not None:
            raise CactusClientException(f"Resource store already has {type} {new_resource.id}. Cannot append a copy.")
        self.id_store[new_resource.id] = new_resource

        existing_resources_of_type = self.resource_store.get(type, None)
        if existing_resources_of_type is None:
            self.resource_store[type] = [new_resource]
        else:
            existing_resources_of_type.append(new_resource)

        return new_resource

    def upsert_resource(
        self, type: CSIPAusResource, parent: StoredResourceId | None, resource: Resource
    ) -> StoredResource:
        """Similar to append_resource but if a resource with the same href+parent already exists, it will be
        replaced.

        raises a CactusClientException if resource is missing a href"""

        new_resource = StoredResource.from_resource(self.tree, type, parent, resource)

        # Update ID store
        self.id_store[new_resource.id] = new_resource

        # Update resource store
        existing_resources_of_type = self.resource_store.get(type, None)
        if existing_resources_of_type is None:
            self.resource_store[type] = [new_resource]
            return new_resource

        # Look for a conflict - replacing it if found
        for idx, potential_match in enumerate(existing_resources_of_type):
            if potential_match.id == new_resource.id:
                existing_resources_of_type[idx] = new_resource
                return new_resource

        # Otherwise just append
        existing_resources_of_type.append(new_resource)
        return new_resource

    def delete_resource(self, id: StoredResourceId) -> StoredResource | None:
        """Removes a specific resource from this store. Returns item if deleted, None otherwise.

        This will NOT unlink any descendents of this resource."""
        deleted_item = self.id_store.pop(id, None)

        # Also remove from the resource list
        if deleted_item is not None:
            resource_list = self.resource_store.get(deleted_item.resource_type, None)
            if resource_list is not None:
                try:
                    resource_list.remove(deleted_item)
                except ValueError:
                    raise CactusClientException(
                        f"Couldn't find {id} in the {deleted_item.resource_type} store. This is a bug with the tests."
                    )

        return deleted_item

    def get_for_id(self, id: StoredResourceId) -> StoredResource | None:
        """Fetches a specific StoredResource by ID. Returns None if it DNE"""
        return self.id_store.get(id, None)

    def get_for_type(self, type: CSIPAusResource) -> list[StoredResource]:
        """Finds all StoredResources of the specified resource type. Returns empty list if none are found"""
        return self.resource_store.get(type, [])

    def get_descendents_of(self, type: CSIPAusResource, parent: StoredResourceId) -> list[StoredResource]:
        """Finds all StoredResources of the specified resource type that ALSO list parent in the their chain of parents
        (at any level). Returns empty list if none are found."""

        return [sr for sr in self.get_for_type(type) if sr.id.is_descendent_of(parent)]

    def get_ancestor_of(self, target_type: CSIPAusResource, child_id: StoredResourceId) -> StoredResource | None:
        """Walks up the parent chain to find an ancestor of the specified type."""
        current_id: StoredResourceId | None = child_id.parent_id()
        while current_id is not None:
            current = self.id_store.get(current_id, None)
            if current is None:
                # We didn't find it - it's possible the ID's at this level were cleared and never re-inserted
                # Either way - we can continue searching up the chain
                current_id = current_id.parent_id()
                continue

            if current.resource_type == target_type:
                return current
            current_id = current_id.parent_id()
        return None


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
