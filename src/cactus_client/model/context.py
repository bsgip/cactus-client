from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.time import utc_now


@dataclass(frozen=True)
class StoredResource:
    created_at: datetime  # When did this resource get created/stored
    type: CSIPAusResource
    parent: Optional["StoredResource"]  # The parent of this resource (at the time of discovery)
    resource: Resource  # The common 2030.5 Resource that is being stored. List items "may" have some children populated


@dataclass(frozen=True)
class ResourceStore:
    """Top level "database" of CSIP Aus resources that have been seen by the client"""

    store: dict[CSIPAusResource, list[StoredResource]] = field(default_factory=dict, init=False)

    def clear(self) -> None:
        """Fully resets this store to its initial state"""
        self.store.clear()

    def set_single(self, type: CSIPAusResource, parent: StoredResource | None, resource: Resource) -> StoredResource:
        """Updates the store so that future calls to get (for type) will return ONLY resource. Any existing resources
        of this type will be deleted.

        Returns the StoredResource that was inserted."""
        new_resource = StoredResource(utc_now(), type, parent, resource)
        self.store[type] = [new_resource]
        return new_resource

    def set_many(
        self, type: CSIPAusResource, parent: StoredResource | None, resources: list[Resource]
    ) -> list[StoredResource]:
        """Updates the store so that future calls to get (for type) will return ONLY resources. Any existing resources
        of this type will be deleted.

        Returns the StoredResources that were inserted."""
        now = utc_now()
        new_resources = [StoredResource(now, type, parent, r) for r in resources]
        self.store[type] = new_resources
        return new_resources

    def get(self, type: CSIPAusResource) -> list[StoredResource]:
        """Finds all StoredResources of the specified resource type. Returns empty list if none are found"""
        return self.store.get(type, [])


@dataclass
class WarningStore:
    """A warning represents some form of (minor) failure of a test that doesn't block the execution but should be
    reported at the end. Example warnings could include a non critical XSD error."""

    warnings: list[str]

    def __init__(self) -> None:
        self.warnings = []

    def log_resource_warning(self, type: CSIPAusResource, message: str) -> None:
        self.warnings.append(f"{type}: {message}")


@dataclass
class Context:
    """This represents the snapshot of the client's 'memory' that has been built up over interactions with the
    server."""

    resources: ResourceStore
    warnings: WarningStore
