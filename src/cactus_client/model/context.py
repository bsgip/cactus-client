from dataclasses import dataclass, field
from datetime import datetime
from ssl import SSLContext
from typing import Optional

from aiohttp import ClientSession
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import TestProcedure
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.model.config import ClientConfig
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
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
class ClientContext:
    """This represents the snapshot of the client's 'memory' that has been built up over interactions with the
    server."""

    test_procedure_alias: str  # What will the test procedure YAML be referring to this context as?
    client_config: ClientConfig
    discovered_resources: ResourceStore
    session: ClientSession  # Used for making HTTP requests - will have base_url, timeouts, ssl_context set


@dataclass
class ExecutionContext:
    """Represents all state/config required for a test run execution"""

    test_procedure: TestProcedure  # The test procedure being run
    dcap_path: str  # The URI path component of the device_capability_uri
    clients_by_alias: dict[str, ClientContext]  # The Clients in use for this test, keyed by their test procedure alias
    steps: StepExecutionList
    warnings: WarningTracker
    progress: ProgressTracker
    responses: ResponseTracker

    def session(self, step: StepExecution) -> ClientSession:
        """Convenience function for accessing the ClientSession for a specific step (based on client alias)"""
        return self.clients_by_alias[step.client_alias].session

    def discovered_resources(self, step: StepExecution) -> ResourceStore:
        """Convenience function for accessing the ResourceStore for a specific step (based on client alias)"""
        return self.clients_by_alias[step.client_resources_alias].discovered_resources
