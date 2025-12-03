from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum, StrEnum
from pathlib import Path

from aiohttp import ClientSession
from cactus_client_notifications.schema import (
    CreateEndpointResponse,
)
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
    TestProcedureId,
)

from cactus_client.error import NotificationException
from cactus_client.model.config import ClientConfig, ServerConfig
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.http import NotificationEndpoint
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import (
    CSIPAusResourceTree,
    ResourceStore,
    StoredResourceId,
)
from cactus_client.time import utc_now


class AnnotationNamespace(StrEnum):
    # Resource has been received via a Subscription Notification
    # tag values will reference the test definition sub_id
    SUBSCRIPTION_RECEIVED = "From Subscription"

    # Resource has had a type of Response sent for it.
    # tag values will reference the response type being sent
    RESPONSES = "Response Replies"


@dataclass
class StoredResourceAnnotations:
    """Annotations are extra metadata that is assigned by the cactus-client to a specific resource. It's usually
    for tracking state / other details associated with a specific resource and are invariant to changes/updates in that
    resource."""

    alias: str | None = None
    tag_creations: dict[tuple[AnnotationNamespace, str | StrEnum | IntEnum], datetime] = field(
        default_factory=dict
    )  # Tags with a value of the created_at

    def add_tag(self, namespace: AnnotationNamespace, value: str | StrEnum | IntEnum) -> None:
        """Adds a tag to the store - if it already exists, has no effect"""
        tag = (namespace, value)
        if tag not in self.tag_creations:
            self.tag_creations[tag] = utc_now()

    def has_tag(self, namespace: AnnotationNamespace, value: str | StrEnum | IntEnum) -> bool:
        """Returns True if the specified tag has been added via add_tag"""
        tag = (namespace, value)
        return tag in self.tag_creations


@dataclass(frozen=True)
class NotificationsContext:
    """Represents the current state of a client's subscription/notification webhooks"""

    # Used for making HTTP requests to the cactus-client-notifications instance
    # will have base_url, timeouts, ssl_context set
    session: ClientSession

    endpoints_by_sub_alias: dict[
        str, list[NotificationEndpoint]
    ]  # notification server endpoints, keyed by the subscription alias that they corresponds to

    def get_resource_notification_endpoint(
        self, sub_id: str, subscribed_resource_id: StoredResourceId
    ) -> NotificationEndpoint | None:
        """Convenience function for accessing endpoints_by_sub_alias and looking for a NotificationEndpoint that
        exists under sub_id AND has the specified subscribed_resource_id"""
        endpoints = self.endpoints_by_sub_alias.get(sub_id, None)
        if not endpoints:
            return None

        for e in endpoints:
            if e.subscribed_resource_id == subscribed_resource_id:
                return e
        return None

    def add_resource_notification_endpoint(
        self,
        sub_id: str,
        created_endpoint: CreateEndpointResponse,
        subscribed_resource_type: CSIPAusResource,
        subscribed_resource_id: StoredResourceId,
    ) -> NotificationEndpoint:
        """Creates a new instance of NotificationEndpoint (with the supplied values) and appends it to the internal
        endpoints_by_sub_alias structure. Performs no duplication checks"""
        new_endpoint = NotificationEndpoint(
            created_endpoint=created_endpoint,
            subscribed_resource_type=subscribed_resource_type,
            subscribed_resource_id=subscribed_resource_id,
        )

        endpoints = self.endpoints_by_sub_alias.get(sub_id, None)
        if not endpoints:
            self.endpoints_by_sub_alias[sub_id] = [new_endpoint]
        else:
            endpoints.append(new_endpoint)

        return new_endpoint


@dataclass
class ClientContext:
    """This represents the snapshot of the client's 'memory' that has been built up over interactions with the
    server."""

    test_procedure_alias: str  # What will the test procedure YAML be referring to this context as?
    client_config: ClientConfig
    discovered_resources: ResourceStore
    annotations: dict[StoredResourceId, StoredResourceAnnotations]
    session: ClientSession  # Used for making HTTP requests - will have base_url, timeouts, ssl_context set
    notifications: (
        NotificationsContext | None
    )  # For handling requests to the cactus-client-notifications instance or None if not configured


@dataclass
class ExecutionContext:
    """Represents all state/config required for a test run execution"""

    test_procedure_id: TestProcedureId
    test_procedure: TestProcedure  # The test procedure being run
    test_procedures_version: str

    output_directory: Path  # The root output directory for any outputs from this test
    dcap_path: str  # The URI path component of the device_capability_uri
    server_config: ServerConfig  # The server config used to generate this context - purely informational
    clients_by_alias: dict[str, ClientContext]  # The Clients in use for this test, keyed by their test procedure alias
    steps: StepExecutionList
    warnings: WarningTracker
    progress: ProgressTracker
    responses: ResponseTracker
    resource_tree: CSIPAusResourceTree

    repeat_delay: timedelta = timedelta(
        seconds=5
    )  # If during execution an action is to be run in a tight loop, use this delay
    created_at: datetime = field(default_factory=utc_now, init=False)

    def client_config(self, step: StepExecution) -> ClientConfig:
        """Convenience function for accessing the ClientConfig for a specific step (based on client alias)"""
        return self.clients_by_alias[step.client_alias].client_config

    def session(self, step: StepExecution) -> ClientSession:
        """Convenience function for accessing the ClientSession for a specific step (based on client alias)"""
        return self.clients_by_alias[step.client_alias].session

    def discovered_resources(self, step: StepExecution) -> ResourceStore:
        """Convenience function for accessing the ResourceStore for a specific step (based on client alias)"""
        return self.clients_by_alias[step.client_resources_alias].discovered_resources

    def resource_annotations(self, step: StepExecution, stored_resource: StoredResourceId) -> StoredResourceAnnotations:
        """Convenience function for accessing the StoredResourceAnnotation for a specific step's resource. If no
        annotations currently exist, a default (empty) annotations will be created, stored and then returned"""
        client_annotations = self.clients_by_alias[step.client_alias].annotations
        annotations = client_annotations.get(stored_resource, None)
        if annotations is not None:
            return annotations
        else:
            annotations = StoredResourceAnnotations()
            client_annotations[stored_resource] = annotations
            return annotations

    def notifications_context(self, step: StepExecution) -> NotificationsContext:
        """Convenience function for accessing the NotificationsContext for a specific step (based on client alias)

        Can raise NotificationException if a notification uri isn't configured."""
        client = self.clients_by_alias[step.client_resources_alias]
        if client.notifications is None:
            raise NotificationException(
                f"No NotificationContext for client {step.client_resources_alias}."
                + " Has a notification_uri been configured?"
            )
        return client.notifications
