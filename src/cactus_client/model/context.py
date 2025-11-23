from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from aiohttp import ClientSession
from cactus_client_notifications.schema import CreateEndpointResponse
from cactus_test_definitions.csipaus import CSIPAusResource
from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
    TestProcedureId,
)

from cactus_client.error import NotificationException
from cactus_client.model.config import ClientConfig, ServerConfig
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import CSIPAusResourceTree, ResourceStore
from cactus_client.time import utc_now


@dataclass(frozen=True)
class NotificationEndpoint:
    """Metadata about a single notification endpoint"""

    created_endpoint: CreateEndpointResponse  # Raw metadata from the cactus-client-notifications instance
    subscribed_resource_href: str  # The Subscription.subscribedResource for this endpoint
    subscribed_resource: CSIPAusResource  # The type of the subscribedResource for this endpoint


@dataclass(frozen=True)
class NotificationsContext:
    """Represents the current state of a client's subscription/notification webhooks"""

    # Used for making HTTP requests to the cactus-client-notifications instance
    # will have base_url, timeouts, ssl_context set
    session: ClientSession

    endpoint_by_sub_alias: dict[
        str, NotificationEndpoint
    ]  # notification server endpoint, keyed by the subscription alias that it corresponds to


@dataclass
class ClientContext:
    """This represents the snapshot of the client's 'memory' that has been built up over interactions with the
    server."""

    test_procedure_alias: str  # What will the test procedure YAML be referring to this context as?
    client_config: ClientConfig
    discovered_resources: ResourceStore
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
