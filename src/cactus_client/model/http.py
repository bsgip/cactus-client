from dataclasses import dataclass, field
from datetime import datetime

from aiohttp import ClientResponse
from cactus_client_notifications.schema import (
    CollectedNotification,
    CreateEndpointResponse,
)
from cactus_test_definitions.csipaus import CSIPAusResource
from multidict import CIMultiDict

from cactus_client.model.resource import (
    StoredResourceId,
)
from cactus_client.schema.validator import validate_xml
from cactus_client.time import utc_now


@dataclass(frozen=True)
class NotificationEndpoint:
    """Metadata about a single notification endpoint"""

    created_endpoint: CreateEndpointResponse  # Raw metadata from the cactus-client-notifications instance
    subscribed_resource_type: CSIPAusResource  # The resource type of the subscribed resource
    subscribed_resource_id: StoredResourceId  # The StoredResource.id that this subscription is for


@dataclass
class SubscriptionNotification:
    """Combination of a collected Notification and the source endpoint that collected the notification"""

    notification: CollectedNotification  # Details on the notification
    source: NotificationEndpoint  # Which endpoint generated this notification


@dataclass
class ServerRequest:
    """Represents a request to the utility server"""

    url: str  # The HTTP url that was resolved
    method: str  # Was this a GET/PUT/POST etc?
    body: str | None  # The raw request body sent (if any)
    headers: dict[str, str]

    created_at: datetime = field(default_factory=utc_now, init=False)


@dataclass
class ServerResponse:
    """Represents a response from the utility server in response to a particular request"""

    url: str  # The HTTP url that was resolved
    method: str  # Was this a GET/PUT/POST etc?
    status: int  # What was returned from the server?
    body: str  # The raw body response (assumed to be a string based)
    location: str | None  # The value of the Location header (if any)
    content_type: str | None  # The value of the Content-Type header (if any)
    xsd_errors: list[str] | None  # Any XSD errors that were detected
    headers: CIMultiDict  # headers received

    request: ServerRequest  # The request that generated this response

    created_at: datetime = field(default_factory=utc_now, init=False)

    def is_success(self) -> bool:
        return self.status >= 200 and self.status < 300

    def is_client_error(self) -> bool:
        return self.status >= 400 and self.status < 500

    @staticmethod
    async def from_response(response: ClientResponse, request: ServerRequest) -> "ServerResponse":
        body_bytes = await response.read()
        location = response.headers.get("Location", None)
        content_type = response.headers.get("Content-Type", None)
        body_xml = body_bytes.decode(response.get_encoding())

        xsd_errors = None
        if body_xml:
            xsd_errors = validate_xml(body_xml)

        return ServerResponse(
            url=str(response.request_info.url),
            method=response.request_info.method,
            status=response.status,
            body=body_xml,
            location=location,
            headers=response.headers.copy(),
            content_type=content_type,
            xsd_errors=xsd_errors,
            request=request,
        )


@dataclass
class NotificationRequest:
    """Represents a request from the utility server to a webhook"""

    method: str  # Was this a GET/PUT/POST etc?
    body: str  # The raw body response (assumed to be a string based)
    content_type: str | None  # The value of the Content-Type header (if any)
    xsd_errors: list[str] | None  # Any XSD errors that were detected
    headers: CIMultiDict  # headers received
    received_at: datetime  # When did this arrive at the notification webhook
    remote: str | None  # What IP address (or network address) sent this request?
    sub_id: str  # What subscription ID was this Notification sent to?
    source: NotificationEndpoint
    created_at: datetime = field(default_factory=utc_now, init=False)

    @staticmethod
    def from_collected_notification(
        source: NotificationEndpoint, notification: CollectedNotification, sub_id: str
    ) -> "NotificationRequest":
        body_xml = notification.body
        headers = CIMultiDict(((h.name, h.value) for h in notification.headers))
        content_type = headers.getone("Content-Type", None)

        xsd_errors = None
        if body_xml:
            xsd_errors = validate_xml(body_xml)

        return NotificationRequest(
            method=notification.method,
            body=body_xml,
            headers=headers,
            content_type=content_type,
            xsd_errors=xsd_errors,
            received_at=notification.received_at,
            remote=notification.remote,
            sub_id=sub_id,
            source=source,
        )
