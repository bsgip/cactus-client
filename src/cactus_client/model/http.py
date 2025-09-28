from dataclasses import dataclass, field
from datetime import datetime

from aiohttp import ClientResponse
from multidict import CIMultiDict

from cactus_client.schema.validator import validate_xml
from cactus_client.time import utc_now


@dataclass
class ServerRequest:
    url: str  # The HTTP url that was resolved
    method: str  # Was this a GET/PUT/POST etc?
    body: str | None  # The raw request body sent (if any)
    headers: dict[str, str]

    created_at: datetime = field(default_factory=utc_now, init=False)


@dataclass
class ServerResponse:
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
