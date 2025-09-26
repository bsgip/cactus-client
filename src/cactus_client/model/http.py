from dataclasses import dataclass
from datetime import datetime

from aiohttp import ClientResponse

from cactus_client.schema.validator import validate_xml


@dataclass
class ServerResponse:
    url: str  # The HTTP url that was resolved
    method: str  # Was this a GET/PUT/POST etc?
    status: int  # What was returned from the server?
    body: str  # The raw body response (assumed to be a string based)
    location: str | None  # The value of the Location header (if any)
    content_type: str | None  # The value of the Content-Type header (if any)
    xsd_errors: list[str] | None  # Any XSD errors that were detected

    requested_at: datetime
    received_at: datetime

    def is_success(self) -> bool:
        return self.status >= 200 and self.status < 300

    def is_client_error(self) -> bool:
        return self.status >= 400 and self.status < 500

    @staticmethod
    async def from_response(
        response: ClientResponse, requested_at: datetime, received_at: datetime
    ) -> "ServerResponse":
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
            content_type=content_type,
            received_at=received_at,
            requested_at=requested_at,
            xsd_errors=xsd_errors,
        )
