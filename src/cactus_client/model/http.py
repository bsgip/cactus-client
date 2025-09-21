from dataclasses import dataclass
from datetime import datetime

from aiohttp import ClientResponse


@dataclass
class ServerResponse:
    url: str  # The HTTP url that was resolved
    method: str  # Was this a GET/PUT/POST etc?
    status: int  # What was returned from the server?
    body: str | None  # The raw body response (assumed to be a string based)
    location: str | None  # The value of the Location header (if any)

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
        return ServerResponse(
            url=response.request_info.url,
            method=response.request_info.method,
            status=response.status,
            body=body_bytes.decode(response.get_encoding()),
            location=location,
            received_at=received_at,
            requested_at=requested_at,
        )
