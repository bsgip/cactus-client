import ssl
import time
from dataclasses import dataclass

from aiohttp import (
    ClientConnectorError,
    ClientSession,
    ClientSSLError,
    ClientTimeout,
    TCPConnector,
)

from cactus_client.error import ConfigError
from cactus_client.execution.build import build_client_ssl_context, build_dcap_parts
from cactus_client.model.config import ClientConfig, ServerConfig

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class ConnectCheckResult:
    """The outcome of attempting to connect to the server's DeviceCapability endpoint as a particular client"""

    client_id: str
    success: bool
    status_code: int | None  # Set if a HTTP response was received
    elapsed_seconds: float
    summary: str  # Short human readable outcome (eg "HTTP 200" or "SSL Certificate Error")
    detail: str | None = None  # Extended diagnostics - only populated on failure


async def check_client_connection(
    server: ServerConfig, client: ClientConfig, timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
) -> ConnectCheckResult:
    """Attempts to fetch the server's DeviceCapability resource while authenticated as client. Never raises -
    all failures (config, TLS, connection, timeout, HTTP) are captured in the returned ConnectCheckResult."""

    started = time.monotonic()

    def failure(summary: str, detail: str | None, status_code: int | None = None) -> ConnectCheckResult:
        return ConnectCheckResult(
            client_id=client.id,
            success=False,
            status_code=status_code,
            elapsed_seconds=time.monotonic() - started,
            summary=summary,
            detail=detail,
        )

    try:
        base_uri, dcap_path = build_dcap_parts(server)
    except ConfigError as exc:
        return failure("Invalid server configuration", str(exc))

    try:
        ssl_context = build_client_ssl_context(
            client, server.verify_ssl, server.verify_host_name, server.serca_pem_file
        )
    except ConfigError as exc:
        return failure("Invalid client certificate/key", str(exc))

    try:
        async with ClientSession(base_url=base_uri, connector=TCPConnector(ssl=ssl_context)) as session:
            async with session.get(dcap_path, timeout=ClientTimeout(total=timeout_seconds)) as response:
                await response.read()
                if response.status >= 300:
                    return failure(
                        f"HTTP {response.status}",
                        f"GET {base_uri.rstrip('/')}{dcap_path} returned HTTP {response.status} ({response.reason}).",
                        status_code=response.status,
                    )
                return ConnectCheckResult(
                    client_id=client.id,
                    success=True,
                    status_code=response.status,
                    elapsed_seconds=time.monotonic() - started,
                    summary=f"HTTP {response.status}",
                )
    except (ClientSSLError, ssl.SSLError) as exc:
        # Covers certificate verification failures and general TLS/handshake errors - aiohttp's more specific
        # SSL exceptions (eg ClientConnectorCertificateError) all derive from ClientSSLError.
        return failure("TLS/SSL error", str(exc))
    except ClientConnectorError as exc:
        return failure("Connection failed", str(exc))
    except TimeoutError:
        return failure("Timed out", f"No response received within {timeout_seconds}s.")
    except Exception as exc:
        return failure("Unexpected error", f"{type(exc).__name__}: {exc}")
