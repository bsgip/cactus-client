import re
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlparse

from multidict import CIMultiDict

from cactus_client.model.context import ExecutionContext
from cactus_client.model.http import NotificationRequest, ServerResponse
from cactus_client.model.output import RunOutputFile, RunOutputManager
from cactus_client.model.resource import StoredResourceId


def sanitise_url_to_filename(url: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", url[1:].split("?")[0])


def generate_request_file(
    method: str, url: str, host: str | None, headers: dict[str, str] | CIMultiDict, body: str | None
) -> list[str]:

    lines = [f"{method} {url} HTTP/1.1"]
    if host:
        lines.append(f"Host: {host}")

    for header, header_val in headers.items():
        lines.append(f"{header}: {header_val}")

    if body:
        lines.append("")
        lines.append(body)

    return lines


def generate_response_file(status: int, headers: dict[str, str] | CIMultiDict, body: str | None) -> list[str]:
    lines = [f"HTTP/1.1 {status} {HTTPStatus(status).name}"]
    for header, header_val in headers.items():
        lines.append(f"{header}: {header_val}")

    if body:
        lines.append("")
        lines.append(body)

    return lines


def persist_server_response(base_dir: Path, idx: int, host: str, response: ServerResponse) -> None:
    # This is the traditional request/response
    request = response.request
    sanitised_url = sanitise_url_to_filename(request.url)
    request_file = base_dir / f"{idx:03}-{request.method}-{sanitised_url}.request"
    response_file = base_dir / f"{idx:03}-{request.method}-{sanitised_url}.response"
    with open(request_file, "w") as fp:
        fp.write("\n".join(generate_request_file(request.method, request.url, host, request.headers, request.body)))
    with open(response_file, "w") as fp:
        fp.write("\n".join(generate_response_file(response.status, response.headers, response.body)))


def persist_notification(
    base_dir: Path, idx: int, webhook_endpoint: str | None, notification: NotificationRequest
) -> None:

    if webhook_endpoint:
        parsed_url = urlparse(webhook_endpoint)
        path = parsed_url.path
    else:
        path = ""
    notification_file = base_dir / f"{idx:03}-NOTIFICATION-{notification.sub_id}.request"

    with open(notification_file, "w") as fp:
        fp.write(
            "\n".join(generate_request_file(notification.method, path, None, notification.headers, notification.body))
        )


def persist_all_request_data(context: ExecutionContext, output_manager: RunOutputManager) -> None:
    """Writes all requests/responses into the output manager for the current run"""

    base_dir = output_manager.file_path(RunOutputFile.RequestsDirectory)
    base_dir.mkdir()

    # There are probably multiple subscription aliases and associated webhook URIs
    # parse them into an easily accessible form
    webhook_by_sub_id: dict[tuple[str, StoredResourceId], str] = {}
    for client in context.clients_by_alias.values():
        if client.notifications:
            for sub_id, endpoints in client.notifications.endpoints_by_sub_alias.items():
                for endpoint in endpoints:
                    webhook_by_sub_id[(sub_id, endpoint.subscribed_resource_id)] = (
                        endpoint.created_endpoint.fully_qualified_endpoint
                    )

    for idx, comms in enumerate(context.responses.responses):

        # We don't have EVERYTHING logged - so we try and reconstitute as much as possible
        if isinstance(comms, ServerResponse):
            # This is a traditional HTTP request/response to the utility server
            host = urlparse(context.server_config.device_capability_uri).netloc
            persist_server_response(base_dir, idx, host, comms)
        else:
            # This is a request that landed at our webhook (typically a pub/sub Notification)
            persist_notification(
                base_dir, idx, webhook_by_sub_id.get((comms.sub_id, comms.source.subscribed_resource_id), None), comms
            )
