import re
from http import HTTPStatus
from urllib.parse import urlparse

from cactus_client.model.context import ExecutionContext
from cactus_client.model.output import RunOutputFile, RunOutputManager


def sanitise_url_to_filename(url: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", url[1:].split("?")[0])


def persist_all_request_data(context: ExecutionContext, output_manager: RunOutputManager) -> None:
    """Writes all requests/responses into the output manager for the current run"""

    base_dir = output_manager.file_path(RunOutputFile.RequestsDirectory)
    base_dir.mkdir()

    for idx, response in enumerate(context.responses.responses):
        request = response.request
        sanitised_url = sanitise_url_to_filename(request.url)
        request_file = base_dir / f"{idx:03}-{sanitised_url}.request"
        response_file = base_dir / f"{idx:03}-{sanitised_url}.response"

        # We don't have EVERYTHING logged - so we try and reconstitute as much as possible
        host = urlparse(context.server_config.device_capability_uri).netloc
        with open(request_file, "w") as fp:
            lines = [f"{request.method} {request.url} HTTP/1.1", f"Host: {host}"]

            for header, header_val in request.headers.items():
                lines.append(f"{header}: {header_val}")

            if request.body:
                lines.append("")
                lines.append(request.body)

            fp.write("\n".join(lines))

        with open(response_file, "w") as fp:
            lines = [f"HTTP/1.1 {response.status} {HTTPStatus(response.status).name}"]
            for header, header_val in response.headers.items():
                lines.append(f"{header}: {header_val}")

            if response.body:
                lines.append("")
                lines.append(response.body)

            fp.write("\n".join(lines))
