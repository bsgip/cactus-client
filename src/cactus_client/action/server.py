import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from http import HTTPMethod, HTTPStatus
from typing import TypeVar

from cactus_test_definitions import CSIPAusVersion
from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.identification import List, Resource, SubscribableList

from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION, CSIPAUS_WELL_KNOWN_PATH, MIME_TYPE_JSON, MIME_TYPE_SEP2
from cactus_client.error import CactusClientError, RequestError
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.http import ServerResponse
from cactus_client.sep2 import get_property_changes

RATE_LIMIT_RETRY_DELAYS = (5, 15, 30)  # seconds to wait between retries on 429

logger = logging.getLogger(__name__)

AnyResourceType = TypeVar("AnyResourceType", bound=Resource)
AnyListType = TypeVar("AnyListType", bound=List | SubscribableList)
AnyType = TypeVar("AnyType")


@dataclass(frozen=True, slots=True)
class AcceptVersion:
    csip_aus_version: str  # eg v1.3 (not a full CSIP-AUS version XMLNS)
    quality_factor: float | None

    def to_accept_header(self) -> str:
        if self.quality_factor is None:
            return f"{MIME_TYPE_SEP2};csipaus={self.csip_aus_version}"
        else:
            return f"{MIME_TYPE_SEP2};csipaus={self.csip_aus_version};q={self.quality_factor:.3f}".rstrip("0").rstrip(
                "."
            )


DEFAULT_ACCEPT_VERSIONS = [AcceptVersion(csip_aus_version=str(ACTIVE_CSIP_AUS_VERSION), quality_factor=None)]


def resource_to_sep2_xml(resource: Resource) -> str:
    xml = resource.to_xml(skip_empty=False, exclude_none=True, exclude_unset=True)
    if xml is None:
        return ""
    if isinstance(xml, bytes):
        return xml.decode()
    return xml


def accept_versions_to_accept_header(versions: list[AcceptVersion]) -> str:
    """Given a list of AcceptVersion - turn it into a representative Accept: header value"""
    return ", ".join(v.to_accept_header() for v in versions)


async def _single_request(
    step: StepExecution,
    context: ExecutionContext,
    path: str,
    method: HTTPMethod,
    headers: dict,
    sep2_xml_body: str | None,
    xml_validation_version: CSIPAusVersion | None,
) -> ServerResponse:
    """Makes a single request and returns the response."""
    session = context.session(step)
    server_request = await context.responses.set_active_request(method, path, body=sep2_xml_body, headers=headers)
    async with session.request(method=method, url=path, data=sep2_xml_body, headers=headers) as raw_response:
        try:
            response = await ServerResponse.from_response(
                raw_response, request=server_request, xml_validation_version=xml_validation_version
            )
        except Exception as exc:
            logger.error(f"Caught exception attempting to {method} {path}", exc_info=exc)
            await context.responses.clear_active_request()
            raise RequestError(f"Caught exception attempting to {method} {path}: {exc}") from exc

        await context.responses.log_response_body(response, step.client_alias, strict=context.run_config.strict)
        await context.responses.clear_active_request()
        return response


async def request_for_step(
    step: StepExecution,
    context: ExecutionContext,
    path: str,
    method: HTTPMethod,
    sep2_xml_body: str | None = None,
    accept_versions: list[AcceptVersion] | None = None,
    xml_validation_version: CSIPAusVersion = ACTIVE_CSIP_AUS_VERSION,
) -> ServerResponse:
    """Makes a request to the CSIP-Aus server (for the current context) and endpoint - returns a raw parsed response and
    logs the actions in the various context trackers. Raises a RequestError on connection failure.

    Retries up to 3 times on 429 responses with delays of 5, 15, 30 seconds."""
    await context.progress.add_log(step, f"Requesting {method} {path}")

    # The Accept header is dynamic but never empty
    if accept_versions is None:
        accept_header_value = accept_versions_to_accept_header(DEFAULT_ACCEPT_VERSIONS)
    elif len(accept_versions) == 0:
        accept_header_value = MIME_TYPE_SEP2
    else:
        accept_header_value = accept_versions_to_accept_header(accept_versions)

    # Start assembling request headers
    headers = {"Accept": accept_header_value}
    if sep2_xml_body is not None:
        headers["Content-Type"] = MIME_TYPE_SEP2

    user_agent = context.client_config(step).user_agent
    if user_agent:
        headers["User-Agent"] = user_agent

    response = await _single_request(
        step, context, path, method, headers, sep2_xml_body, xml_validation_version=xml_validation_version
    )

    for delay in RATE_LIMIT_RETRY_DELAYS:
        if response.status != HTTPStatus.TOO_MANY_REQUESTS:
            break
        await context.progress.add_log(step, f"Rate limited (429), retrying in {delay}s")
        await asyncio.sleep(delay)
        response = await _single_request(
            step, context, path, method, headers, sep2_xml_body, xml_validation_version=xml_validation_version
        )

    return response


def parse_type_response(t: type[AnyResourceType], response: ServerResponse) -> AnyResourceType:
    href = response.request.url
    try:
        # bytes, as lxml only accepts an <?xml encoding=...?> declaration on bytes input
        return t.from_xml(response.body.encode())
    except Exception as exc:
        logger.error(
            f"Caught exception attempting to parse {len(response.body)} chars from {href}",
            exc_info=exc,
        )
        logger.error(response.body)
        raise RequestError(f"Caught exception parsing {len(response.body)} chars from {href}: {exc}") from exc


def parse_error_response(
    step: StepExecution, context: ExecutionContext, response: ServerResponse
) -> ErrorResponse | None:
    """Attempts to parse an ErrorResponse from a 4xx response body. Returns None if the body is empty or cannot
    be parsed. CSIP-Aus only mandates an ErrorResponse body for ConnectionPoint requests - for every other request
    a server is free to return an error with no body, so a missing body is not logged as a warning. A non-empty
    body that fails to parse is still logged, as that indicates a malformed (rather than absent) error payload."""
    if not response.body:
        return None

    try:
        # bytes, as lxml only accepts an <?xml encoding=...?> declaration on bytes input
        return ErrorResponse.from_xml(response.body.encode())
    except Exception as exc:
        context.warnings.log_step_warning(
            step,
            f"Could not parse ErrorResponse from {len(response.body)} chars at {response.request.url}: {exc}. "
            "Skipping error body validation.",
        )
        return None


async def request_well_known_for_step(
    step: StepExecution,
    context: ExecutionContext,
) -> str:
    """Accesses the CSIP-AUS .well-known file for the server - returns the raw JSON string response"""
    response = await _single_request(
        step,
        context,
        CSIPAUS_WELL_KNOWN_PATH,
        HTTPMethod.GET,
        {"Accept": MIME_TYPE_JSON},
        sep2_xml_body=None,
        xml_validation_version=None,
    )

    if not response.is_success():
        raise RequestError(
            f"Received status {response.status} but expected 2XX requesting"
            + f" {response.method} {CSIPAUS_WELL_KNOWN_PATH}."
        )

    return response.body


async def client_error_request_for_step(
    step: StepExecution,
    context: ExecutionContext,
    path: str,
    method: HTTPMethod,
    sep2_xml_body: str | None = None,
    accept_versions: list[AcceptVersion] | None = None,
) -> ErrorResponse | None:
    """Similar to request_for_step but is only successful if the resulting response is a 4xx.

    Returns None if the response body could not be parsed as a valid ErrorResponse XML."""
    response = await request_for_step(step, context, path, method, sep2_xml_body, accept_versions=accept_versions)

    if not response.is_client_error():
        raise RequestError(f"Received status {response.status} but expected 4XX requesting {response.method} {path}.")

    return parse_error_response(step, context, response)


async def client_error_or_empty_list_request_for_step(
    t: type[AnyListType],
    step: StepExecution,
    context: ExecutionContext,
    path: str,
    method: HTTPMethod,
    sep2_xml_body: str | None = None,
    accept_versions: list[AcceptVersion] | None = None,
) -> ErrorResponse | AnyListType | None:
    """Similar to client_error_request_for_step but also allows a successful response if it's an empty sep2 List.

    Raises a RequestError if the response is neither a 4xx nor an empty List.
    Returns None if the error response body could not be parsed as a valid ErrorResponse XML."""

    # Fire off the request and if it fails, handle the error response.
    response = await request_for_step(step, context, path, method, sep2_xml_body, accept_versions=accept_versions)
    if response.is_client_error():
        return parse_error_response(step, context, response)

    if not response.is_success():
        raise RequestError(f"Received status {response.status} (expected 4XX) requesting {response.method} {path}.")

    # At this point - we're assuming we've got a List response
    list_data = parse_type_response(t, response)
    if list_data.all_ is None or list_data.results is None:
        context.warnings.log_step_warning(
            step,
            f"{method} {path} yielded a List that's missing either the 'all' or 'results' attribute.",
        )
    elif list_data.all_ != 0 or list_data.results != 0:
        raise RequestError(
            f"{method} {path} should've returned an error or empty List but instead got a List with "
            f"'all'={list_data.all_} 'results'={list_data.results}"
        )
    return list_data


async def get_resource_for_step(
    t: type[AnyResourceType],
    step: StepExecution,
    context: ExecutionContext,
    href: str,
    accept_versions: list[AcceptVersion] | None = None,
) -> AnyResourceType:
    """Makes a GET request for a particular href and parses the resulting XML into an expected type (t). Raises a
    RequestError if the connection fails, returns an error or fails to parse to t"""
    # Make the raw request
    response = await request_for_step(step, context, href, HTTPMethod.GET, accept_versions=accept_versions)

    if not response.is_success():
        raise RequestError(f"Received status {response.status} requesting {response.method} {href}.")

    return parse_type_response(t, response)


async def delete_and_check_resource_for_step(step: StepExecution, context: ExecutionContext, href: str) -> None:
    """Makes a DELETE request for a particular href and then performs a followup GET expecting a 404/401/403 error.

    Raises a RequestError if the connection fails, returns an error or succeeds on the refetch."""
    # Make the delete request
    delete_response = await request_for_step(step, context, href, HTTPMethod.DELETE)
    if not delete_response.is_success():
        raise RequestError(f"Received status {delete_response.status} requesting {delete_response.method} {href}.")

    # There might be a refetch delay
    refetch_delay_ms = (
        context.run_config.refetch_delay_ms
        if context.run_config.refetch_delay_ms is not None
        else context.server_config.refetch_delay_ms
    )
    if refetch_delay_ms:
        delay_seconds = refetch_delay_ms / 1000
        await context.progress.add_log(
            step,
            f"Delaying re-fetch for {delay_seconds}s (as per server configuration).",
        )
        await asyncio.sleep(delay_seconds)

    # Try and refetch it - it should now be "deleted" so we'll accept a few different error statuses as a pass
    refetch_response = await request_for_step(step, context, href, HTTPMethod.GET)
    if refetch_response.status not in {
        HTTPStatus.NOT_FOUND,
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.FORBIDDEN,
    }:
        raise RequestError(f"Refetching {href} yielded {delete_response.status}. Expected it to be deleted.")


async def submit_and_refetch_resource_for_step(
    t: type[AnyResourceType],
    step: StepExecution,
    context: ExecutionContext,
    method: HTTPMethod,
    href: str,
    submitted_resource: AnyResourceType,
    no_location_header: bool = False,
    strict_fields: set[str] | None = None,
) -> AnyResourceType:
    """Makes a method request to a particular href, submitting submitted_resource and expecting a success response. Then
    parse the resulting response for a Location header and then GET that URI, returning the resulting resource.

    if no_location_header is set - the initial response will not be checked for a Location header and instead href
    will be used as the GET (use this for when updating a resource insitu, not creating a new resource)

    Any differences between submitted and returned resource will be logged as warnings, unless they are listed in
    strict_fields, in which case a CactusClientError will be raised."""

    # Make the submit request
    response = await request_for_step(
        step,
        context,
        href,
        method,
        sep2_xml_body=resource_to_sep2_xml(submitted_resource),
    )
    if not response.is_success():
        raise RequestError(f"Received status {response.status} requesting {response.method} {href}.")

    if no_location_header:
        refetch_href = href
    else:
        if not response.location:
            raise RequestError(
                f"{response.status} response from {response.method} {href} did not return an expected 'Location' header"
            )
        refetch_href = response.location

    # There might be a refetch delay
    refetch_delay_ms = (
        context.run_config.refetch_delay_ms
        if context.run_config.refetch_delay_ms is not None
        else context.server_config.refetch_delay_ms
    )
    if refetch_delay_ms:
        delay_seconds = refetch_delay_ms / 1000
        await context.progress.add_log(
            step,
            f"Delaying re-fetch for {delay_seconds}s (as per server configuration).",
        )
        await asyncio.sleep(delay_seconds)

    # Check the returned resource matches the submitted resource
    returned_resource = await get_resource_for_step(t, step, context, refetch_href)
    changes = get_property_changes(submitted_resource, returned_resource)
    strict_changes = changes & strict_fields if strict_fields else set()
    lenient_changes = changes - strict_changes

    def describe(keys: set[str]) -> str:
        return ", ".join(
            f"{k} had {getattr(submitted_resource, k)} changed to {getattr(returned_resource, k, None)}"
            for k in sorted(keys)
        )

    if lenient_changes:
        context.warnings.log_step_warning(
            step,
            f"GET {refetch_href} didn't match the values submitted to {method} {href}: {describe(lenient_changes)}",
        )
    if strict_changes:
        raise CactusClientError(
            f"GET {refetch_href} didn't match the values submitted to {method} {href}: {describe(strict_changes)}"
        )

    return returned_resource


def build_paging_params(
    start: int | None = None,
    limit: int | None = None,
    changed_after: datetime | None = None,
) -> str:
    """Builds up a sep2 paging query string in the form of ?s={start}&l={limit}&a={changed_after}.
    None params will not be included in the query string"""

    parts: list[str] = []
    if start is not None:
        parts.append(f"s={start}")
    if limit is not None:
        parts.append(f"l={limit}")
    if changed_after is not None:
        parts.append(f"a={int(changed_after.timestamp())}")

    return "?" + "&".join(parts)


async def paginate_list_resource_items(
    list_type: type[AnyResourceType],
    step: StepExecution,
    context: ExecutionContext,
    list_href: str,
    page_size: int,
    item_callback: Callable[[AnyResourceType], list[AnyType] | None],
    max_pages_requested: int = 20,
) -> list[AnyType]:
    """Helper function for paginating through an entire list object (eg EndDeviceList) over multiple requests and
    returning the resulting child items (eg EndDevice) as a single list.

    list_type: The type to parse the responses as (eg EndDeviceList)
    step: The step this request is being made for
    context: The execution context that this request is being made under
    list_href: The href to the list (no query params included). Eg /sep2/edev
    page_size: How many items to request on each page
    item_callback: Will be called on each page object to extract the items
    max_pages_requested: A safety valve to prevent infinite pagination
    """
    pages_requested = 0
    start = 0
    every_all_value: list[int] = []
    all_items: list[AnyType] = []

    while True:
        latest_items, received_all = await fetch_list_page(
            list_type, step, context, list_href, start, page_size, item_callback
        )
        all_items.extend(latest_items)

        if received_all is not None:
            every_all_value.append(received_all)

        # Prepare next page
        # This is deliberately over paginating in order to catch any odd server behaviour
        start += page_size
        if len(latest_items) == 0:  # When we receive an empty page - we know we are done
            break

        # Safety valve in case a server misbehaves and keeps sending us more data
        pages_requested += 1
        if pages_requested >= max_pages_requested:
            raise RequestError(
                f"Paginating {list_href} exceeded max pages {max_pages_requested} at page size {page_size}."
            )

    # Final check of the all attributes
    expected_count = 0 if len(every_all_value) == 0 else every_all_value[0]
    if len(set(every_all_value)) > 1:
        context.warnings.log_step_warning(
            step,
            f"The 'all' attribute at {list_href} has varied while paginating through. This is likely an error.",
        )
    elif expected_count != len(all_items):
        context.warnings.log_step_warning(
            step,
            f"The 'all' attribute at {list_href} indicated {expected_count} items but {len(all_items)} items returned.",
        )

    return all_items


async def fetch_list_page(
    list_type: type[AnyResourceType],
    step: StepExecution,
    context: ExecutionContext,
    list_href: str,
    start: int,
    limit: int,
    item_callback: Callable[[AnyResourceType], list[AnyType] | None],
) -> tuple[list[AnyType], int | None]:
    """
    Fetch a single page of a list resource and extract items with validation.

    Returns:
        tuple of (items, all_attribute)
    """
    page_href = list_href + build_paging_params(start=start, limit=limit)
    latest_list = await get_resource_for_step(list_type, step, context, page_href)
    latest_items = item_callback(latest_list)
    if latest_items is None:
        latest_items = []  # pydantic-xml can parse a missing/empty list as None

    # Extract and validate metadata
    received_all: int | None = getattr(latest_list, "all_", None)
    received_results: int | None = getattr(latest_list, "results", None)

    if received_results is None:
        context.warnings.log_step_warning(step, f"Missing 'results' attribute at {page_href}")
    elif received_results != len(latest_items):
        context.warnings.log_step_warning(
            step,
            f"'results' attribute shows {received_results} but got {len(latest_items)} items",
        )

    if received_all is None:
        context.warnings.log_step_warning(step, f"Missing 'all' attribute at {page_href}")

    return latest_items, received_all
