import logging
from datetime import datetime
from http import HTTPMethod
from typing import Callable, TypeVar

from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import RequestException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.http import ServerResponse

logger = logging.getLogger(__name__)

AnyResourceType = TypeVar("AnyResourceType", bound=Resource)
AnyType = TypeVar("AnyType")


def resource_to_sep2_xml(resource: Resource) -> str:
    xml = resource.to_xml(skip_empty=False, exclude_none=True, exclude_unset=True)
    if xml is None:
        return ""
    if isinstance(xml, bytes):
        return xml.decode()
    return xml


async def request_for_step(
    step: StepExecution, context: ExecutionContext, path: str, method: HTTPMethod, sep2_xml_body: str | None = None
) -> ServerResponse:
    """Makes a request to the CSIP-Aus server (for the current context) and endpoint - returns a raw parsed response and
    logs the actions in the various context trackers. Raises a RequestException on connection failure."""
    session = context.session(step)

    await context.progress.add_log(step, f"Requesting {method} {path}")

    headers = {"Accept": MIME_TYPE_SEP2}
    if sep2_xml_body is not None:
        headers["Content-Type"] = MIME_TYPE_SEP2

    server_request = await context.responses.set_active_request(method, path, body=sep2_xml_body, headers=headers)
    async with session.request(method=method, url=path, data=sep2_xml_body, headers=headers) as raw_response:
        try:
            response = await ServerResponse.from_response(raw_response, request=server_request)
        except Exception as exc:
            logger.error(f"Caught exception attempting to {method} {path}", exc_info=exc)
            await context.responses.clear_active_request()
            raise RequestException(f"Caught exception attempting to {method} {path}: {exc}")

        await context.responses.log_response_body(response)
        await context.responses.clear_active_request()
        return response


async def client_error_request_for_step(
    step: StepExecution, context: ExecutionContext, path: str, method: HTTPMethod, sep2_xml_body: str | None = None
) -> ErrorResponse:
    """Similar to request_for_step but is only successful if the resulting response is returned as a valid sep2 error"""
    response = await request_for_step(step, context, path, method, sep2_xml_body)

    if not response.is_client_error():
        raise RequestException(
            f"Received status {response.status} but expected 4XX requesting {response.method} {path}."
        )

    try:
        return ErrorResponse.from_xml(response.body)
    except Exception as exc:
        logger.error(f"Failure parsing ErrorResponse from {len(response.body)} chars at {path}", exc_info=exc)
        logger.error(response.body)
        raise RequestException(f"Failure parsing ErrorResponse from {len(response.body)} chars at {path}: {exc}")


async def get_resource_for_step(
    t: type[AnyResourceType], step: StepExecution, context: ExecutionContext, href: str
) -> AnyResourceType:
    """Makes a GET request for a particular href and parses the resulting XML into an expected type (t). Raises a
    RequestException if the connection fails, returns an error or fails to parse to t"""
    # Make the raw request
    response = await request_for_step(step, context, href, HTTPMethod.GET)

    if not response.is_success():
        raise RequestException(f"Received status {response.status} requesting {response.method} {href}.")

    try:
        return t.from_xml(response.body)
    except Exception as exc:
        logger.error(f"Caught exception attempting to parse {len(response.body)} chars from {href}", exc_info=exc)
        logger.error(response.body)
        raise RequestException(f"Caught exception parsing {len(response.body)} chars from {href}: {exc}")


async def submit_and_refetch_resource_for_step(
    t: type[AnyResourceType],
    step: StepExecution,
    context: ExecutionContext,
    method: HTTPMethod,
    href: str,
    sep2_xml_body: str,
    no_location_header: bool = False,
) -> AnyResourceType:
    """Makes a method request to a particular href, submitting sep2_xml_body and expecting a success response. Then
    parse the resulting response for a Location header and then GET that URI, returning the resulting resource.

    if no_location_header is set - the initial response will not be checked for a Location header and instead href
    will be used as the GET (use this for when updating a resource insitu, not creating a new resource)"""

    # Make the submit request
    response = await request_for_step(step, context, href, method, sep2_xml_body=sep2_xml_body)
    if not response.is_success():
        raise RequestException(f"Received status {response.status} requesting {response.method} {href}.")

    if no_location_header:
        refetch_href = href
    else:
        if not response.location:
            raise RequestException(
                f"{response.status} response from {response.method} {href} did not return an expected 'Location' header."
            )
        refetch_href = response.location

    return await get_resource_for_step(t, step, context, refetch_href)


def build_paging_params(
    start: int | None = None, limit: int | None = None, changed_after: datetime | None = None
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
    context: The execution context tha this request is being made under
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
        page_href = list_href + build_paging_params(start=start, limit=page_size)
        latest_list = await get_resource_for_step(list_type, step, context, page_href)
        latest_items = item_callback(latest_list)
        if latest_items is None:
            latest_items = []  # pydantic-xml  can parse a missing/empty list as None - we need to compensate
        all_items.extend(latest_items)

        # Start pulling apart the response and doing some cursory checks
        received_all: int | None = getattr(latest_list, "all_", None)
        received_results: int | None = getattr(latest_list, "results", None)
        if received_results is None:
            context.warnings.log_step_warning(step, f"Missing 'results' attribute at {page_href}")
        elif received_results != len(latest_items):
            context.warnings.log_step_warning(
                step, f"'results' attribute shows {received_results} but got {len(latest_items)} items"
            )

        if received_all is None:
            context.warnings.log_step_warning(step, f"Missing 'all' attribute at {page_href}")
        else:
            every_all_value.append(received_all)

        # Prepare next page
        start += page_size
        if len(latest_items) == 0:  # When we receive an empty page - we know we are done
            break

        # This is a safety valve in case a server misbehaves and keeps sending us more data
        pages_requested += 1
        if pages_requested >= max_pages_requested:
            raise RequestException(
                f"Paginating {list_href} exceeded max pages {max_pages_requested} at page size {page_size}."
            )

    # Final check of the all attributes
    expected_count = 0 if len(every_all_value) == 0 else every_all_value[0]
    if len(set(every_all_value)) > 1:
        context.warnings.log_step_warning(
            step, f"The 'all' attribute at {list_href} has varied while paginating through. This is likely an error."
        )
    elif expected_count != len(all_items):
        context.warnings.log_step_warning(
            step,
            f"The 'all' attribute at {list_href} indicated {expected_count} items but {len(all_items)} items returned.",
        )

    return all_items
