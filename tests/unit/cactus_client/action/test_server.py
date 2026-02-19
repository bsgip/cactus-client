import unittest.mock as mock
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from http import HTTPMethod, HTTPStatus
from itertools import product
from typing import AsyncIterator, cast

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceRequest,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePointListResponse

from cactus_client.action.server import (
    RATE_LIMIT_RETRY_DELAYS,
    client_error_or_empty_list_request_for_step,
    client_error_request_for_step,
    delete_and_check_resource_for_step,
    fetch_list_page,
    get_resource_for_step,
    paginate_list_resource_items,
    request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import RequestException


@dataclass
class RouteBehaviour:
    status: HTTPStatus
    body: bytes
    headers: dict[str, str]

    @staticmethod
    def xml(status: HTTPStatus, file_name: str) -> "RouteBehaviour":
        with open("tests/data/" + file_name, "r") as fp:
            raw_xml = fp.read()
        return RouteBehaviour(status, raw_xml.encode(), {"Content-Type": MIME_TYPE_SEP2})

    @staticmethod
    def no_content_location(status: HTTPStatus, location: str) -> "RouteBehaviour":
        return RouteBehaviour(status, bytes(), {"Location": location})


@dataclass
class TestingAppRoute:
    __test__ = False
    method: HTTPMethod
    path: str
    behaviour: list[RouteBehaviour]


def create_test_app_for_routes(routes: list[TestingAppRoute]):
    """This is a mess of closures - apologies for that!

    Will create a test app with a route for item in routes (and those created routes will have the expected behaviour)
    """

    def add_route_to_app(app: web.Application, route: TestingAppRoute) -> None:
        async def do_behaviour(request):
            if len(route.behaviour) == 0:
                return web.Response(body=b"No more mocked behaviour", status=500)

            b = route.behaviour.pop(0)
            return web.Response(body=b.body, status=b.status, headers=b.headers)

        app.router.add_route(route.method.value, route.path, do_behaviour)

    app = web.Application()
    for r in routes:
        add_route_to_app(app, r)
    return app


@asynccontextmanager
async def create_test_session(aiohttp_client, routes: list[TestingAppRoute]) -> AsyncIterator[ClientSession]:
    client: TestClient = await aiohttp_client(create_test_app_for_routes(routes))

    yield ClientSession(base_url=client.server.make_url("/"))


@pytest.mark.parametrize(
    "refetch_status, refetch_delay",
    product([HTTPStatus.NOT_FOUND, HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN], [0, 2000]),
)
@pytest.mark.asyncio
async def test_delete_and_check_resource_for_step_success(
    aiohttp_client, testing_contexts_factory, refetch_status: bool, refetch_delay: int
):
    """Does delete_and_check_resource_for_step handle a variety of "deleted" responses on refetch"""
    delete_route = TestingAppRoute(HTTPMethod.DELETE, "/foo/bar", [RouteBehaviour(HTTPStatus.OK, bytes(), {})])
    get_route = TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour(refetch_status, bytes(), {})])
    async with create_test_session(aiohttp_client, [delete_route, get_route]) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        execution_context.server_config = replace(execution_context.server_config, refetch_delay_ms=refetch_delay)

        start = datetime.now()
        await delete_and_check_resource_for_step(step_execution, execution_context, "/foo/bar")
        finish = datetime.now()

    assert len(delete_route.behaviour) == 0, "Request should've been made"
    assert len(get_route.behaviour) == 0, "Request should've been made"

    # Assert use of the refetch delay
    if refetch_delay == 0:
        assert (finish - start).total_seconds() < 1, "There shouldn't have been any delay"
    else:
        assert (finish - start).total_seconds() >= (refetch_delay / 1000)


@pytest.mark.parametrize(
    "refetch_status",
    [
        HTTPStatus.OK,
        HTTPStatus.NO_CONTENT,
        HTTPStatus.MOVED_PERMANENTLY,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.METHOD_NOT_ALLOWED,
    ],
)
@pytest.mark.asyncio
async def test_delete_and_check_resource_for_step_refetch_bad_response(
    aiohttp_client, testing_contexts_factory, refetch_status
):
    """Does delete_and_check_resource_for_step Raise exceptions if the refetch doesn't behave as expected"""
    delete_route = TestingAppRoute(HTTPMethod.DELETE, "/foo/bar", [RouteBehaviour(HTTPStatus.OK, bytes(), {})])
    get_route = TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour(refetch_status, bytes(), {})])
    async with create_test_session(aiohttp_client, [delete_route, get_route]) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await delete_and_check_resource_for_step(step_execution, execution_context, "/foo/bar")

    assert len(delete_route.behaviour) == 0, "Request should've been made"
    assert len(get_route.behaviour) == 0, "Request should've been made"


@pytest.mark.parametrize(
    "delete_status",
    [
        HTTPStatus.MOVED_PERMANENTLY,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.METHOD_NOT_ALLOWED,
    ],
)
@pytest.mark.asyncio
async def test_delete_and_check_resource_for_step_delete_bad_response(
    aiohttp_client, testing_contexts_factory, delete_status
):
    """Does delete_and_check_resource_for_step Raise exceptions if the delete doesn't behave as expected"""
    delete_route = TestingAppRoute(HTTPMethod.DELETE, "/foo/bar", [RouteBehaviour(delete_status, bytes(), {})])
    get_route = TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour(HTTPStatus.NOT_FOUND, bytes(), {})])
    async with create_test_session(aiohttp_client, [delete_route, get_route]) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await delete_and_check_resource_for_step(step_execution, execution_context, "/foo/bar")

    assert len(delete_route.behaviour) == 0, "Request should've been made"
    assert len(get_route.behaviour) == 1, "The refetch shouldn't have been made"


@pytest.mark.asyncio
async def test_get_resource_for_step_success(aiohttp_client, testing_contexts_factory):
    """Does get_resource_for_step handle parsing the XML and returning the correct data"""
    async with create_test_session(
        aiohttp_client, [TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")])]
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

    # Assert - contents of response
    assert isinstance(result, DeviceCapabilityResponse)
    assert result.EndDeviceListLink.all_ == 2
    assert result.EndDeviceListLink.href == "/envoy-svc-static-36/edev"

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.asyncio
async def test_get_resource_for_step_bad_request(aiohttp_client, testing_contexts_factory):
    """Does get_resource_for_step properly raise exceptions if a failure status is returned"""

    # We will try and trick the code by returning a normal dcap but with a proper error
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.BAD_REQUEST, "dcap.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

        # Assert - contents of trackers
        assert len(execution_context.responses.responses) == 1, "We still log errors"


@pytest.mark.asyncio
async def test_get_resource_for_step_xml_failure(aiohttp_client, testing_contexts_factory):
    """Does get_resource_for_step properly raise exceptions if the XML can't parse into the desired type"""

    # The server is sending valid sep2 XML but the type doesn't match what we want
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

        # Assert - contents of trackers
        assert len(execution_context.responses.responses) == 1, "We still log errors"


@pytest.mark.parametrize("has_property_changes, refetch_delay", product([True, False], [0, 2000]))
@mock.patch("cactus_client.action.server.get_property_changes")
@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_success(
    mock_get_property_changes: mock.MagicMock,
    has_property_changes: bool,
    refetch_delay: int,
    aiohttp_client,
    testing_contexts_factory,
):
    """Does submit_and_refetch_resource_for_step handle parsing the XML and returning the correct data"""
    if has_property_changes:
        mock_get_property_changes.return_value = "Some form of property error"
    else:
        mock_get_property_changes.return_value = None
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.PUT, "/baz", [RouteBehaviour.no_content_location(HTTPStatus.NO_CONTENT, "/foo/bar")]
            ),
            TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")]),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        execution_context.server_config = replace(execution_context.server_config, refetch_delay_ms=refetch_delay)

        start = datetime.now()
        result = await submit_and_refetch_resource_for_step(
            DeviceCapabilityResponse,
            step_execution,
            execution_context,
            HTTPMethod.PUT,
            "/baz",
            generate_class_instance(DeviceCapabilityResponse),
        )
        finish = datetime.now()

    # Assert - contents of response
    assert isinstance(result, DeviceCapabilityResponse)
    assert result.EndDeviceListLink.all_ == 2
    assert result.EndDeviceListLink.href == "/envoy-svc-static-36/edev"

    # Assert - contents of trackers
    if has_property_changes:
        assert len(execution_context.warnings.warnings) == 1
    else:
        assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 2

    # Assert use of the refetch delay
    if refetch_delay == 0:
        assert (finish - start).total_seconds() < 1, "There shouldn't have been any delay"
    else:
        assert (finish - start).total_seconds() >= (refetch_delay / 1000)


@pytest.mark.parametrize("has_property_changes", [True, False])
@mock.patch("cactus_client.action.server.get_property_changes")
@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_success_no_location_header(
    mock_get_property_changes: mock.Mock, has_property_changes: bool, aiohttp_client, testing_contexts_factory
):
    """Does submit_and_refetch_resource_for_step handle parsing the XML and returning the correct data"""
    if has_property_changes:
        mock_get_property_changes.return_value = "Some form of property error"
    else:
        mock_get_property_changes.return_value = None

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(HTTPMethod.PUT, "/foo", [RouteBehaviour(HTTPStatus.NO_CONTENT, bytes(), {})]),
            TestingAppRoute(HTTPMethod.GET, "/foo", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")]),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await submit_and_refetch_resource_for_step(
            DeviceCapabilityResponse,
            step_execution,
            execution_context,
            HTTPMethod.PUT,
            "/foo",
            generate_class_instance(DeviceCapabilityResponse),
            no_location_header=True,
        )

    # Assert - contents of response
    assert isinstance(result, DeviceCapabilityResponse)
    assert result.EndDeviceListLink.all_ == 2
    assert result.EndDeviceListLink.href == "/envoy-svc-static-36/edev"

    # Assert - contents of trackers
    if has_property_changes:
        assert len(execution_context.warnings.warnings) == 1
    else:
        assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 2


@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_failure_no_location_header(
    aiohttp_client, testing_contexts_factory
):
    """Does submit_and_refetch_resource_for_step abort if we don't get a location header"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(HTTPMethod.PUT, "/foo", [RouteBehaviour(HTTPStatus.NO_CONTENT, bytes(), {})]),
            TestingAppRoute(HTTPMethod.GET, "/foo", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")]),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await submit_and_refetch_resource_for_step(
                DeviceCapabilityResponse,
                step_execution,
                execution_context,
                HTTPMethod.PUT,
                "/foo",
                generate_class_instance(DeviceCapabilityResponse),
            )


@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_server_overrides_post_rate(
    aiohttp_client, testing_contexts_factory
):
    """When the server overrides postRate on the returned resource, it should NOT produce a warning."""

    submitted = EndDeviceRequest(changedTime=1000, sFDI=12345, postRate=60)
    returned = EndDeviceResponse(changedTime=1000, sFDI=12345, postRate=30)
    returned_xml = resource_to_sep2_xml(returned)

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST, "/edev", [RouteBehaviour.no_content_location(HTTPStatus.CREATED, "/edev/1")]
            ),
            TestingAppRoute(
                HTTPMethod.GET,
                "/edev/1",
                [RouteBehaviour(HTTPStatus.OK, returned_xml.encode(), {"Content-Type": MIME_TYPE_SEP2})],
            ),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await submit_and_refetch_resource_for_step(
            EndDeviceResponse,
            step_execution,
            execution_context,
            HTTPMethod.POST,
            "/edev",
            submitted,
        )

    assert isinstance(result, EndDeviceResponse)
    assert result.postRate == 30  # Server's overridden value
    assert len(execution_context.warnings.warnings) == 0


@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_failure_initial_request(aiohttp_client, testing_contexts_factory):
    """Does submit_and_refetch_resource_for_step abort if the first request fails"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(HTTPMethod.POST, "/foo", [RouteBehaviour(HTTPStatus.INTERNAL_SERVER_ERROR, bytes(), {})]),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await submit_and_refetch_resource_for_step(
                DeviceCapabilityResponse,
                step_execution,
                execution_context,
                HTTPMethod.POST,
                "/foo",
                generate_class_instance(DeviceCapabilityResponse),
                no_location_header=True,
            )


@pytest.mark.asyncio
async def test_submit_and_refetch_resource_for_step_failure_refetch_request(aiohttp_client, testing_contexts_factory):
    """Does submit_and_refetch_resource_for_step abort if the refetch request fails"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(HTTPMethod.DELETE, "/foo", [RouteBehaviour.no_content_location(HTTPStatus.OK, "/foo/bar")]),
            TestingAppRoute(
                HTTPMethod.GET, "/foo/bar", [RouteBehaviour(HTTPStatus.INTERNAL_SERVER_ERROR, bytes(), {})]
            ),
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await submit_and_refetch_resource_for_step(
                DeviceCapabilityResponse,
                step_execution,
                execution_context,
                HTTPMethod.DELETE,
                "/foo",
                generate_class_instance(DeviceCapabilityResponse),
            )


@pytest.mark.asyncio
async def test_paginate_list_resource_items(aiohttp_client, testing_contexts_factory):
    """Does paginate_list_resource_items work with EndDevice lists of multiple pages"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-2.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await paginate_list_resource_items(
            EndDeviceListResponse,
            step_execution,
            execution_context,
            "/foo/bar",
            2,
            lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
        )

    # Assert - contents of response
    assert_list_type(EndDeviceResponse, result, count=3)
    assert result[0].href == "/envoy-svc-static-36/edev/0"
    assert result[1].href == "/envoy-svc-static-36/edev/1"
    assert result[2].href == "/envoy-svc-static-36/edev/2"

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 3, "we requested 3 pages of data"
    assert "?s=0&l=2" in execution_context.responses.responses[0].url
    assert "?s=2&l=2" in execution_context.responses.responses[1].url
    assert "?s=4&l=2" in execution_context.responses.responses[2].url


@pytest.mark.asyncio
async def test_paginate_list_resource_items_handle_failure(aiohttp_client, testing_contexts_factory):
    """Does paginate_list_resource_items handle failures in one of the pagination requests"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
                    RouteBehaviour.xml(HTTPStatus.INTERNAL_SERVER_ERROR, "edev-list-2.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),  # Should never run
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await paginate_list_resource_items(
                EndDeviceListResponse,
                step_execution,
                execution_context,
                "/foo/bar",
                2,
                lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
            )

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 2, "we requested 2 pages of data (we aborted due to failure)"
    assert "?s=0&l=2" in execution_context.responses.responses[0].url
    assert "?s=2&l=2" in execution_context.responses.responses[1].url


@pytest.mark.asyncio
async def test_paginate_list_resource_items_bad_all_count(aiohttp_client, testing_contexts_factory):
    """Does paginate_list_resource_items check the"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await paginate_list_resource_items(
            EndDeviceListResponse,
            step_execution,
            execution_context,
            "/foo/bar",
            2,
            lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
        )

    # Assert - contents of response
    assert_list_type(EndDeviceResponse, result, count=2)
    assert result[0].href == "/envoy-svc-static-36/edev/0"
    assert result[1].href == "/envoy-svc-static-36/edev/1"

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 1, "The all count said 3 - but we only got 2"
    assert len(execution_context.responses.responses) == 2, "we requested 2 pages of data"
    assert "?s=0&l=2" in execution_context.responses.responses[0].url
    assert "?s=2&l=2" in execution_context.responses.responses[1].url


@pytest.mark.asyncio
async def test_paginate_list_resource_items_empty_list(aiohttp_client, testing_contexts_factory):
    """Does paginate_list_resource_items work with an empty list"""
    behaviour = RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml")
    behaviour.body = behaviour.body.decode().replace('all="3"', 'all="0"').encode()  # Make this a proper empty list
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.GET, "/foo/bar", [behaviour])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await paginate_list_resource_items(
            EndDeviceListResponse,
            step_execution,
            execution_context,
            "/foo/bar",
            3,
            lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
        )

    # Assert - contents of response
    assert_list_type(EndDeviceResponse, result, count=0)

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1, "we requested 1 page of data"
    assert "?s=0&l=3" in execution_context.responses.responses[0].url


@pytest.mark.asyncio
async def test_paginate_list_resource_items_too_many_requests(aiohttp_client, testing_contexts_factory):
    """Does paginate_list_resource_items handle failures in one of the pagination requests"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-2.xml"),
                    RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),  # Should never run
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await paginate_list_resource_items(
                EndDeviceListResponse,
                step_execution,
                execution_context,
                "/foo/bar",
                2,
                lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
                max_pages_requested=2,
            )

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 2, "we requested 2 pages of data (we aborted early)"
    assert "?s=0&l=2" in execution_context.responses.responses[0].url
    assert "?s=2&l=2" in execution_context.responses.responses[1].url


def test_resource_to_sep2_xml():
    """Mainly a sanity check on resource_to_sep2_xml to ensure it generates something that looks like XML"""
    xml1 = resource_to_sep2_xml(generate_class_instance(EndDeviceRequest, seed=1, generate_relationships=True))
    xml2 = resource_to_sep2_xml(generate_class_instance(EndDeviceRequest, seed=2, generate_relationships=True))
    xml3 = resource_to_sep2_xml(
        generate_class_instance(EndDeviceRequest, seed=2, generate_relationships=True, optional_is_none=True)
    )

    assert xml1 and isinstance(xml1, str)
    assert xml2 and isinstance(xml2, str)
    assert xml3 and isinstance(xml3, str)

    assert xml1 != xml2

    assert "</EndDevice>" in xml1


@pytest.mark.asyncio
async def test_fetch_list_page(aiohttp_client, testing_contexts_factory):
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        start = 5
        limit = 10

        items, all_attribute = await fetch_list_page(
            EndDeviceListResponse,
            step_execution,
            execution_context,
            "/foo/bar",
            start,
            limit,
            lambda list_response: cast(EndDeviceListResponse, list_response).EndDevice,
        )

    # Assert - contents of response
    assert_list_type(EndDeviceResponse, items, count=2)
    assert items[0].href == "/envoy-svc-static-36/edev/0"
    assert items[1].href == "/envoy-svc-static-36/edev/1"

    # Assert - only made a single request with correct params
    assert len(execution_context.responses.responses) == 1, "should only request 1 page"
    requested_url = execution_context.responses.responses[0].url
    assert f"?s={start}&l={limit}" in requested_url, f"Expected params s={start}&l={limit} in URL"


@mock.patch("cactus_client.action.server.asyncio.sleep")
@pytest.mark.asyncio
async def test_request_for_step_429_retry_success(mock_sleep, aiohttp_client, testing_contexts_factory):
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour(HTTPStatus.TOO_MANY_REQUESTS, b"", {}),
                    RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml"),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        response = await request_for_step(step_execution, execution_context, "/foo/bar", HTTPMethod.GET)

    assert response.status == HTTPStatus.OK
    assert len(execution_context.responses.responses) == 2
    mock_sleep.assert_called_once_with(RATE_LIMIT_RETRY_DELAYS[0])


@mock.patch("cactus_client.action.server.asyncio.sleep")
@pytest.mark.asyncio
async def test_request_for_step_429_all_retries_exhausted(mock_sleep, aiohttp_client, testing_contexts_factory):
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                "/foo/bar",
                [
                    RouteBehaviour(HTTPStatus.TOO_MANY_REQUESTS, b"", {})
                    for _ in range(len(RATE_LIMIT_RETRY_DELAYS) + 1)
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        response = await request_for_step(step_execution, execution_context, "/foo/bar", HTTPMethod.GET)

    assert response.status == HTTPStatus.TOO_MANY_REQUESTS
    assert len(execution_context.responses.responses) == len(RATE_LIMIT_RETRY_DELAYS) + 1
    assert mock_sleep.call_count == len(RATE_LIMIT_RETRY_DELAYS)
    for i, delay in enumerate(RATE_LIMIT_RETRY_DELAYS):
        assert mock_sleep.call_args_list[i] == mock.call(delay)


@pytest.mark.asyncio
async def test_client_error_request_for_step_success(aiohttp_client, testing_contexts_factory):
    """Does client_error_request_for_step handle parsing the XML and returning the correct data"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.BAD_REQUEST, "error.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await client_error_request_for_step(
            step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "post body"
        )

    # Assert - contents of response
    assert isinstance(result, ErrorResponse)
    assert result.reasonCode == 2
    assert result.maxRetryDuration == 180

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.parametrize("status_code", [HTTPStatus.OK, HTTPStatus.INTERNAL_SERVER_ERROR])
@pytest.mark.asyncio
async def test_client_error_request_for_step_non_client_error(status_code, aiohttp_client, testing_contexts_factory):
    """Does client_error_request_for_step handle parsing the XML and returning the correct data"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(status_code, "error.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await client_error_request_for_step(step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "body")

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.parametrize(
    "list_type, xml_file",
    [
        (EndDeviceListResponse, "edev-list-completely-empty.xml"),
        (MirrorUsagePointListResponse, "mup-list-empty.xml"),
    ],
)
@pytest.mark.asyncio
async def test_client_error_or_empty_list_request_for_step_success_empty(
    list_type, xml_file, aiohttp_client, testing_contexts_factory
):
    """Does client_error_or_empty_list_request_for_step handle parsing the empty list XML and returning the correct
    data"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, xml_file)])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await client_error_or_empty_list_request_for_step(
            list_type, step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "post body"
        )

    # Assert - contents of response
    assert isinstance(result, list_type)
    assert result.all_ == 0
    assert result.results == 0

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.parametrize(
    "list_type, xml_file",
    [
        (EndDeviceListResponse, "edev-list-1.xml"),  # This has entries
        (EndDeviceListResponse, "edev-list-empty.xml"),  # This has all="3" results="0"
    ],
)
@pytest.mark.asyncio
async def test_client_error_or_empty_list_request_for_step_fail_not_empty(
    list_type, xml_file, aiohttp_client, testing_contexts_factory
):
    """Does client_error_or_empty_list_request_for_step check the all/results field"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, xml_file)])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await client_error_or_empty_list_request_for_step(
                list_type, step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "post body"
            )

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.asyncio
async def test_client_error_or_empty_list_request_for_step_success_error(aiohttp_client, testing_contexts_factory):
    """Does client_error_or_empty_list_request_for_step handle parsing an Error XML"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.BAD_REQUEST, "error.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)
        result = await client_error_or_empty_list_request_for_step(
            EndDeviceListResponse, step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "post body"
        )

    # Assert - contents of response
    assert isinstance(result, ErrorResponse)
    assert result.reasonCode == 2
    assert result.maxRetryDuration == 180

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.parametrize("status_code", [HTTPStatus.BAD_GATEWAY, HTTPStatus.INTERNAL_SERVER_ERROR])
@pytest.mark.asyncio
async def test_client_error_or_empty_list_request_for_step_non_client_error(
    status_code, aiohttp_client, testing_contexts_factory
):
    """Does client_error_or_empty_list_request_for_step handle 5XX errors with an exception?"""
    async with create_test_session(
        aiohttp_client,
        [TestingAppRoute(HTTPMethod.POST, "/foo/bar", [RouteBehaviour.xml(status_code, "error.xml")])],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(session)

        with pytest.raises(RequestException):
            await client_error_or_empty_list_request_for_step(
                EndDeviceListResponse, step_execution, execution_context, "/foo/bar", HTTPMethod.POST, "body"
            )

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1
