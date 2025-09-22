from contextlib import asynccontextmanager
from dataclasses import dataclass
from http import HTTPMethod, HTTPStatus
from typing import AsyncIterator, cast

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)

from cactus_client.action.server import (
    get_resource_for_step,
    paginate_list_resource_items,
)
from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import RequestException
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import ClientContext, ExecutionContext, ResourceStore
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)


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


def create_test_app_for_route(method: HTTPMethod, route: str, behaviour: list[RouteBehaviour]):
    async def do_behaviour(request):
        if len(behaviour) == 0:
            return web.Response(body=b"No more mocked behaviour", status=500)

        b = behaviour.pop(0)
        return web.Response(body=b.body, status=b.status, headers=b.headers)

    app = web.Application()
    app.router.add_route(method.value, route, do_behaviour)
    return app


@asynccontextmanager
async def create_test_session(
    aiohttp_client, method: HTTPMethod, route: str, behaviour: list[RouteBehaviour]
) -> AsyncIterator[ClientSession]:
    client: TestClient = await aiohttp_client(create_test_app_for_route(method, route, behaviour))

    yield ClientSession(base_url=client.server.make_url("/"))


def create_testing_contexts(tp: TestProcedure, client_session) -> tuple[ExecutionContext, StepExecution]:

    client_alias = tp.preconditions.required_clients[0].id
    client_context = ClientContext(
        test_procedure_alias=client_alias,
        client_config=generate_class_instance(ClientConfig, optional_is_none=True),
        discovered_resources=ResourceStore(),
        session=client_session,
    )

    execution_context = ExecutionContext(
        tp,
        "/my/dcap/path",
        {client_alias: client_context},
        StepExecutionList(),
        WarningTracker(),
        ProgressTracker(),
        ResponseTracker(),
    )

    # attempts: int  # How many times has this step been attempted
    step_execution = generate_class_instance(
        StepExecution,
        optional_is_none=True,
        generate_relationships=True,
        client_alias=client_alias,
        client_resources_alias=client_alias,
    )

    return (execution_context, step_execution)


@pytest.mark.asyncio
async def test_get_resource_for_step_success(aiohttp_client, dummy_test_procedure):
    """Does get_resource_for_step handle parsing the XML and returning the correct data"""
    async with create_test_session(
        aiohttp_client, HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")]
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)
        result = await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

    # Assert - contents of response
    assert isinstance(result, DeviceCapabilityResponse)
    assert result.EndDeviceListLink.all_ == 2
    assert result.EndDeviceListLink.href == "/envoy-svc-static-36/edev"

    # Assert - contents of trackers
    assert len(execution_context.warnings.warnings) == 0
    assert len(execution_context.responses.responses) == 1


@pytest.mark.asyncio
async def test_get_resource_for_step_bad_request(aiohttp_client, dummy_test_procedure):
    """Does get_resource_for_step properly raise exceptions if a failure status is returned"""

    # We will try and trick the code by returning a normal dcap but with a proper error
    async with create_test_session(
        aiohttp_client, HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.BAD_REQUEST, "dcap.xml")]
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)

        with pytest.raises(RequestException):
            await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

        # Assert - contents of trackers
        assert len(execution_context.responses.responses) == 1, "We still log errors"


@pytest.mark.asyncio
async def test_get_resource_for_step_xml_failure(aiohttp_client, dummy_test_procedure):
    """Does get_resource_for_step properly raise exceptions if a failure status is returned"""

    # The server is sending valid sep2 XML but the type doesn't match what we want
    async with create_test_session(
        aiohttp_client, HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "edev-list.xml")]
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)

        with pytest.raises(RequestException):
            await get_resource_for_step(DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar")

        # Assert - contents of trackers
        assert len(execution_context.responses.responses) == 1, "We still log errors"


@pytest.mark.asyncio
async def test_paginate_list_resource_items(aiohttp_client, dummy_test_procedure):
    """Does paginate_list_resource_items work with EndDevice lists of multiple pages"""
    async with create_test_session(
        aiohttp_client,
        HTTPMethod.GET,
        "/foo/bar",
        [
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-2.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),
        ],
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)
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
async def test_paginate_list_resource_items_handle_failure(aiohttp_client, dummy_test_procedure):
    """Does paginate_list_resource_items handle failures in one of the pagination requests"""
    async with create_test_session(
        aiohttp_client,
        HTTPMethod.GET,
        "/foo/bar",
        [
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
            RouteBehaviour.xml(HTTPStatus.INTERNAL_SERVER_ERROR, "edev-list-2.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),  # Should never run
        ],
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)

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
async def test_paginate_list_resource_items_bad_all_count(aiohttp_client, dummy_test_procedure):
    """Does paginate_list_resource_items check the"""
    async with create_test_session(
        aiohttp_client,
        HTTPMethod.GET,
        "/foo/bar",
        [
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),
        ],
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)
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
async def test_paginate_list_resource_items_empty_list(aiohttp_client, dummy_test_procedure):
    """Does paginate_list_resource_items work with an empty list"""
    behaviour = RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml")
    behaviour.body = behaviour.body.decode().replace('all="3"', 'all="0"').encode()  # Make this a proper empty list
    async with create_test_session(
        aiohttp_client,
        HTTPMethod.GET,
        "/foo/bar",
        [behaviour],
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)
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
async def test_paginate_list_resource_items_too_many_requests(aiohttp_client, dummy_test_procedure):
    """Does paginate_list_resource_items handle failures in one of the pagination requests"""
    async with create_test_session(
        aiohttp_client,
        HTTPMethod.GET,
        "/foo/bar",
        [
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-1.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-2.xml"),
            RouteBehaviour.xml(HTTPStatus.OK, "edev-list-empty.xml"),  # Should never run
        ],
    ) as session:
        (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, session)

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
