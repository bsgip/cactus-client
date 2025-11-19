from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from http import HTTPMethod, HTTPStatus
from typing import AsyncIterator

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_client_notifications.schema import (
    URI_MANAGE_ENDPOINT,
    URI_MANAGE_ENDPOINT_LIST,
    CollectedNotification,
    CollectEndpointResponse,
    CreateEndpointResponse,
)

from cactus_client.action.notifications import (
    collect_notifications_for_subscription,
    fetch_notification_webhook_for_subscription,
    safely_delete_all_notification_webhooks,
    update_notification_webhook_for_subscription,
)
from cactus_client.error import NotificationException
from cactus_client.model.context import NotificationsContext


@dataclass
class RouteBehaviour:
    status: HTTPStatus
    body: str


@dataclass
class TestingAppRoute:
    __test__ = False
    method: HTTPMethod
    path: str
    behaviour: list[RouteBehaviour]
    request_bodies: list[str] = field(default_factory=list)


def create_test_app_for_routes(routes: list[TestingAppRoute]):
    """This is a mess of closures - apologies for that!

    Will create a test app with a route for item in routes (and those created routes will have the expected behaviour)
    """

    def add_route_to_app(app: web.Application, route: TestingAppRoute) -> None:
        async def do_behaviour(request):

            route.request_bodies.append(await request.text())

            if len(route.behaviour) == 0:
                return web.Response(body=b"No more mocked behaviour", status=500)

            b = route.behaviour.pop(0)
            return web.Response(body=b.body, status=b.status, headers={"Content-Type": "application/json"})

        app.router.add_route(route.method.value, route.path, do_behaviour)

    app = web.Application()
    for r in routes:
        add_route_to_app(app, r)
    return app


@asynccontextmanager
async def create_test_session(aiohttp_client, routes: list[TestingAppRoute]) -> AsyncIterator[ClientSession]:
    client: TestClient = await aiohttp_client(create_test_app_for_routes(routes))

    yield ClientSession(base_url=client.server.make_url("/"))


@pytest.mark.asyncio
async def test_fetch_notification_webhook_for_subscription(aiohttp_client, testing_contexts_factory):
    """Does fetch_notification_webhook_for_subscription handle a valid response from the server"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                URI_MANAGE_ENDPOINT_LIST,
                [
                    RouteBehaviour(
                        HTTPStatus.OK, CreateEndpointResponse("abc123", "https://my.example:123/uri").to_json()
                    ),
                    RouteBehaviour(
                        HTTPStatus.OK, CreateEndpointResponse("def456", "https://my.other.example:456/path").to_json()
                    ),
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        result1 = await fetch_notification_webhook_for_subscription(step_execution, execution_context, "sub123")
        result2 = await fetch_notification_webhook_for_subscription(step_execution, execution_context, "sub123")
        result3 = await fetch_notification_webhook_for_subscription(step_execution, execution_context, "sub1234")

    # Assert - contents of response
    assert isinstance(result1, str) and isinstance(result2, str) and isinstance(result3, str)
    assert result1 == "https://my.example:123/uri"
    assert result2 == "https://my.example:123/uri", "The second request should come from the cache"
    assert result3 == "https://my.other.example:456/path"


@pytest.mark.asyncio
async def test_notifications_server_request_status_error(aiohttp_client, testing_contexts_factory):
    """Does fetch_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                URI_MANAGE_ENDPOINT_LIST,
                [
                    RouteBehaviour(
                        HTTPStatus.BAD_REQUEST, CreateEndpointResponse("abc123", "https://my.example:123/uri").to_json()
                    )
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await fetch_notification_webhook_for_subscription(step_execution, execution_context, "sub123")


@pytest.mark.asyncio
async def test_notifications_server_request_parsing_error(aiohttp_client, testing_contexts_factory):
    """Does fetch_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                URI_MANAGE_ENDPOINT_LIST,
                [RouteBehaviour(HTTPStatus.OK, "{ }")],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await fetch_notification_webhook_for_subscription(step_execution, execution_context, "sub123")


@pytest.mark.parametrize(
    "expected",
    [
        [],
        [generate_class_instance(CollectedNotification, seed=1, generate_relationships=True)],
        [
            generate_class_instance(CollectedNotification, seed=1, generate_relationships=True),
            generate_class_instance(CollectedNotification, seed=2, generate_relationships=True, optional_is_none=True),
        ],
    ],
)
@pytest.mark.asyncio
async def test_collect_notifications_for_subscription(aiohttp_client, testing_contexts_factory, expected):
    """Does collect_notifications_for_subscription handle a valid response from the server"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse(expected).to_json()),
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("abc-123", "foo")

        result = await collect_notifications_for_subscription(step_execution, execution_context, "sub1")

    # Assert - contents of response
    assert_list_type(CollectedNotification, result, count=len(expected))
    assert result == expected


@pytest.mark.asyncio
async def test_collect_notifications_for_subscription_not_configured(aiohttp_client, testing_contexts_factory):
    """Does collect_notifications_for_subscription fail gracefully if an endpoint hasn't been created yet"""
    n1 = generate_class_instance(CollectedNotification, seed=1, generate_relationships=True)
    n2 = generate_class_instance(CollectedNotification, seed=2, generate_relationships=True, optional_is_none=True)

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([n1, n2]).to_json()),
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)

        with pytest.raises(NotificationException):
            await collect_notifications_for_subscription(step_execution, execution_context, "sub1")


@pytest.mark.asyncio
async def test_collect_notifications_for_subscription_status_error(aiohttp_client, testing_contexts_factory):
    """Does collect_notifications_for_subscription fail gracefully if the HTTP response is an error"""

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.BAD_REQUEST, CollectEndpointResponse([]).to_json()),
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("abc-123", "foo")

        with pytest.raises(NotificationException):
            await collect_notifications_for_subscription(step_execution, execution_context, "sub1")


@pytest.mark.asyncio
async def test_collect_notifications_for_subscription_bad_response(aiohttp_client, testing_contexts_factory):
    """Does collect_notifications_for_subscription fail gracefully if the HTTP response is unparseable"""

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.GET,
                URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, "{ ]"),
                ],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("abc-123", "foo")

        with pytest.raises(NotificationException):
            await collect_notifications_for_subscription(step_execution, execution_context, "sub1")


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription(aiohttp_client, testing_contexts_factory, enabled):
    """Does update_notification_webhook_for_subscription transmit the request"""

    route = TestingAppRoute(
        HTTPMethod.PUT,
        URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    async with create_test_session(aiohttp_client, [route]) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("ABC123", "foo")

        await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=enabled)

    assert len(route.request_bodies) == 1
    assert str(enabled).lower() in route.request_bodies[0]


@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription_not_configured(aiohttp_client, testing_contexts_factory):
    """Does update_notification_webhook_for_subscription fail gracefully if the request hasn't configured a webhook
    yet"""

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.PUT,
                URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
                [RouteBehaviour(HTTPStatus.OK, "")],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=False)


@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription_status_error(aiohttp_client, testing_contexts_factory):
    """Does update_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.PUT,
                URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
                [RouteBehaviour(HTTPStatus.BAD_REQUEST, "")],
            )
        ],
    ) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)
        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("ABC123", "foo")

        with pytest.raises(NotificationException):
            await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=False)


@pytest.mark.asyncio
async def test_safely_delete_all_notification_webhooks(aiohttp_client, testing_contexts_factory):
    """Does safely_delete_all_notification_webhooks continue to perform deletes until all routes have been attempted"""
    route1 = TestingAppRoute(
        HTTPMethod.DELETE, URI_MANAGE_ENDPOINT.format(endpoint_id="abc123"), [RouteBehaviour(HTTPStatus.NOT_FOUND, "")]
    )
    route2 = TestingAppRoute(
        HTTPMethod.DELETE,
        URI_MANAGE_ENDPOINT.format(endpoint_id="def456"),
        [RouteBehaviour(HTTPStatus.INTERNAL_SERVER_ERROR, "")],
    )
    route3 = TestingAppRoute(
        HTTPMethod.DELETE,
        URI_MANAGE_ENDPOINT.format(endpoint_id="ghi789"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    async with create_test_session(aiohttp_client, [route1, route2, route3]) as session:
        (execution_context, step_execution) = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoint_by_sub_alias["sub1"] = CreateEndpointResponse("abc123", "foo")
        notification_context.endpoint_by_sub_alias["sub2"] = CreateEndpointResponse("def456", "foo")
        notification_context.endpoint_by_sub_alias["sub3"] = CreateEndpointResponse("ghi789", "foo")
        await safely_delete_all_notification_webhooks(notification_context)

    assert len(route1.behaviour) == 0, "All requests should've been consumed"
    assert len(route2.behaviour) == 0, "All requests should've been consumed"
    assert len(route3.behaviour) == 0, "All requests should've been consumed"
