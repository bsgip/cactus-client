from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from http import HTTPMethod, HTTPStatus
from typing import AsyncIterator

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient
from assertical.asserts.type import assert_list_type
from assertical.fake.generator import generate_class_instance
from cactus_schema.notification import (
    CollectedNotification,
    CollectEndpointResponse,
    CreateEndpointResponse,
    uri,
)
from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.action.notifications import (
    collect_notifications_for_subscription,
    fetch_notification_webhook_for_subscription,
    safely_delete_all_notification_webhooks,
    update_notification_webhook_for_subscription,
)
from cactus_client.error import NotificationException
from cactus_client.model.context import (
    ExecutionContext,
    NotificationsContext,
)
from cactus_client.model.http import NotificationEndpoint, SubscriptionNotification
from cactus_client.model.resource import StoredResourceId


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
    create_endpoint_1 = CreateEndpointResponse("abc123", "https://my.example:123/uri")
    create_endpoint_2 = CreateEndpointResponse("def456", "https://my.other.example:456/path")
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                uri.URI_MANAGE_ENDPOINT_LIST,
                [
                    RouteBehaviour(HTTPStatus.OK, create_endpoint_1.to_json()),
                    RouteBehaviour(HTTPStatus.OK, create_endpoint_2.to_json()),
                ],
            )
        ],
    ) as session:
        execution_context: ExecutionContext
        execution_context, step_execution = testing_contexts_factory(None, session)
        result1 = await fetch_notification_webhook_for_subscription(
            step_execution,
            execution_context,
            "sub123",
            CSIPAusResource.DER,
            StoredResourceId.from_parent(None, "/hrefa"),
        )
        result2 = await fetch_notification_webhook_for_subscription(
            step_execution,
            execution_context,
            "sub123",
            CSIPAusResource.DERCapability,
            StoredResourceId.from_parent(None, "/hrefa"),
        )
        result3 = await fetch_notification_webhook_for_subscription(
            step_execution,
            execution_context,
            "sub1234",
            CSIPAusResource.DERControl,
            StoredResourceId.from_parent(None, "/hrefc"),
        )

    # Assert - contents of response
    assert isinstance(result1, str) and isinstance(result2, str) and isinstance(result3, str)
    assert result1 == "https://my.example:123/uri"
    assert result2 == "https://my.example:123/uri", "The second request should come from the cache"
    assert result3 == "https://my.other.example:456/path"

    # Assert the notifications context is populated with what we expect
    assert execution_context.notifications_context(step_execution).endpoints_by_sub_alias["sub123"] == [
        NotificationEndpoint(create_endpoint_1, CSIPAusResource.DER, StoredResourceId.from_parent(None, "/hrefa"))
    ]
    assert execution_context.notifications_context(step_execution).endpoints_by_sub_alias["sub1234"] == [
        NotificationEndpoint(
            create_endpoint_2, CSIPAusResource.DERControl, StoredResourceId.from_parent(None, "/hrefc")
        )
    ]


@pytest.mark.asyncio
async def test_notifications_server_request_status_error(aiohttp_client, testing_contexts_factory):
    """Does fetch_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                uri.URI_MANAGE_ENDPOINT_LIST,
                [
                    RouteBehaviour(
                        HTTPStatus.BAD_REQUEST, CreateEndpointResponse("abc123", "https://my.example:123/uri").to_json()
                    )
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await fetch_notification_webhook_for_subscription(
                step_execution,
                execution_context,
                "sub123",
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            )


@pytest.mark.asyncio
async def test_notifications_server_request_parsing_error(aiohttp_client, testing_contexts_factory):
    """Does fetch_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""
    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.POST,
                uri.URI_MANAGE_ENDPOINT_LIST,
                [RouteBehaviour(HTTPStatus.OK, "{ }")],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await fetch_notification_webhook_for_subscription(
                step_execution,
                execution_context,
                "sub123",
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            )


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
                uri.URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse(expected).to_json()),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("abc-123", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            )
        ]

        result = await collect_notifications_for_subscription(step_execution, execution_context, "sub1")

    # Assert - contents of response
    assert_list_type(SubscriptionNotification, result, count=len(expected))
    assert [n.notification for n in result] == expected


@pytest.mark.asyncio
async def test_collect_notifications_for_subscription_multi(aiohttp_client, testing_contexts_factory):
    """Does collect_notifications_for_subscription handle combining multiple routes"""
    n1 = generate_class_instance(CollectedNotification, seed=1, generate_relationships=True)
    n2 = generate_class_instance(CollectedNotification, seed=2, generate_relationships=True)
    n3 = generate_class_instance(CollectedNotification, seed=3, generate_relationships=True)
    n4 = generate_class_instance(CollectedNotification, seed=4, generate_relationships=True)

    route1 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r1"),
        [
            RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([]).to_json()),
        ],
    )
    route2 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r2"),
        [
            RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([n1]).to_json()),
        ],
    )
    route3 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r3"),
        [
            RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([n2, n3]).to_json()),
        ],
    )
    route4 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r4"),
        [
            RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([]).to_json()),
        ],
    )
    route5 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r5"),
        [
            RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([n4]).to_json()),
        ],
    )

    endpoint1 = NotificationEndpoint(
        CreateEndpointResponse("r1", "foo"),
        CSIPAusResource.DERControl,
        StoredResourceId.from_parent(None, "/fake1"),
    )
    endpoint2 = NotificationEndpoint(
        CreateEndpointResponse("r2", "foo"),
        CSIPAusResource.DERControl,
        StoredResourceId.from_parent(None, "/fake2"),
    )
    endpoint3 = NotificationEndpoint(
        CreateEndpointResponse("r3", "foo"),
        CSIPAusResource.DERControl,
        StoredResourceId.from_parent(None, "/fake3"),
    )
    endpoint4 = NotificationEndpoint(
        CreateEndpointResponse("r4", "foo"),
        CSIPAusResource.DERControl,
        StoredResourceId.from_parent(None, "/fake4"),
    )
    endpoint5 = NotificationEndpoint(
        CreateEndpointResponse("r5", "foo"),
        CSIPAusResource.DERControl,
        StoredResourceId.from_parent(None, "/fake5"),
    )

    async with create_test_session(
        aiohttp_client,
        [route1, route2, route3, route4, route5],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [endpoint1, endpoint2, endpoint3, endpoint4, endpoint5]

        result = await collect_notifications_for_subscription(step_execution, execution_context, "sub1")

    # Assert - contents of response
    assert_list_type(SubscriptionNotification, result, count=4)
    assert [n.notification for n in result] == [n1, n2, n3, n4]
    assert [n.source for n in result] == [endpoint2, endpoint3, endpoint3, endpoint5]


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
                uri.URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, CollectEndpointResponse([n1, n2]).to_json()),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        with pytest.raises(NotificationException):
            await collect_notifications_for_subscription(step_execution, execution_context, "sub1")


@pytest.mark.asyncio
async def test_collect_notifications_for_subscription_status_error(aiohttp_client, testing_contexts_factory):
    """Does collect_notifications_for_subscription fail gracefully if the HTTP response is an error"""

    route1 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r1"),
        [
            RouteBehaviour(
                HTTPStatus.OK,
                CollectEndpointResponse(
                    [generate_class_instance(CollectedNotification, seed=1, generate_relationships=True)]
                ).to_json(),
            ),
        ],
    )
    route2 = TestingAppRoute(
        HTTPMethod.GET,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="r2"),
        [
            RouteBehaviour(HTTPStatus.BAD_REQUEST, CollectEndpointResponse([]).to_json()),
        ],
    )

    async with create_test_session(
        aiohttp_client,
        [route1, route2],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("r1", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            ),
            NotificationEndpoint(
                CreateEndpointResponse("r2", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            ),
        ]

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
                uri.URI_MANAGE_ENDPOINT.format(endpoint_id="abc-123"),
                [
                    RouteBehaviour(HTTPStatus.OK, "{ ]"),
                ],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("abc-123", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            )
        ]

        with pytest.raises(NotificationException):
            await collect_notifications_for_subscription(step_execution, execution_context, "sub1")


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription(aiohttp_client, testing_contexts_factory, enabled):
    """Does update_notification_webhook_for_subscription transmit the request"""

    route1 = TestingAppRoute(
        HTTPMethod.PUT,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    route2 = TestingAppRoute(
        HTTPMethod.PUT,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="DEF456"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    async with create_test_session(aiohttp_client, [route1, route2]) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)
        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("ABC123", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake1"),
            ),
            NotificationEndpoint(
                CreateEndpointResponse("DEF456", "bar"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake2"),
            ),
        ]

        await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=enabled)

    assert len(route1.request_bodies) == 1
    assert str(enabled).lower() in route1.request_bodies[0]

    assert len(route2.request_bodies) == 1
    assert str(enabled).lower() in route2.request_bodies[0]


@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription_not_configured(aiohttp_client, testing_contexts_factory):
    """Does update_notification_webhook_for_subscription fail gracefully if the request hasn't configured a webhook
    yet"""

    async with create_test_session(
        aiohttp_client,
        [
            TestingAppRoute(
                HTTPMethod.PUT,
                uri.URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
                [RouteBehaviour(HTTPStatus.OK, "")],
            )
        ],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)
        with pytest.raises(NotificationException):
            await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=False)


@pytest.mark.asyncio
async def test_update_notification_webhook_for_subscription_status_error(aiohttp_client, testing_contexts_factory):
    """Does update_notification_webhook_for_subscription handle the case where a HTTP status error is returned"""

    route1 = TestingAppRoute(
        HTTPMethod.PUT,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="ABC123"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    route2 = TestingAppRoute(
        HTTPMethod.PUT,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="DEF456"),
        [RouteBehaviour(HTTPStatus.BAD_REQUEST, "")],
    )
    async with create_test_session(
        aiohttp_client,
        [route1, route2],
    ) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)
        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("ABC123", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            ),
            NotificationEndpoint(
                CreateEndpointResponse("DEF456", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake"),
            ),
        ]

        with pytest.raises(NotificationException):
            await update_notification_webhook_for_subscription(step_execution, execution_context, "sub1", enabled=False)

    assert len(route1.request_bodies) == 1
    assert len(route2.request_bodies) == 1


@pytest.mark.asyncio
async def test_safely_delete_all_notification_webhooks(aiohttp_client, testing_contexts_factory):
    """Does safely_delete_all_notification_webhooks continue to perform deletes until all routes have been attempted"""
    route1 = TestingAppRoute(
        HTTPMethod.DELETE,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="abc123"),
        [RouteBehaviour(HTTPStatus.NOT_FOUND, "")],
    )
    route2 = TestingAppRoute(
        HTTPMethod.DELETE,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="def456"),
        [RouteBehaviour(HTTPStatus.INTERNAL_SERVER_ERROR, "")],
    )
    route3 = TestingAppRoute(
        HTTPMethod.DELETE,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="ghi789"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    route4 = TestingAppRoute(
        HTTPMethod.DELETE,
        uri.URI_MANAGE_ENDPOINT.format(endpoint_id="jkl111"),
        [RouteBehaviour(HTTPStatus.OK, "")],
    )
    async with create_test_session(aiohttp_client, [route1, route2, route3, route4]) as session:
        execution_context, step_execution = testing_contexts_factory(None, session)

        notification_context: NotificationsContext = execution_context.notifications_context(step_execution)
        notification_context.endpoints_by_sub_alias["sub1"] = [
            NotificationEndpoint(
                CreateEndpointResponse("abc123", "foo"),
                CSIPAusResource.DERControl,
                StoredResourceId.from_parent(None, "/fake1"),
            )
        ]
        notification_context.endpoints_by_sub_alias["sub2"] = [
            NotificationEndpoint(
                CreateEndpointResponse("def456", "foo"),
                CSIPAusResource.DERCapability,
                StoredResourceId.from_parent(None, "/fake2"),
            ),
            NotificationEndpoint(
                CreateEndpointResponse("jkl111", "foo"),
                CSIPAusResource.DERCapability,
                StoredResourceId.from_parent(None, "/fake2"),
            ),
        ]
        notification_context.endpoints_by_sub_alias["sub3"] = [
            NotificationEndpoint(
                CreateEndpointResponse("ghi789", "foo"),
                CSIPAusResource.DeviceCapability,
                StoredResourceId.from_parent(None, "/fake3"),
            )
        ]
        await safely_delete_all_notification_webhooks(notification_context)

    assert len(route1.behaviour) == 0, "All requests should've been consumed"
    assert len(route2.behaviour) == 0, "All requests should've been consumed"
    assert len(route3.behaviour) == 0, "All requests should've been consumed"
    assert len(route4.behaviour) == 0, "All requests should've been consumed"
