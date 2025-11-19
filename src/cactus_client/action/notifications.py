import logging
from dataclasses import dataclass
from http import HTTPMethod

from aiohttp import ClientSession
from cactus_client_notifications.schema import (
    URI_MANAGE_ENDPOINT,
    URI_MANAGE_ENDPOINT_LIST,
    CollectedNotification,
    CollectEndpointResponse,
    ConfigureEndpointRequest,
    CreateEndpointResponse,
)

from cactus_client.error import NotificationException
from cactus_client.model.context import ExecutionContext, NotificationsContext
from cactus_client.model.execution import StepExecution

logger = logging.getLogger(__name__)

MIME_TYPE_JSON = "application/json"


@dataclass
class NotificationApiResponse:
    status: int
    body: str

    def is_success(self) -> bool:
        return self.status >= 200 and self.status <= 299


async def notifications_server_request(
    session: ClientSession,
    step: StepExecution,
    context: ExecutionContext,
    path: str,
    method: HTTPMethod,
    json_body: str | None = None,
) -> NotificationApiResponse:
    """Makes a request to the notification server (for the current context) - returns a raw response as string and
    logs the actions in the various context trackers. Raises a NotificationException on connection failure."""

    await context.progress.add_log(step, f"Requesting {method} {path}")

    headers = {"Accept": MIME_TYPE_JSON}
    if json_body is not None:
        headers["Content-Type"] = MIME_TYPE_JSON

    await context.progress.add_log(step, f"Contacting notification server: {method} {path}")
    try:
        async with session.request(method=method, url=path, data=json_body, headers=headers) as raw_response:
            return NotificationApiResponse(raw_response.status, await raw_response.text())
    except Exception as exc:
        logger.error(f"Exception requesting {method} {path} - '{json_body}'", exc_info=exc)
        raise NotificationException(f"Error requesting {method} {path} from notification server. {exc}")


async def fetch_notification_webhook_for_subscription(
    step: StepExecution, context: ExecutionContext, subscription_alias: str
) -> str:
    """Fetches the fully qualified webhook for notifications associated with subscription_alias. This will be cached
    for future calls.

    Will involve interacting with the remote notifications server.

    Can raise NotificationException"""

    notification_context = context.notifications_context(step)

    # If we have it in the cache - just grab it from there
    endpoint = notification_context.endpoint_by_sub_alias.get(subscription_alias, None)
    if endpoint is not None:
        return endpoint.fully_qualified_endpoint

    # otherwise we need to make an outgoing request for a new endpoint

    response = await notifications_server_request(
        notification_context.session, step, context, URI_MANAGE_ENDPOINT_LIST, HTTPMethod.POST, json_body=None
    )
    if not response.is_success():
        raise NotificationException(
            f"Creating a new notification webhook raised a HTTP {response.status}: {response.body}"
        )

    try:
        new_endpoint = CreateEndpointResponse.from_json(response.body)
        if isinstance(new_endpoint, list):
            raise Exception("Expected a singular response object. Received a list")
    except Exception as exc:
        logger.error(f"Exception parsing {response.body} into a CreateEndpointResponse", exc_info=exc)
        raise NotificationException("The CreateEndpointResponse from the notification server appears to be invalid.")

    logger.info(f"Created webhook {new_endpoint.fully_qualified_endpoint} for {subscription_alias}")
    notification_context.endpoint_by_sub_alias[subscription_alias] = new_endpoint
    return new_endpoint.fully_qualified_endpoint


async def update_notification_webhook_for_subscription(
    step: StepExecution, context: ExecutionContext, subscription_alias: str, enabled: bool
) -> None:
    """Updates the notification webhook for the specified subscription_alias. Requires a prior call to
    fetch_notification_webhook_for_subscription

    enabled: Whether the webhook should be enabled or not (disabled webhooks always serve HTTP errors)

    Will involve interacting with the remote notifications server.

    Can raise NotificationException"""

    notification_context = context.notifications_context(step)

    # Need to have an existing subscription
    endpoint = notification_context.endpoint_by_sub_alias.get(subscription_alias, None)
    if endpoint is None:
        raise NotificationException(f"No notification webhook has been created for {subscription_alias}.")

    response = await notifications_server_request(
        notification_context.session,
        step,
        context,
        URI_MANAGE_ENDPOINT.format(endpoint_id=endpoint.endpoint_id),
        HTTPMethod.PUT,
        json_body=ConfigureEndpointRequest(enabled=enabled).to_json(),
    )
    if not response.is_success():
        raise NotificationException(
            f"Updating a notification webhook {endpoint.fully_qualified_endpoint} to enabled={enabled}"
            + f" raised a HTTP {response.status}: {response.body}"
        )


async def collect_notifications_for_subscription(
    step: StepExecution, context: ExecutionContext, subscription_alias: str
) -> list[CollectedNotification]:
    """Fetches the current set of sep2 Notifications for subscription_alias. Requires a prior call to
    fetch_notification_webhook_for_subscription

    Will involve interacting with the remote notifications server.

    Can raise NotificationException"""

    notification_context = context.notifications_context(step)

    # Need to have an existing subscription
    endpoint = notification_context.endpoint_by_sub_alias.get(subscription_alias, None)
    if endpoint is None:
        raise NotificationException(f"No notification webhook has been created for {subscription_alias}.")

    response = await notifications_server_request(
        notification_context.session,
        step,
        context,
        URI_MANAGE_ENDPOINT.format(endpoint_id=endpoint.endpoint_id),
        HTTPMethod.GET,
        json_body=None,
    )
    if not response.is_success():
        raise NotificationException(
            f"Fetching notifications for {endpoint.fully_qualified_endpoint}"
            + f" raised a HTTP {response.status}: {response.body}"
        )

    try:
        collected_response = CollectEndpointResponse.from_json(response.body)
        if isinstance(collected_response, list):
            raise Exception("Expected a singular response object. Received a list")
    except Exception as exc:
        logger.error(f"Exception parsing {response.body} into a CollectEndpointResponse", exc_info=exc)
        raise NotificationException("The CollectEndpointResponse from the notification server appears to be invalid.")

    if collected_response.notifications is None:
        return []
    return collected_response.notifications


async def safely_delete_all_notification_webhooks(notification_context: NotificationsContext) -> None:
    """Enumerates all created notification webhooks  - attempting to delete them. Raises no exceptions on failure.

    Will involve interacting with the remote notifications server."""

    for endpoint in notification_context.endpoint_by_sub_alias.values():
        try:
            async with notification_context.session.request(
                method=HTTPMethod.DELETE, url=URI_MANAGE_ENDPOINT.format(endpoint_id=endpoint.endpoint_id)
            ) as raw_response:
                logger.info(
                    f"Deleting notification endpoint: {endpoint.endpoint_id} yielded a HTTP {raw_response.status}"
                )
        except Exception as exc:
            logger.info(f"Deleting notification endpoint: {endpoint.endpoint_id} yielded an error", exc_info=exc)
