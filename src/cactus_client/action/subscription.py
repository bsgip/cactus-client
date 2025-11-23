import asyncio
import logging
from datetime import datetime
from http import HTTPMethod
from typing import Any, Callable, cast

from cactus_client_notifications.schema import CollectedNotification
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.der import (
    DefaultDERControl,
    DERAvailability,
    DERCapability,
    DERControlListResponse,
    DERControlResponse,
    DERListResponse,
    DERProgramListResponse,
    DERSettings,
    DERStatus,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
)
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering import ReadingListResponse
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePointListResponse
from envoy_schema.server.schema.sep2.pub_sub import (
    XSI_TYPE_DEFAULT_DER_CONTROL,
    XSI_TYPE_DER_AVAILABILITY,
    XSI_TYPE_DER_CAPABILITY,
    XSI_TYPE_DER_CONTROL_LIST,
    XSI_TYPE_DER_PROGRAM_LIST,
    XSI_TYPE_DER_SETTINGS,
    XSI_TYPE_DER_STATUS,
    XSI_TYPE_END_DEVICE_LIST,
    XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST,
    XSI_TYPE_READING_LIST,
    XSI_TYPE_RESOURCE,
    XSI_TYPE_TIME_TARIFF_INTERVAL_LIST,
    Notification,
    NotificationResourceCombined,
    NotificationStatus,
    Subscription,
    SubscriptionEncoding,
    SubscriptionListResponse,
)
from envoy_schema.server.schema.sep2.types import SubscribableType

from cactus_client.action.notifications import (
    collect_notifications_for_subscription,
    fetch_notification_webhook_for_subscription,
    update_notification_webhook_for_subscription,
)
from cactus_client.action.server import (
    delete_and_check_resource_for_step,
    get_resource_for_step,
    paginate_list_resource_items,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.http import NotificationRequest
from cactus_client.model.resource import RESOURCE_SEP2_TYPES, ResourceStore
from cactus_client.schema.validator import validate_xml
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)

VALID_SUBSCRIBABLE_VALUES = {
    SubscribableType.resource_supports_both_conditional_and_non_conditional_subscriptions,
    SubscribableType.resource_supports_non_conditional_subscriptions,
}

SUBSCRIPTION_LIMIT = 100


RESOURCE_TYPE_BY_XSI: dict[str, type[Resource]] = {
    XSI_TYPE_DEFAULT_DER_CONTROL: DefaultDERControl,
    XSI_TYPE_DER_AVAILABILITY: DERAvailability,
    XSI_TYPE_DER_CAPABILITY: DERCapability,
    XSI_TYPE_DER_CONTROL_LIST: DERControlListResponse,
    XSI_TYPE_DER_PROGRAM_LIST: DERProgramListResponse,
    XSI_TYPE_DER_SETTINGS: DERSettings,
    XSI_TYPE_DER_STATUS: DERStatus,
    XSI_TYPE_END_DEVICE_LIST: EndDeviceListResponse,
    XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST: FunctionSetAssignmentsListResponse,
    XSI_TYPE_READING_LIST: ReadingListResponse,
}
VALID_XSI_TYPES: set[str] = set(RESOURCE_TYPE_BY_XSI.keys())


async def action_create_subscription(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    sub_id: str = resolved_parameters["sub_id"]  # Mandatory param
    resource = CSIPAusResource(resolved_parameters["resource"])  # Mandatory param

    store = context.discovered_resources(step)

    # Find the subscription list to receive this new subscription
    subscription_lists = store.get(CSIPAusResource.SubscriptionList)
    if len(subscription_lists) != 1:
        raise CactusClientException(
            f"Found {len(subscription_lists)} SubscriptionList resource(s) but expected 1. Cannot create subscription."
        )
    subscription_list_href = subscription_lists[0].resource.href
    if not subscription_list_href:
        raise CactusClientException(
            "SubscriptionList resource has no href attribute encoded. Cannot create subscription."
        )

    subscription_targets = store.get(resource)
    if len(subscription_targets) != 1:
        raise CactusClientException(
            f"Found {len(subscription_targets)} {resource} resource(s) but expected 1. Cannot create subscription."
        )
    target = subscription_targets[0]

    if target.resource.href is None:
        raise CactusClientException(f"Found {resource} with no href attribute encoded. Cannot subscribe to this.")

    # Figure out what webhook URI we can use for our subscription alias
    webhook_uri = await fetch_notification_webhook_for_subscription(step, context, sub_id, target.resource.href)

    # Check that the element is marked as subscribable
    subscribable: SubscribableType | None = getattr(target.resource, "subscribable", None)
    if subscribable not in VALID_SUBSCRIBABLE_VALUES:
        context.warnings.log_step_warning(
            step,
            f"{resource} {target.resource.href} does not have the 'subscribable' attribute set to a value that"
            + " indicates support for a non conditional subscription.",
        )

    # Submit the subscription
    subscription = Subscription(
        encoding=SubscriptionEncoding.XML,
        level="+S1",
        limit=SUBSCRIPTION_LIMIT,
        notificationURI=webhook_uri,
        subscribedResource=target.resource.href,
    )
    returned_subscription = await submit_and_refetch_resource_for_step(
        Subscription, step, context, HTTPMethod.POST, subscription_list_href, resource_to_sep2_xml(subscription)
    )
    store.upsert_resource(CSIPAusResource.Subscription, subscription_lists[0], returned_subscription, alias=sub_id)

    return ActionResult.done()


async def action_delete_subscription(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    sub_id: str = resolved_parameters["sub_id"]  # Mandatory param

    store = context.discovered_resources(step)

    # Figure out what webhook URI we can use for our subscription alias
    matching_subs = [r for r in store.get(CSIPAusResource.Subscription) if r.annotations.alias == sub_id]
    if len(matching_subs) != 1:
        raise CactusClientException(
            f"Found {len(matching_subs)} Subscription resource(s) with alias {sub_id} but expected 1. Cannot delete."
        )
    target = matching_subs[0]
    if target.resource.href is None:
        raise CactusClientException("Found Subscription with no href attribute encoded. Cannot delete this.")

    await delete_and_check_resource_for_step(step, context, target.resource.href)
    store.delete_resource(target)

    return ActionResult.done()


def parse_combined_resource(xsi_type: str, resource: NotificationResourceCombined) -> Resource:
    """Generates a properly typed instance of xsi_type based on the combined NotificationResourceCombined input.

    eg - Maps NotificationResourceCombined to a properly typed DERControlList with the same values."""
    target_type = RESOURCE_TYPE_BY_XSI.get(xsi_type)
    if target_type is None:
        raise CactusClientException(f"Received unrecognised resource xsi_type '{xsi_type}'. Expected {VALID_XSI_TYPES}")

    return target_type.model_validate(resource.__dict__)


async def handle_notification_resource(
    step: StepExecution, context: ExecutionContext, notification: Notification, sub_id: str
) -> None:
    """Takes a raw sep2 Notification and extracts any contents before injecting it into the current context's
    resource store."""

    # This might be controversial but ideally the server SHOULD be sending the contents of the Notification
    # in order to prevent a flood of client's resolving the returned href.
    #
    # This could be subject to change if vendors/clients agree.
    if notification.resource is None:
        raise CactusClientException("Received a (non cancellation) Notification with no <resource> element.")

    xsi_type: str | None = notification.resource.type
    if xsi_type is None:
        raise CactusClientException("Received a Notification.resource with a missing xsi:type attribute.")

    # Turn the resource into a fully fledged Resource (eg: a DERControl or EndDeviceList)
    logger.info(f"Handling a '{xsi_type}' Notification for {notification.subscribedResource}")
    parsed_resource = parse_combined_resource(xsi_type, notification.resource)

    store = context.discovered_resources(step)
    endpoint = context.notifications_context(step).endpoint_by_sub_alias.get(sub_id)
    if endpoint is None:
        raise CactusClientException(f"There is no subscription endpoint for {sub_id}. Has a subscription been created?")

    # Find the parent resource to nest this new resource under
    parent_resource_type = context.resource_tree.parent_resource(endpoint.subscribed_resource)
    raise NotImplementedError()

    # parent_resource = endpoint.subscribed_resource
    # parent_href = endpoint.subscribed_resource_href
    # matching_parents = [sr for sr in store.get(parent_resource) if sr.resource.href == parent_href]
    # if len(matching_parents) != 1:
    #     raise CactusClientException(
    #         f"Found {len(matching_parents)} {parent_resource} resource(s) with href {parent_href}. Expected 1."
    #     )
    # parent = matching_parents[0]

    # store.upsert_resource(endpoint.subscribed_resource,)


async def handle_notification_cancellation(
    step: StepExecution, context: ExecutionContext, notification: Notification
) -> None:
    """Takes a raw sep2 Notification and extracts any contents before injecting it into the current context"""
    logger.info(f"Handling a cancellation ({notification.status}) Notification for {notification.subscribedResource}")

    if notification.resource:
        context.warnings.log_step_warning(
            step,
            f"Received a cancellation Notification with a resource '{notification.resource.type}' (nonsensical).",
        )

    # Nothing else to do


async def collect_and_validate_notification(
    step: StepExecution, context: ExecutionContext, collected_notification: CollectedNotification, sub_id: str
) -> None:
    """Takes a CollectedNotification and parses into a NotificationRequest (for logging) and decomposes a Notification
    from it in order to add things to the Resource store"""

    notification_context = context.notifications_context(step)
    endpoint = notification_context.endpoint_by_sub_alias.get(sub_id)
    if endpoint is None:
        raise CactusClientException(f"There is no subscription endpoint for {sub_id}. Has a subscription been created?")

    notification = NotificationRequest.from_collected_notification(collected_notification, sub_id)
    await context.responses.log_notification_body(notification)

    if notification.method != "POST":
        context.warnings.log_step_warning(
            step, f"Received a HTTP {notification.method} at the notification webhook. Only POST will be accepted."
        )
        return

    if not notification.body:
        context.warnings.log_step_warning(
            step, f"Received a HTTP {notification.method} at the notification webhook but it had no body."
        )
        return

    # Having a borked Content-Type is worth raising a warning but not worth stopping the test
    if notification.content_type != MIME_TYPE_SEP2:
        context.warnings.log_step_warning(
            step, f"Expected header Content-Type: {MIME_TYPE_SEP2} but got '{notification.content_type}'"
        )

    try:
        sep2_notification = Notification.from_xml(notification.body)
    except Exception as exc:
        logger.error("Error parsing sep2 Notification from notification body", exc_info=exc)
        raise CactusClientException(
            "Error parsing sep2 Notification from notification body. This is likely a malformed response."
        )

    # Now start inspecting the returned Notification
    context.discovered_resources(step).append_resource(
        CSIPAusResource.Notification, None, sep2_notification, alias=sub_id
    )
    if sep2_notification.subscribedResource != endpoint.subscribed_resource_href:
        context.warnings.log_step_warning(
            step,
            f"Notification <subscribedResource> has value {sep2_notification.subscribedResource}"
            + f" but expected {endpoint.subscribed_resource_href} as per initial Subscription.",
        )

    if sep2_notification.status == NotificationStatus.DEFAULT:
        await handle_notification_resource(step, context, sep2_notification, sub_id)
    else:
        await handle_notification_cancellation(step, context, sep2_notification)


# notifications	collect: bool disable: bool	If collect, consumes subscription notifications and inserts them into the current context, if disable causes the subscription notification webhook to simulate an outage (return HTTP 5XX)
async def action_notification(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    sub_id: str = resolved_parameters["sub_id"]  # Mandatory param
    collect: bool = resolved_parameters.get("collect", False)
    disable: bool | None = resolved_parameters.get("disable", None)

    if collect:
        store = context.discovered_resources(step)
        for collected_notification in await collect_notifications_for_subscription(step, context, sub_id):
            await collect_and_validate_notification(step, context, collected_notification, sub_id)

    if disable is not None:
        await update_notification_webhook_for_subscription(step, context, sub_id, enabled=not disable)

    return ActionResult.done()
