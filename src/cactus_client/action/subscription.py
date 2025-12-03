import logging
from http import HTTPMethod
from typing import Any, cast

from cactus_client_notifications.schema import CollectedNotification
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.der import (
    DefaultDERControl,
    DERAvailability,
    DERCapability,
    DERControlListResponse,
    DERProgramListResponse,
    DERSettings,
    DERStatus,
)
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
)
from envoy_schema.server.schema.sep2.identification import List as Sep2List
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering import ReadingListResponse
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
    Notification,
    NotificationResourceCombined,
    NotificationStatus,
    Subscription,
    SubscriptionEncoding,
)
from envoy_schema.server.schema.sep2.types import SubscribableType

from cactus_client.action.discovery import get_list_item_callback
from cactus_client.action.notifications import (
    collect_notifications_for_subscription,
    fetch_notification_webhook_for_subscription,
    update_notification_webhook_for_subscription,
)
from cactus_client.action.server import (
    delete_and_check_resource_for_step,
    submit_and_refetch_resource_for_step,
)
from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import CactusClientException
from cactus_client.model.context import (
    AnnotationNamespace,
    ExecutionContext,
)
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.http import NotificationEndpoint, NotificationRequest

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
    subscription_lists = store.get_for_type(CSIPAusResource.SubscriptionList)
    if len(subscription_lists) != 1:
        raise CactusClientException(
            f"Found {len(subscription_lists)} SubscriptionList resource(s) but expected 1. Cannot create subscription."
        )
    subscription_list_href = subscription_lists[0].resource.href
    if not subscription_list_href:
        raise CactusClientException(
            "SubscriptionList resource has no href attribute encoded. Cannot create subscription."
        )

    subscription_targets = store.get_for_type(resource)
    if len(subscription_targets) == 0:
        raise CactusClientException(
            f"Found no {resource} resource(s) but expected at least 1. Cannot create subscription."
        )

    # Create a subscription
    for target in subscription_targets:
        if target.resource.href is None:
            raise CactusClientException(f"Found {resource} with no href attribute encoded. Cannot subscribe to this.")

        # Figure out what webhook URI we can use for our subscription alias
        webhook_uri = await fetch_notification_webhook_for_subscription(
            step, context, sub_id, target.resource_type, target.id
        )

        # Check that the element is marked as subscribable
        subscribable: SubscribableType | None = getattr(target.resource, "subscribable", None)
        if subscribable not in VALID_SUBSCRIBABLE_VALUES:
            context.warnings.log_step_warning(
                step,
                f"{resource} {target.resource.href} does not have the 'subscribable' attribute set to a value that"
                + " indicates support for a non conditional subscription.",
            )

        # Submit the subscription - ensure it's annotated correctly
        subscription = Subscription(
            encoding=SubscriptionEncoding.XML,
            level="+S1",
            limit=SUBSCRIPTION_LIMIT,
            notificationURI=webhook_uri,
            subscribedResource=target.resource.href,
        )
        returned_subscription = await submit_and_refetch_resource_for_step(
            Subscription, step, context, HTTPMethod.POST, subscription_list_href, subscription
        )
        sub_sr = store.upsert_resource(CSIPAusResource.Subscription, subscription_lists[0].id, returned_subscription)
        context.resource_annotations(step, sub_sr.id).alias = sub_id

    return ActionResult.done()


async def action_delete_subscription(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    sub_id: str = resolved_parameters["sub_id"]  # Mandatory param

    store = context.discovered_resources(step)

    # Figure out what webhook URI we can use for our subscription alias
    matching_subs = [
        r
        for r in store.get_for_type(CSIPAusResource.Subscription)
        if context.resource_annotations(step, r.id).alias == sub_id
    ]
    if len(matching_subs) == 0:
        raise CactusClientException(
            f"Found no Subscription resource(s) with alias {sub_id} but expected at least 1. Cannot delete."
        )

    for target in matching_subs:
        if target.resource.href is None:
            raise CactusClientException("Found Subscription with no href attribute encoded. Cannot delete this.")

        await delete_and_check_resource_for_step(step, context, target.resource.href)
        store.delete_resource(target.id)

    return ActionResult.done()


def parse_combined_resource(xsi_type: str, resource: NotificationResourceCombined) -> Resource:
    """Generates a properly typed instance of xsi_type based on the combined NotificationResourceCombined input.

    eg - Maps NotificationResourceCombined to a properly typed DERControlList with the same values."""
    target_type = RESOURCE_TYPE_BY_XSI.get(xsi_type)
    if target_type is None:
        raise CactusClientException(f"Received unrecognised resource xsi_type '{xsi_type}'. Expected {VALID_XSI_TYPES}")

    return target_type.model_validate(resource.__dict__)


async def handle_notification_resource(
    step: StepExecution,
    context: ExecutionContext,
    notification: Notification,
    sub_id: str,
    source: NotificationEndpoint,
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

    # Add this new notification contents to the store - this can be done via direct upsert
    upserted_resource = store.upsert_resource(
        source.subscribed_resource_type, source.subscribed_resource_id.parent_id(), parsed_resource
    )
    context.resource_annotations(step, upserted_resource.id).add_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id)

    # The upserted item might also be a list - in which case we will need to insert any included list items to the
    # store as well
    if is_list_resource(upserted_resource.resource_type):
        get_list_items, list_item_type = get_list_item_callback(upserted_resource.resource_type)
        list_items = get_list_items(parsed_resource) or []
        for child_list_item in list_items:
            child_upserted_resource = store.upsert_resource(list_item_type, upserted_resource.id, child_list_item)
            context.resource_annotations(step, child_upserted_resource.id).add_tag(
                AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id
            )

        total_results = cast(Sep2List, upserted_resource.resource).results
        if len(list_items) != cast(Sep2List, upserted_resource.resource).results:
            context.warnings.log_step_warning(
                step,
                f"Notification for {upserted_resource.resource_type} returned a List with results={total_results} but "
                + f"{len(list_items)} items in the list",
            )


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
    step: StepExecution,
    context: ExecutionContext,
    source: NotificationEndpoint,
    collected_notification: CollectedNotification,
    sub_id: str,
) -> None:
    """Takes a CollectedNotification and parses into a NotificationRequest (for logging) and decomposes a Notification
    from it in order to add things to the Resource store"""

    notification = NotificationRequest.from_collected_notification(source, collected_notification, sub_id)
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
    if sep2_notification.subscribedResource != source.subscribed_resource_id.href():
        context.warnings.log_step_warning(
            step,
            f"Notification <subscribedResource> has value {sep2_notification.subscribedResource}"
            + f" but expected {source.subscribed_resource_id.href()} as per initial Subscription.",
        )

    if sep2_notification.status == NotificationStatus.DEFAULT:
        await handle_notification_resource(step, context, sep2_notification, sub_id, source)
    else:
        await handle_notification_cancellation(step, context, sep2_notification)


async def action_notifications(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    sub_id: str = resolved_parameters["sub_id"]  # Mandatory param
    collect: bool = resolved_parameters.get("collect", False)
    disable: bool | None = resolved_parameters.get("disable", None)

    if collect:
        for n in await collect_notifications_for_subscription(step, context, sub_id):
            await collect_and_validate_notification(step, context, n.source, n.notification, sub_id)

    if disable is not None:
        await update_notification_webhook_for_subscription(step, context, sub_id, enabled=not disable)

    return ActionResult.done()
