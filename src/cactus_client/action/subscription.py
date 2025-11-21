import asyncio
from datetime import datetime
from http import HTTPMethod
from typing import Any, Callable, cast

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource
from envoy_schema.server.schema.sep2.der import (
    DERControlListResponse,
    DERListResponse,
    DERProgramListResponse,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.identification import Resource
from envoy_schema.server.schema.sep2.metering_mirror import MirrorUsagePointListResponse
from envoy_schema.server.schema.sep2.pub_sub import (
    Subscription,
    SubscriptionEncoding,
    SubscriptionListResponse,
)
from envoy_schema.server.schema.sep2.types import SubscribableType

from cactus_client.action.notifications import (
    fetch_notification_webhook_for_subscription,
)
from cactus_client.action.server import (
    delete_and_check_resource_for_step,
    get_resource_for_step,
    paginate_list_resource_items,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import RESOURCE_SEP2_TYPES, ResourceStore
from cactus_client.time import utc_now

VALID_SUBSCRIBABLE_VALUES = {
    SubscribableType.resource_supports_both_conditional_and_non_conditional_subscriptions,
    SubscribableType.resource_supports_non_conditional_subscriptions,
}

SUBSCRIPTION_LIMIT = 100


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

    # Figure out what webhook URI we can use for our subscription alias
    webhook_uri = await fetch_notification_webhook_for_subscription(step, context, sub_id)

    subscription_targets = store.get(resource)
    if len(subscription_targets) != 1:
        raise CactusClientException(
            f"Found {len(subscription_targets)} {resource} resource(s) but expected 1. Cannot create subscription."
        )
    target = subscription_targets[0]

    if target.resource.href is None:
        raise CactusClientException(f"Found {resource} with no href attribute encoded. Cannot subscribe to this.")

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
    matching_subs = [r for r in store.get(CSIPAusResource.Subscription) if r.alias == sub_id]
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
