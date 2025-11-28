import unittest.mock as mock
from datetime import datetime, timezone
from http import HTTPMethod
from itertools import product

import pytest
from assertical.asserts.generator import assert_class_instance_equality
from assertical.fake.generator import generate_class_instance, register_value_generator
from assertical.fixtures.generator import generator_registry_snapshot
from cactus_client_notifications.schema import (
    CollectedHeader,
    CollectedNotification,
    CreateEndpointResponse,
)
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import (
    DERControlListResponse,
    DERControlResponse,
    DERProgramListResponse,
)
from envoy_schema.server.schema.sep2.end_device import EndDeviceListResponse
from envoy_schema.server.schema.sep2.function_set_assignments import (
    FunctionSetAssignmentsListResponse,
)
from envoy_schema.server.schema.sep2.pub_sub import (
    XSI_TYPE_DER_CONTROL_LIST,
    XSI_TYPE_DER_PROGRAM_LIST,
    XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST,
    Notification,
    NotificationResourceCombined,
    NotificationStatus,
    Subscription,
    SubscriptionListResponse,
)
from envoy_schema.server.schema.sep2.types import SubscribableType

from cactus_client.action.subscription import (
    RESOURCE_TYPE_BY_XSI,
    action_create_subscription,
    action_delete_subscription,
    action_notifications,
    collect_and_validate_notification,
    handle_notification_cancellation,
    handle_notification_resource,
    parse_combined_resource,
)
from cactus_client.constants import MIME_TYPE_SEP2
from cactus_client.error import CactusClientException
from cactus_client.model.context import (
    AnnotationNamespace,
    ExecutionContext,
    NotificationEndpoint,
    NotificationsContext,
)
from cactus_client.model.execution import ActionResult, StepExecution


@pytest.fixture
def assertical_all_hexbinary8():
    """Forces all strings to generate as a hexbinary8 (eg: 0A)"""
    with generator_registry_snapshot():
        register_value_generator(str, lambda x: f"{(x % 256):02X}")
        yield


@mock.patch("cactus_client.action.subscription.fetch_notification_webhook_for_subscription")
@mock.patch("cactus_client.action.subscription.submit_and_refetch_resource_for_step")
@pytest.mark.asyncio
async def test_action_create_subscription(
    mock_submit_and_refetch_resource_for_step: mock.MagicMock,
    mock_fetch_notification_webhook_for_subscription: mock.MagicMock,
    testing_contexts_factory,
):
    """Tests the happy path of creation - ensures that the resource store is properly updated"""
    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    resource = CSIPAusResource.DERProgramList
    sub_id = "MY sub id"

    sub_list_sr = store.append_resource(
        CSIPAusResource.SubscriptionList,
        None,
        generate_class_instance(SubscriptionListResponse, seed=101, href="/sublist"),
    )
    target_sr = store.append_resource(
        CSIPAusResource.DERProgramList,
        None,
        generate_class_instance(
            DERProgramListResponse,
            seed=202,
            href="/derplist",
            subscribable=SubscribableType.resource_supports_both_conditional_and_non_conditional_subscriptions,
        ),
    )
    # Add an unrelated subscription - should be left alone
    other_sub_sr = store.append_resource(
        CSIPAusResource.Subscription,
        sub_list_sr.id,
        generate_class_instance(Subscription, seed=303, href="/othersub"),
    )

    mock_fetch_notification_webhook_for_subscription.return_value = "https://fake.webhook/"

    refetched_subscription = generate_class_instance(Subscription, seed=404)
    mock_submit_and_refetch_resource_for_step.return_value = refetched_subscription

    # Act
    result = await action_create_subscription({"sub_id": sub_id, "resource": resource.name}, step, context)

    # Assert
    assert isinstance(result, ActionResult)
    assert result == ActionResult.done()

    fetched_subs = store.get_for_type(CSIPAusResource.Subscription)
    assert len(fetched_subs) == 2, "Should've only added a single new subscription"
    assert fetched_subs[0] is other_sub_sr
    new_sub_sr = fetched_subs[1]

    assert new_sub_sr.id.parent_id() == sub_list_sr.id
    assert context.resource_annotations(step, new_sub_sr.id).alias == sub_id
    assert new_sub_sr.resource is refetched_subscription

    mock_submit_and_refetch_resource_for_step.assert_has_calls(
        [mock.call(Subscription, step, context, HTTPMethod.POST, sub_list_sr.id.href(), mock.ANY)]
    )
    mock_fetch_notification_webhook_for_subscription.assert_has_calls(
        [mock.call(step, context, sub_id, resource, target_sr.id)]
    )

    assert len(context.warnings.warnings) == 0


@mock.patch("cactus_client.action.subscription.delete_and_check_resource_for_step")
@pytest.mark.asyncio
async def test_action_delete_subscription(
    mock_delete_and_check_resource_for_step: mock.MagicMock, testing_contexts_factory
):
    """Tests the happy path for deletion (and ensures the underlying resource store is updated)"""
    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    sub_id = "MY sub id 2"

    sub1_sr = store.append_resource(
        CSIPAusResource.Subscription, None, generate_class_instance(Subscription, seed=101, href="/othersub1")
    )
    context.resource_annotations(step, sub1_sr.id).alias = sub_id + "mismatch"
    sub2_sr = store.append_resource(
        CSIPAusResource.Subscription, None, generate_class_instance(Subscription, seed=202, href="/target")
    )
    context.resource_annotations(step, sub2_sr.id).alias = sub_id
    sub3_sr = store.append_resource(
        CSIPAusResource.Subscription, None, generate_class_instance(Subscription, seed=303, href="/othersub2")
    )

    # Act
    result = await action_delete_subscription({"sub_id": sub_id}, step, context)

    # Assert
    assert isinstance(result, ActionResult)
    assert result == ActionResult.done()

    mock_delete_and_check_resource_for_step.assert_has_calls([mock.call(step, context, sub2_sr.id.href())])

    assert store.get_for_id(sub1_sr.id) is sub1_sr
    assert store.get_for_id(sub2_sr.id) is None, "Should've been deleted"
    assert store.get_for_id(sub3_sr.id) is sub3_sr

    assert len(context.warnings.warnings) == 0


@pytest.mark.parametrize(
    "xsi_type, optional_is_none",
    product(RESOURCE_TYPE_BY_XSI.keys(), [True, False]),
)
def test_parse_combined_resource(xsi_type: str, optional_is_none: bool, assertical_all_hexbinary8):
    """This tries to stress test our conversion from NotificationResourceCombined to a specific type like DERControl"""

    # Start by generating our target type so we get the expected optional/mandatory params
    target_type = RESOURCE_TYPE_BY_XSI[xsi_type]
    source_values = generate_class_instance(target_type, optional_is_none=optional_is_none, generate_relationships=True)

    # Next - we bring those values across to a NotificationResourceCombined instance (so it looks 'real')
    source = NotificationResourceCombined(**source_values.__dict__)

    # Finally - do the test and see if the resulting object is of the right type and has pulled the right values
    actual = parse_combined_resource(xsi_type=xsi_type, resource=source)
    assert isinstance(actual, target_type)
    assert_class_instance_equality(target_type, source, actual)
    assert_class_instance_equality(target_type, source_values, actual)


@pytest.mark.parametrize(
    "xsi_type, source, expected",
    [
        (
            XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST,
            NotificationResourceCombined(
                type=XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST, pollRate=1234, all_=456, results=789
            ),
            FunctionSetAssignmentsListResponse(
                type=XSI_TYPE_FUNCTION_SET_ASSIGNMENTS_LIST, pollRate=1234, all_=456, results=789
            ),
        ),
        (
            XSI_TYPE_DER_PROGRAM_LIST,
            NotificationResourceCombined(type=XSI_TYPE_DER_PROGRAM_LIST, pollRate=1234, all_=456, results=789),
            DERProgramListResponse(type=XSI_TYPE_DER_PROGRAM_LIST, pollRate=1234, all_=456, results=789),
        ),
        (
            XSI_TYPE_DER_CONTROL_LIST,
            NotificationResourceCombined(type=XSI_TYPE_DER_CONTROL_LIST, pollRate=1234, all_=456, results=789),
            DERControlListResponse(type=XSI_TYPE_DER_CONTROL_LIST, pollRate=1234, all_=456, results=789),
        ),
    ],
)
def test_parse_combined_resource_edge_cases(xsi_type: str, source, expected):
    """This tries to hit the various edge cases that can come up with the NotificationResourceCombined conversions"""

    # Finally - do the test and see if the resulting object is of the right type and has pulled the right values
    actual = parse_combined_resource(xsi_type=xsi_type, resource=source)
    assert isinstance(actual, type(expected))
    assert_class_instance_equality(type(expected), source, expected)


@pytest.mark.parametrize("bad_type", [None, "", "DERControlButDNE"])
def test_parse_combined_resource_bad_type(bad_type):
    with pytest.raises(CactusClientException):
        parse_combined_resource(bad_type, generate_class_instance(NotificationResourceCombined))


@mock.patch("cactus_client.action.subscription.parse_combined_resource")
@pytest.mark.asyncio
async def test_handle_notification_resource(mock_parse_combined_resource: mock.MagicMock, testing_contexts_factory):

    # Arrange
    context: ExecutionContext
    step: StepExecution
    context, step = testing_contexts_factory(mock.Mock())
    store = context.discovered_resources(step)
    resource = CSIPAusResource.DERControlList
    sub_id = "MY sub id #2"

    # Setup the existing webhook info / resources
    derc_list_sr = store.append_resource(
        CSIPAusResource.DERControlList,
        None,
        generate_class_instance(DERControlListResponse, seed=101, href="/derclist"),
    )
    context.clients_by_alias[step.client_alias].notifications = NotificationsContext(
        mock.Mock(),  # We won't be using the session
        {
            sub_id: NotificationEndpoint(
                CreateEndpointResponse(endpoint_id="abc", fully_qualified_endpoint="https://fake.webhook/abc"),
                resource,
                derc_list_sr.id,
            )
        },
    )
    existing_derc_sr = store.append_resource(
        CSIPAusResource.DERControl,
        derc_list_sr.id,
        generate_class_instance(DERControlResponse, seed=202, href="/derc1"),
    )

    # This isn't a realistic Notification but we are mocking mock_parse_combined_resource so it's fine
    #
    # We will be returning an updated DERControlList with two new DERControls
    notification = generate_class_instance(Notification, seed=303, generate_relationships=True)
    notification_derc_list = generate_class_instance(DERControlListResponse, seed=404, href="/derclist")
    notification_derc_list.DERControl = [
        generate_class_instance(DERControlResponse, seed=505, href="/derc2"),
        generate_class_instance(DERControlResponse, seed=606, href="/derc3"),
    ]
    notification_derc_list.results = 2
    mock_parse_combined_resource.return_value = notification_derc_list

    # Act
    await handle_notification_resource(step, context, notification, sub_id)

    # Assert

    # Check DERControlList
    derc_lists = store.get_for_type(CSIPAusResource.DERControlList)
    assert len(derc_lists) == 1, "We updated the DERControlList"
    assert derc_lists[0].id == derc_list_sr.id, "No change in ID"
    assert derc_lists[0].resource is notification_derc_list, "The resource should now point to the new DERControlList"
    assert context.resource_annotations(step, derc_lists[0].id).has_tag(
        AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id
    )

    # Check DERControls - There is an existing DERControl that should've been left alone and two inserted controls
    dercs = store.get_for_type(CSIPAusResource.DERControl)
    assert len(dercs) == 3, "We added two DERControls to the existing store"
    assert dercs[0] is existing_derc_sr
    assert dercs[1].id.parent_id() == derc_list_sr.id
    assert dercs[2].id.parent_id() == derc_list_sr.id
    assert dercs[1].resource is notification_derc_list.DERControl[0]
    assert dercs[2].resource is notification_derc_list.DERControl[1]
    assert not context.resource_annotations(step, dercs[0].id).has_tag(
        AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id
    )
    assert context.resource_annotations(step, dercs[1].id).has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id)
    assert context.resource_annotations(step, dercs[2].id).has_tag(AnnotationNamespace.SUBSCRIPTION_RECEIVED, sub_id)

    mock_parse_combined_resource.assert_called_once_with(notification.resource.type, notification.resource)

    assert len(context.warnings.warnings) == 0


@pytest.mark.parametrize("has_resource", [True, False])
@pytest.mark.asyncio
async def test_handle_notification_cancellation(testing_contexts_factory, has_resource: bool):

    # Arrange
    context: ExecutionContext
    step: StepExecution
    context, step = testing_contexts_factory(mock.Mock())

    # Act
    notification = generate_class_instance(Notification, seed=303, generate_relationships=has_resource)
    await handle_notification_cancellation(step, context, notification)

    # Assert
    if has_resource:
        assert len(context.warnings.warnings) == 1
    else:
        assert len(context.warnings.warnings) == 0


@pytest.mark.parametrize("is_cancel", [True, False])
@mock.patch("cactus_client.action.subscription.handle_notification_resource")
@mock.patch("cactus_client.action.subscription.handle_notification_cancellation")
@pytest.mark.asyncio
async def test_collect_and_validate_notification(
    mock_handle_notification_cancellation: mock.MagicMock,
    mock_handle_notification_resource: mock.MagicMock,
    is_cancel: bool,
    testing_contexts_factory,
    assertical_all_hexbinary8,
):
    """Tests the happy path for validating an incoming Notification"""

    # Arrange
    context: ExecutionContext
    step: StepExecution
    context, step = testing_contexts_factory(mock.Mock())
    sub_id = "my SUB id"
    store = context.discovered_resources(step)

    # Setup the existing webhook info / resources
    edev_list_sr = store.append_resource(
        CSIPAusResource.EndDeviceList,
        None,
        generate_class_instance(EndDeviceListResponse, seed=101, href="/edev"),
    )
    context.clients_by_alias[step.client_alias].notifications = NotificationsContext(
        mock.Mock(),  # We won't be using the session
        {
            sub_id: NotificationEndpoint(
                CreateEndpointResponse(endpoint_id="abc", fully_qualified_endpoint="https://fake.webhook/abc"),
                CSIPAusResource.EndDeviceList,
                edev_list_sr.id,
            )
        },
    )

    notification = generate_class_instance(
        Notification,
        seed=202,
        generate_relationships=True,
        subscribedResource="/edev",
        subscriptionURI="/sub1",
        newResourceURI=None,
        status=NotificationStatus.SUBSCRIPTION_CANCELLED_NO_INFO if is_cancel else NotificationStatus.DEFAULT,
    )

    collected_notification = CollectedNotification(
        method="POST",
        headers=[CollectedHeader("Content-Type", MIME_TYPE_SEP2)],
        received_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        remote="127.0.0.1",
        body=notification.to_xml(),
    )

    # Act
    await collect_and_validate_notification(step, context, collected_notification, sub_id)

    # Assert
    assert len(context.warnings.warnings) == 0
    if is_cancel:
        mock_handle_notification_cancellation.assert_called_once_with(step, context, notification)
        mock_handle_notification_resource.assert_not_called()
    else:
        mock_handle_notification_cancellation.assert_not_called()
        mock_handle_notification_resource.assert_called_once_with(step, context, notification, sub_id)


@pytest.mark.parametrize("collect, disable", product([True, False], [True, False, None]))
@mock.patch("cactus_client.action.subscription.collect_and_validate_notification")
@mock.patch("cactus_client.action.subscription.update_notification_webhook_for_subscription")
@mock.patch("cactus_client.action.subscription.collect_notifications_for_subscription")
@pytest.mark.asyncio
async def test_action_notification(
    mock_collect_notifications_for_subscription: mock.MagicMock,
    mock_update_notification_webhook_for_subscription: mock.MagicMock,
    mock_collect_and_validate_notification: mock.MagicMock,
    collect: bool,
    disable: bool | None,
    testing_contexts_factory,
):
    """Tests the various wa"""
    # Arrange
    context: ExecutionContext
    step: StepExecution
    context, step = testing_contexts_factory(mock.Mock())
    sub_id = "my SUB id"

    resolved_params = {"sub_id": sub_id, "collect": collect}
    if disable is not None:
        resolved_params["disable"] = disable

    collected_notification1 = generate_class_instance(CollectedNotification, seed=101)
    collected_notification2 = generate_class_instance(CollectedNotification, seed=202)
    mock_collect_notifications_for_subscription.return_value = [collected_notification1, collected_notification2]

    # Act
    result = await action_notifications(resolved_params, step, context)

    # Assert
    assert isinstance(result, ActionResult)
    assert result == ActionResult.done()

    if collect:
        mock_collect_notifications_for_subscription.assert_called_once_with(step, context, sub_id)
        mock_collect_and_validate_notification.assert_has_calls(
            [
                mock.call(step, context, collected_notification1, sub_id),
                mock.call(step, context, collected_notification2, sub_id),
            ]
        )
    else:
        mock_collect_notifications_for_subscription.assert_not_called()
        mock_collect_and_validate_notification.assert_not_called()

    if disable is not None:
        mock_update_notification_webhook_for_subscription.assert_called_once_with(
            step, context, sub_id, enabled=not disable
        )
    else:
        mock_update_notification_webhook_for_subscription.assert_not_called()
