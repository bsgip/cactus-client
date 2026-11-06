from http import HTTPMethod, HTTPStatus
from typing import Any
from unittest import mock

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.csip_aus.connection_point import ConnectionPointResponse
from envoy_schema.server.schema.sep2.end_device import (
    EndDeviceListResponse,
    EndDeviceResponse,
)
from envoy_schema.server.schema.sep2.error import ErrorResponse
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorUsagePoint,
    MirrorUsagePointListResponse,
)
from multidict import CIMultiDict

from cactus_client.action.refresh_resource import (
    action_refresh_resource,
    generate_accept_versions,
)
from cactus_client.action.server import AcceptVersion
from cactus_client.error import CactusClientError, RequestError
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult
from cactus_client.model.http import ServerRequest, ServerResponse


def _generate_server_response(status: HTTPStatus) -> ServerResponse:
    """Generates a ServerResponse with the specified status"""
    return ServerResponse(
        url="/foobar",
        method="GET",
        status=status,
        body="",
        location=None,
        content_type=None,
        xsd_errors=None,
        headers=CIMultiDict(),
        request=generate_class_instance(ServerRequest),
    )


@pytest.mark.parametrize(
    "versions, quality_factors, expected",
    [
        (None, None, None),
        (None, [0.5], None),  # No versions means no overrides - quality factors have nothing to apply to
        ([], None, []),
        ([], [], []),
        (["v1.3"], None, [AcceptVersion("v1.3", None)]),
        (["v1.3", "v1.2"], None, [AcceptVersion("v1.3", None), AcceptVersion("v1.2", None)]),
        (["v1.3", "v1.2"], [0.9, 0.1], [AcceptVersion("v1.3", 0.9), AcceptVersion("v1.2", 0.1)]),
    ],
)
def test_generate_accept_versions(
    versions: list[str] | None, quality_factors: list[float] | None, expected: list[AcceptVersion] | None
):
    assert generate_accept_versions(versions, quality_factors) == expected


@pytest.mark.parametrize(
    "versions, quality_factors",
    [
        ([], [0.5]),
        (["v1.3"], []),
        (["v1.3"], [0.9, 0.1]),
        (["v1.3", "v1.2"], [0.9]),
    ],
)
def test_generate_accept_versions_mismatched_lengths(versions: list[str], quality_factors: list[float]):
    with pytest.raises(ValueError):
        generate_accept_versions(versions, quality_factors)


@pytest.mark.asyncio
async def test_action_refresh_resource_happy_path(testing_contexts_factory):

    # Arrange
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create multiple EndDevices in the store
    edev1 = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=60)
    edev2 = generate_class_instance(EndDeviceResponse, href="/edev/2", postRate=60)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev1)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev2)

    with mock.patch("cactus_client.action.refresh_resource.get_resource_for_step") as mock_get:
        # Create updated versions
        updated_edev1 = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=120)
        updated_edev2 = generate_class_instance(EndDeviceResponse, href="/edev/2", postRate=200)
        mock_get.side_effect = [updated_edev1, updated_edev2]

        resolved_params = {"resource": CSIPAusResource.EndDevice.value}

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Verify get_resource_for_step was called twice
        assert mock_get.call_count == 2

        # Check first call in detail
        first_call_args = mock_get.call_args_list[0]
        assert first_call_args[0][0] == EndDeviceResponse
        assert first_call_args[0][2] == context
        assert first_call_args[0][3] == "/edev/1"

        # Verify both resources were updated in the store
        stored_edevs = resource_store.get_for_type(CSIPAusResource.EndDevice)
        assert len(stored_edevs) == 2
        assert stored_edevs[0].resource.postRate == 120  # Updated value
        assert stored_edevs[1].resource.postRate == 200  # Updated value


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection(testing_contexts_factory):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing ConnectionPoint
    cp = generate_class_instance(ConnectionPointResponse, href="/edev/1/cp/1")
    resource_store.upsert_resource(CSIPAusResource.ConnectionPoint, None, cp)

    with mock.patch("cactus_client.action.refresh_resource.client_error_request_for_step") as mock_error:
        mock_error.return_value = mock.Mock()  # Return a mock error response

        resolved_params = {
            "resource": CSIPAusResource.ConnectionPoint.value,
            "expect_rejection": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Verify client_error_request_for_step was called
        mock_error.assert_called_once()
        call_args = mock_error.call_args
        assert call_args[0][2] == "/edev/1/cp/1"
        assert call_args[0][3] == HTTPMethod.GET


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_failure(
    testing_contexts_factory,
):

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing ConnectionPoint
    cp = generate_class_instance(ConnectionPointResponse, href="/edev/1/cp/1")
    resource_store.upsert_resource(CSIPAusResource.ConnectionPoint, None, cp)

    with mock.patch("cactus_client.action.refresh_resource.client_error_request_for_step") as mock_error:
        mock_error.side_effect = RequestError("mock exception abc")

        resolved_params = {
            "resource": CSIPAusResource.ConnectionPoint.value,
            "expect_rejection": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str) and "mock exception abc" in result.description

        # Verify client_error_request_for_step was called
        mock_error.assert_called_once()
        call_args = mock_error.call_args
        assert call_args[0][2] == "/edev/1/cp/1"
        assert call_args[0][3] == HTTPMethod.GET


@pytest.mark.parametrize(
    "list_resource, list_type",
    [
        (CSIPAusResource.EndDeviceList, EndDeviceListResponse),
        (CSIPAusResource.MirrorUsagePointList, MirrorUsagePointListResponse),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_list_success(
    testing_contexts_factory, list_resource: CSIPAusResource, list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty correctly for list types"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(list_type, href="/foobar")
    resource_store.upsert_resource(list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_or_empty_list_request_for_step"
    ) as mock_client_error_or_empty_list_request_for_step:
        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_or_empty_list_request_for_step.return_value = generate_class_instance(ErrorResponse)
        resolved_params = {
            "resource": list_resource.value,
            "expect_rejection_or_empty": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called first to check response
        mock_client_error_or_empty_list_request_for_step.assert_called_once_with(
            list_type, step, context, "/foobar", HTTPMethod.GET, accept_versions=None
        )


@pytest.mark.parametrize(
    "non_list_resource, non_list_type",
    [
        (CSIPAusResource.EndDevice, EndDeviceResponse),
        (CSIPAusResource.MirrorUsagePoint, MirrorUsagePoint),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_non_list_success(
    testing_contexts_factory, non_list_resource: CSIPAusResource, non_list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty correctly for NON list types"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(non_list_type, href="/foobar")
    resource_store.upsert_resource(non_list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_client_error_request_for_step:
        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_request_for_step.return_value = generate_class_instance(ErrorResponse)
        resolved_params = {
            "resource": non_list_resource.value,
            "expect_rejection_or_empty": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.done()

        # Check request_for_step was called first to check response
        mock_client_error_request_for_step.assert_called_once_with(
            step, context, "/foobar", HTTPMethod.GET, accept_versions=None
        )


@pytest.mark.parametrize(
    "list_resource, list_type",
    [
        (CSIPAusResource.EndDeviceList, EndDeviceListResponse),
        (CSIPAusResource.MirrorUsagePointList, MirrorUsagePointListResponse),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_list_failure(
    testing_contexts_factory, list_resource: CSIPAusResource, list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty raising an error correctly"""
    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(list_type, href="/foobar")
    resource_store.upsert_resource(list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_or_empty_list_request_for_step"
    ) as mock_client_error_or_empty_list_request_for_step:
        mock_client_error_or_empty_list_request_for_step.side_effect = RequestError("mock exception")
        resolved_params = {
            "resource": list_resource.value,
            "expect_rejection_or_empty": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str)

        # Check request_for_step was called first to check response
        mock_client_error_or_empty_list_request_for_step.assert_called_once_with(
            list_type, step, context, "/foobar", HTTPMethod.GET, accept_versions=None
        )


@pytest.mark.parametrize(
    "non_list_resource, non_list_type",
    [
        (CSIPAusResource.EndDevice, EndDeviceResponse),
        (CSIPAusResource.MirrorUsagePoint, MirrorUsagePoint),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_non_list_failure(
    testing_contexts_factory, non_list_resource: CSIPAusResource, non_list_type: type
):
    """Will action_refresh_resource handle expect_rejection_or_empty failures correctly for NON list types"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    # Create an existing EndDeviceList
    list_to_refresh = generate_class_instance(non_list_type, href="/foobar")
    resource_store.upsert_resource(non_list_resource, None, list_to_refresh)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_client_error_request_for_step:
        # Mock response indicating client error (therefore the action is receiving what is expected)
        mock_client_error_request_for_step.side_effect = RequestError("mock exception")
        resolved_params = {
            "resource": non_list_resource.value,
            "expect_rejection_or_empty": True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str)

        # Check request_for_step was called first to check response
        mock_client_error_request_for_step.assert_called_once_with(
            step, context, "/foobar", HTTPMethod.GET, accept_versions=None
        )


@pytest.mark.parametrize(
    "version_params, expected_accept_versions",
    [
        ({}, None),
        ({"version_value_overrides": ["v99"]}, [AcceptVersion("v99", None)]),
        (
            {"version_value_overrides": ["v99", "v1.3"], "version_qf_overrides": [0.8, 0.2]},
            [AcceptVersion("v99", 0.8), AcceptVersion("v1.3", 0.2)],
        ),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_success(
    testing_contexts_factory, version_params: dict[str, Any], expected_accept_versions: list[AcceptVersion] | None
):
    """Will action_refresh_resource accept a 406 for every matching resource (and leave the store untouched)"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev1 = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=60)
    edev2 = generate_class_instance(EndDeviceResponse, href="/edev/2", postRate=60)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev1)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev2)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step:
        mock_request_for_step.return_value = _generate_server_response(HTTPStatus.NOT_ACCEPTABLE)
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            "expect_not_acceptable": True,
        } | version_params

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        assert mock_request_for_step.call_args_list == [
            mock.call(step, context, "/edev/1", HTTPMethod.GET, accept_versions=expected_accept_versions),
            mock.call(step, context, "/edev/2", HTTPMethod.GET, accept_versions=expected_accept_versions),
        ]

        # The store should hold what it held before the request
        assert [sr.resource for sr in resource_store.get_for_type(CSIPAusResource.EndDevice)] == [edev1, edev2]


@pytest.mark.parametrize(
    "status",
    [
        HTTPStatus.OK,
        HTTPStatus.NO_CONTENT,
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.NOT_FOUND,
        HTTPStatus.INTERNAL_SERVER_ERROR,
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_wrong_status(testing_contexts_factory, status: HTTPStatus):
    """Will action_refresh_resource raise for anything that isn't a 406 (including other 4XX errors and successes)"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1")
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step:
        mock_request_for_step.return_value = _generate_server_response(status)
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            "expect_not_acceptable": True,
        }

        # Act / Assert
        with pytest.raises(CactusClientError, match=f"Received HTTP status {status.value} "):
            await action_refresh_resource(resolved_params, step, context)

        mock_request_for_step.assert_called_once_with(step, context, "/edev/1", HTTPMethod.GET, accept_versions=None)


@pytest.mark.parametrize(
    "expect_param",
    ["expect_not_acceptable", "expect_not_acceptable_or_v12"],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_request_error(testing_contexts_factory, expect_param: str):
    """Will action_refresh_resource bundle up a RequestError as a retryable failure in the not acceptable pathways"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1")
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step:
        mock_request_for_step.side_effect = RequestError("mock exception abc")
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            expect_param: True,
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert not result.completed
        assert result.description and isinstance(result.description, str) and "mock exception abc" in result.description

        mock_request_for_step.assert_called_once()


@pytest.mark.parametrize(
    "status, version_params, expected_accept_versions",
    [
        (HTTPStatus.NOT_ACCEPTABLE, {}, None),
        (HTTPStatus.OK, {}, None),
        (HTTPStatus.OK, {"version_value_overrides": ["v1.2"]}, [AcceptVersion("v1.2", None)]),
        (
            HTTPStatus.NOT_ACCEPTABLE,
            {"version_value_overrides": ["v1.2", "v99"], "version_qf_overrides": [0.5, 0.25]},
            [AcceptVersion("v1.2", 0.5), AcceptVersion("v99", 0.25)],
        ),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_or_v12_success(
    testing_contexts_factory,
    status: HTTPStatus,
    version_params: dict[str, Any],
    expected_accept_versions: list[AcceptVersion] | None,
):
    """Will action_refresh_resource accept a 406 or a success, validating against v1.2 and not touching the store"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=60)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step:
        mock_request_for_step.return_value = _generate_server_response(status)
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            "expect_not_acceptable_or_v12": True,
        } | version_params

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        mock_request_for_step.assert_called_once_with(
            step,
            context,
            "/edev/1",
            HTTPMethod.GET,
            accept_versions=expected_accept_versions,
            xml_validation_version=CSIPAusVersion.RELEASE_1_2,
        )

        # The body is not parsed - the store should hold what it held before the request
        assert [sr.resource for sr in resource_store.get_for_type(CSIPAusResource.EndDevice)] == [edev]


@pytest.mark.parametrize(
    "status",
    [
        HTTPStatus.FOUND,
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.NOT_FOUND,
        HTTPStatus.INTERNAL_SERVER_ERROR,
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_or_v12_wrong_status(
    testing_contexts_factory, status: HTTPStatus
):
    """Will action_refresh_resource raise for anything that isn't a 406 or a success"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1")
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step:
        mock_request_for_step.return_value = _generate_server_response(status)
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            "expect_not_acceptable_or_v12": True,
        }

        # Act / Assert
        with pytest.raises(CactusClientError, match=f"Received HTTP status {status.value} "):
            await action_refresh_resource(resolved_params, step, context)

        mock_request_for_step.assert_called_once()


@pytest.mark.parametrize(
    "extra_params, expected_xml_validation_kwargs",
    [
        ({"expect_not_acceptable": True, "expect_not_acceptable_or_v12": True}, {}),  # expect_not_acceptable is first
        ({"expect_not_acceptable": True, "expect_rejection": True}, {}),
        ({"expect_not_acceptable": True, "expect_rejection_or_empty": True}, {}),
        (
            {"expect_not_acceptable_or_v12": True, "expect_rejection": True},
            {"xml_validation_version": CSIPAusVersion.RELEASE_1_2},
        ),
        (
            {"expect_not_acceptable_or_v12": True, "expect_rejection_or_empty": True},
            {"xml_validation_version": CSIPAusVersion.RELEASE_1_2},
        ),
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_not_acceptable_precedence(
    testing_contexts_factory, extra_params: dict[str, Any], expected_xml_validation_kwargs: dict[str, Any]
):
    """The not acceptable pathways take precedence when multiple expect_* params are set"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1")
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with (
        mock.patch("cactus_client.action.refresh_resource.request_for_step") as mock_request_for_step,
        mock.patch("cactus_client.action.refresh_resource.client_error_request_for_step") as mock_client_error,
    ):
        mock_request_for_step.return_value = _generate_server_response(HTTPStatus.NOT_ACCEPTABLE)
        resolved_params = {"resource": CSIPAusResource.EndDevice.value} | extra_params

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        mock_request_for_step.assert_called_once_with(
            step, context, "/edev/1", HTTPMethod.GET, accept_versions=None, **expected_xml_validation_kwargs
        )
        mock_client_error.assert_not_called()


@pytest.mark.asyncio
async def test_action_refresh_resource_happy_path_accept_versions(testing_contexts_factory):
    """Are version overrides forwarded when doing a regular refresh?"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=60)
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch("cactus_client.action.refresh_resource.get_resource_for_step") as mock_get:
        mock_get.return_value = generate_class_instance(EndDeviceResponse, href="/edev/1", postRate=120)
        resolved_params = {
            "resource": CSIPAusResource.EndDevice.value,
            "version_value_overrides": ["v1.3", "v1.2"],
            "version_qf_overrides": [0.75, 0.5],
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        mock_get.assert_called_once_with(
            EndDeviceResponse,
            step,
            context,
            "/edev/1",
            accept_versions=[AcceptVersion("v1.3", 0.75), AcceptVersion("v1.2", 0.5)],
        )

        # This pathway should still be updating the store
        assert [sr.resource.postRate for sr in resource_store.get_for_type(CSIPAusResource.EndDevice)] == [120]


@pytest.mark.parametrize(
    "resolved_params",
    [
        {"resource": CSIPAusResource.EndDevice.value, "expect_rejection": True},
        {"resource": CSIPAusResource.EndDevice.value, "expect_rejection_or_empty": True},
    ],
)
@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_accept_versions(
    testing_contexts_factory, resolved_params: dict[str, Any]
):
    """Are version overrides forwarded when expecting a rejection on a NON list type?"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev = generate_class_instance(EndDeviceResponse, href="/edev/1")
    resource_store.upsert_resource(CSIPAusResource.EndDevice, None, edev)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_request_for_step"
    ) as mock_client_error_request_for_step:
        mock_client_error_request_for_step.return_value = generate_class_instance(ErrorResponse)

        # Act
        result = await action_refresh_resource(
            resolved_params | {"version_value_overrides": ["v99"], "version_qf_overrides": [0.5]}, step, context
        )

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        mock_client_error_request_for_step.assert_called_once_with(
            step, context, "/edev/1", HTTPMethod.GET, accept_versions=[AcceptVersion("v99", 0.5)]
        )


@pytest.mark.asyncio
async def test_action_refresh_resource_expect_rejection_or_empty_list_accept_versions(testing_contexts_factory):
    """Are version overrides forwarded when expecting a rejection or empty list on a list type?"""

    # Arrange
    context, step = testing_contexts_factory(mock.Mock())
    resource_store = context.discovered_resources(step)

    edev_list = generate_class_instance(EndDeviceListResponse, href="/edev")
    resource_store.upsert_resource(CSIPAusResource.EndDeviceList, None, edev_list)

    with mock.patch(
        "cactus_client.action.refresh_resource.client_error_or_empty_list_request_for_step"
    ) as mock_client_error_or_empty_list_request_for_step:
        mock_client_error_or_empty_list_request_for_step.return_value = generate_class_instance(ErrorResponse)
        resolved_params = {
            "resource": CSIPAusResource.EndDeviceList.value,
            "expect_rejection_or_empty": True,
            "version_value_overrides": ["v99"],
        }

        # Act
        result = await action_refresh_resource(resolved_params, step, context)

        # Assert
        assert isinstance(result, ActionResult)
        assert result.completed

        mock_client_error_or_empty_list_request_for_step.assert_called_once_with(
            EndDeviceListResponse,
            step,
            context,
            "/edev",
            HTTPMethod.GET,
            accept_versions=[AcceptVersion("v99", None)],
        )
