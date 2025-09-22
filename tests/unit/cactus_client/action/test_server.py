from dataclasses import dataclass
from http import HTTPMethod, HTTPStatus

import pytest
from aiohttp import web
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
)
from envoy_schema.server.schema.sep2.device_capability import DeviceCapabilityResponse

from cactus_client.action.server import get_identified_object_for_step
from cactus_client.constants import MIME_TYPE_SEP2
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


def create_testing_contexts(tp: TestProcedure, client_session) -> tuple[ExecutionContext, StepExecution]:

    client_alias = tp.preconditions.required_clients[0].id
    client_context = ClientContext(
        test_procedure_alias=client_alias,
        client_config=generate_class_instance(ClientConfig, optional_is_none=True),
        discovered_resources=ResourceStore(),
        client_session=client_session,
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
async def test_get_identified_object_for_step_success(aiohttp_client, dummy_test_procedure):

    client_session = aiohttp_client(
        create_test_app_for_route(HTTPMethod.GET, "/foo/bar", [RouteBehaviour.xml(HTTPStatus.OK, "dcap.xml")])
    )
    (execution_context, step_execution) = create_testing_contexts(dummy_test_procedure, client_session)

    result = await get_identified_object_for_step(
        DeviceCapabilityResponse, step_execution, execution_context, "/foo/bar"
    )

    # Assert - contents of response
    assert isinstance(result, DeviceCapabilityResponse)
    assert result.EndDeviceListLink.all_ == 2
    assert result.EndDeviceListLink.href == "/envoy-svc-static-36/edev"

    # Assert - no warnings raised
    assert len(execution_context.warnings.warnings) == 0
