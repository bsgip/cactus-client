from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance, register_value_generator
from assertical.fixtures.generator import generator_registry_snapshot
from cactus_test_definitions.server.test_procedures import (
    Preconditions,
    RequiredClient,
    TestProcedure,
)

from cactus_client.model.config import ClientConfig
from cactus_client.model.context import ClientContext, ExecutionContext, ResourceStore
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)


@pytest.fixture
def dummy_client_alias_1():
    return "my-client-1"


@pytest.fixture
def assertical_extensions():
    with generator_registry_snapshot():
        register_value_generator(dict, lambda _: {})
        yield


@pytest.fixture
def dummy_test_procedure(dummy_client_alias_1, assertical_extensions) -> TestProcedure:
    return generate_class_instance(
        TestProcedure,
        optional_is_none=True,
        generate_relationships=True,
        preconditions=generate_class_instance(
            Preconditions,
            optional_is_none=True,
            required_clients=[generate_class_instance(RequiredClient, id=dummy_client_alias_1)],
        ),
    )


@pytest.fixture
def testing_contexts_factory(dummy_test_procedure) -> Callable[[ClientSession], tuple[ExecutionContext, StepExecution]]:
    """Returns a callable(session: ClientSession) that when executed will yield a tuple
    containing a fully populated ExecutionContext and StepExcecution"""

    def create_testing_contexts(client_session) -> tuple[ExecutionContext, StepExecution]:
        client_alias = dummy_test_procedure.preconditions.required_clients[0].id
        client_context = ClientContext(
            test_procedure_alias=client_alias,
            client_config=generate_class_instance(ClientConfig, optional_is_none=True),
            discovered_resources=ResourceStore(),
            session=client_session,
        )

        execution_context = ExecutionContext(
            dummy_test_procedure,
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

    return create_testing_contexts
