import pytest
from assertical.fake.generator import generate_class_instance, register_value_generator
from assertical.fixtures.generator import generator_registry_snapshot
from cactus_test_definitions.server.test_procedures import (
    Preconditions,
    RequiredClient,
    Step,
    TestProcedure,
)


@pytest.fixture
def dummy_client_alias_1():
    return "my-client-1"


@pytest.fixture
def dummy_test_procedure(dummy_client_alias_1):

    with generator_registry_snapshot():
        register_value_generator(dict, lambda _: {})
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
