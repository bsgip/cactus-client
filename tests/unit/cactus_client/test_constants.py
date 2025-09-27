from cactus_client.constants import (
    CACTUS_CLIENT_VERSION,
    CACTUS_TEST_DEFINITIONS_VERSION,
    ENVOY_SCHEMA_VERSION,
)


def test_versions_not_empty():

    assert CACTUS_CLIENT_VERSION and isinstance(CACTUS_CLIENT_VERSION, str)
    assert CACTUS_TEST_DEFINITIONS_VERSION and isinstance(CACTUS_TEST_DEFINITIONS_VERSION, str)
    assert ENVOY_SCHEMA_VERSION and isinstance(ENVOY_SCHEMA_VERSION, str)
