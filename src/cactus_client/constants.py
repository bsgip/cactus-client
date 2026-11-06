from importlib.metadata import version

from cactus_test_definitions import CSIPAusVersion

CACTUS_CLIENT_VERSION = version("cactus-client")
CACTUS_TEST_DEFINITIONS_VERSION = version("cactus-test-definitions")
ENVOY_SCHEMA_VERSION = version("envoy-schema")

MIME_TYPE_SEP2 = "application/sep+xml"


# We will accept a "desync" in time up to this value
# This will need to compensate for transmission / processing time delays so we are being pretty generous
MAX_TIME_DRIFT_SECONDS = 5

# What is the CSIP-AUS version that this build is targetting
ACTIVE_CSIP_AUS_VERSION = CSIPAusVersion.RELEASE_1_3
