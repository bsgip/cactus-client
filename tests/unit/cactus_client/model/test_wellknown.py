from cactus_client.model.wellknown import WellKnown, WellKnownSchemaVersion
from cactus_client.schema.validator import validate_well_known_json


def test_wellknown_from_json():
    """Tests that a schema valid well-known JSON document can be parsed into the expected models"""
    json_body = """
{
    "supportedSchemaVersions": {
        "https://csipaus.org/ns/v1.2": {"dcap": ["/dcap"]},
        "https://csipaus.org/ns/v1.3": {"dcap": ["/v13/dcap", "https://example.com/dcap"]}
    }
}"""
    assert validate_well_known_json(json_body) == [], "The test document should be schema valid"

    result = WellKnown.from_json(json_body)

    assert isinstance(result, WellKnown)
    assert result.supported_schema_versions == {
        "https://csipaus.org/ns/v1.2": WellKnownSchemaVersion(dcap=["/dcap"]),
        "https://csipaus.org/ns/v1.3": WellKnownSchemaVersion(dcap=["/v13/dcap", "https://example.com/dcap"]),
    }
