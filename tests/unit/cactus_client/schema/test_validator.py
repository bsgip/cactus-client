from random import randint

import pytest
from assertical.asserts.type import assert_list_type
from cactus_test_definitions import CSIPAusVersion

from cactus_client.schema.validator import (
    to_hex_binary,
    validate_well_known_json,
    validate_xml,
)


@pytest.mark.parametrize(
    "xml",
    [
        """
<DERControlList
    xmlns="urn:ieee:std:2030.5:ns"
    xmlns:csipaus="https://csipaus.org/ns/v1.3" all="2" href="/derp/0/derc" results="1">
    <DERControl replyTo="/rsp" responseRequired="03">
        <mRID>ABCDEF0123456789</mRID>
        <description>Example DERControl 1</description>
        <creationTime>1605621300</creationTime>
        <EventStatus>
            <currentStatus>0</currentStatus>
            <dateTime>1605621300</dateTime>
            <potentiallySuperseded>false</potentiallySuperseded>
        </EventStatus>
        <interval>
            <duration>86400</duration>
            <start>1605621600</start>
        </interval>
        <DERControlBase>
            <csipaus:opModImpLimW>
                <multiplier>0</multiplier>
                <value>20000</value>
            </csipaus:opModImpLimW>
            <csipaus:opModExpLimW>
                <multiplier>0</multiplier>
                <value>5000</value>
            </csipaus:opModExpLimW>
            <csipaus:opModGenLimW>
                <multiplier>0</multiplier>
                <value>5000</value>
            </csipaus:opModGenLimW>
            <csipaus:opModLoadLimW>
                <multiplier>0</multiplier>
                <value>20000</value>
            </csipaus:opModLoadLimW>
        </DERControlBase>
    </DERControl>
</DERControlList>""",
        """
<ConnectionPoint xmlns="https://csipaus.org/ns/v1.3">
    <connectionPointId>1234567890</connectionPointId>
</ConnectionPoint>""",
        """
<DERControlBase xmlns="urn:ieee:std:2030.5:ns" xmlns:csipaus="https://csipaus.org/ns/v1.3">
    <csipaus:opModImpLimW>
        <multiplier>0</multiplier>
        <value>20000</value>
    </csipaus:opModImpLimW>
</DERControlBase>""",
    ],
)
def test_validate_xml_valid_xml(xml):
    """Tests validate_xml against various valid CSIP-Aus XML snippets"""
    result = validate_xml(xml, version=CSIPAusVersion.RELEASE_1_3)
    assert isinstance(result, list)
    assert len(result) == 0, "\n".join(result)

    # If we change the version it should no longer be valid
    result = validate_xml(xml, version=CSIPAusVersion.RELEASE_1_2)
    assert isinstance(result, list)
    assert len(result) > 0, "\n".join(result)


@pytest.mark.parametrize(
    "xml",
    [
        "",
        "123451",
        '{"foo": 123}',
        '<ConnectionPoint xmlns="https://csipaus.org/ns/v1.3"><c',
    ],
)
def test_validate_xml_not_xml(xml):
    """Tests validate_xml can handle a variety of "not xml" strings and fail appropriately"""
    result = validate_xml(xml)
    assert_list_type(str, result, count=1)  # We expect exactly 1 error if the XML is bad


@pytest.mark.parametrize(
    "xml",
    [
        """
<ConnectionPoint xmlns="https://csipaus.org/ns/v1.3">
    <connectionPointId>1234567890</connectionPointId>
    <extraElement/>
</ConnectionPoint>
""",  # Extra elements
        """
<DERControlBase xmlns="urn:ieee:std:2030.5:ns" xmlns:csipaus="https://csipaus.org/ns/v1.3">
    <csipaus:opModImpLimW>
        <value>20000</value>
        <multiplier>0</multiplier>
    </csipaus:opModImpLimW>
</DERControlBase>""",  # Element ordering
    ],
)
def test_validate_xml_schema_invalid(xml):
    """Tests validate_xml can handle a variety of xml strings that fail schema validation"""
    result = validate_xml(xml)
    assert_list_type(str, result)
    assert len(result) > 0


@pytest.mark.parametrize(
    "json_body",
    [
        '{"supportedSchemaVersions": {"https://csipaus.org/ns/v1.3": {"dcap": ["/dcap"]}}}',
        '{"supportedSchemaVersions": {"v1": {"dcap": []}}}',  # No dcap entries is still schema valid
        '{"supportedSchemaVersions": {"v1": {"dcap": ["/dcap", "https://example.com/dcap"]}}}',
        """
{
    "supportedSchemaVersions": {
        "https://csipaus.org/ns/v1.2": {"dcap": ["/dcap"]},
        "https://csipaus.org/ns/v1.3": {"dcap": ["/v13/dcap"]}
    }
}""",
    ],
)
def test_validate_well_known_json_valid(json_body):
    """Tests validate_well_known_json accepts valid documents, supplied as either str or bytes"""
    assert validate_well_known_json(json_body) == []
    assert validate_well_known_json(json_body.encode()) == []


@pytest.mark.parametrize(
    "json_body",
    [
        "",
        "not json",
        "<xml/>",
        '{"supportedSchemaVersions": ',
        b"",
        b"\xff\xff\xff",  # Bytes that can't be decoded as UTF-8
    ],
)
def test_validate_well_known_json_not_json(json_body):
    """Tests validate_well_known_json can handle a variety of "not json" bodies and fail appropriately"""
    result = validate_well_known_json(json_body)
    assert_list_type(str, result, count=1)  # We expect exactly 1 error if the JSON is bad
    assert result[0].startswith("Invalid JSON")


@pytest.mark.parametrize(
    "json_body,expected_path",
    [
        ("null", "$"),  # Valid JSON but not an object
        ("[]", "$"),
        ("123", "$"),
        ("{}", "$"),  # Missing supportedSchemaVersions
        ('{"supportedSchemaVersions": {"v1": {"dcap": []}}, "extra": 1}', "$"),  # Extra top level property
        ('{"supportedSchemaVersions": []}', "$.supportedSchemaVersions"),  # Wrong type
        ('{"supportedSchemaVersions": {}}', "$.supportedSchemaVersions"),  # minProperties violated
        ('{"supportedSchemaVersions": {"v1": []}}', "$.supportedSchemaVersions.v1"),  # Wrong type
        ('{"supportedSchemaVersions": {"v1": {}}}', "$.supportedSchemaVersions.v1"),  # Missing dcap
        (
            '{"supportedSchemaVersions": {"v1": {"dcap": [], "extra": 1}}}',  # Extra version property
            "$.supportedSchemaVersions.v1",
        ),
        ('{"supportedSchemaVersions": {"v1": {"dcap": "/dcap"}}}', "$.supportedSchemaVersions.v1.dcap"),  # Not array
        ('{"supportedSchemaVersions": {"v1": {"dcap": [1]}}}', "$.supportedSchemaVersions.v1.dcap[0]"),  # Not string
    ],
)
def test_validate_well_known_json_schema_invalid(json_body, expected_path):
    """Tests validate_well_known_json reports a single error at the location of the schema violation"""
    result = validate_well_known_json(json_body)
    assert_list_type(str, result, count=1)
    assert result[0].startswith(f"{expected_path}: ")
    assert result[0] != f"{expected_path}: "  # There should be a descriptive message
    assert not result[0].startswith("Invalid JSON")

    # bytes input should behave identically
    assert validate_well_known_json(json_body.encode()) == result


def test_validate_well_known_json_multiple_errors_sorted():
    """Tests that every schema violation is reported, ordered by their location in the document"""
    json_body = """
{
    "supportedSchemaVersions": {
        "v2": {"dcap": [1, "/dcap", 2]},
        "v1": {}
    }
}"""
    result = validate_well_known_json(json_body)
    assert_list_type(str, result, count=3)
    assert result[0].startswith("$.supportedSchemaVersions.v1: ")
    assert result[1].startswith("$.supportedSchemaVersions.v2.dcap[0]: ")
    assert result[2].startswith("$.supportedSchemaVersions.v2.dcap[2]: ")


@pytest.mark.parametrize(
    "value,expected_length",
    [
        (0, 2),  # "00"
        (1, 2),  # "01"
        (15, 2),  # "0F"
        (255, 2),  # "FF"
        (256, 4),  # "0100"
        (2147483647, 8),  # "7FFFFFFF" - max signed 32-bit (8 chars)
        (4294967295, 8),  # "FFFFFFFF" - maximum 32-bit (8 chars)
        pytest.param(randint(1, 255), 2, id="random small"),
        pytest.param(randint(256, 65535), 4, id="random medium"),
        pytest.param(randint(65536, 16777215), 6, id="random large 3-byte values"),
        pytest.param(randint(16777216, 4294967295), 8, id="random large 4-byte values"),
    ],
)
def test_to_hex_binary(value, expected_length):
    result = to_hex_binary(value)

    # Check it's even
    assert len(result) % 2 == 0

    # Check length
    assert len(result) == expected_length

    # Check all characters are valid hex
    assert all(c in "0123456789ABCDEF" for c in result)

    # Verify it converts back to original value
    assert int(result, 16) == value
