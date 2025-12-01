import pytest

from cactus_client.cli.common import parse_bool


@pytest.mark.parametrize(
    "input, expected",
    [
        ("", ValueError),
        ("not a value", ValueError),
        ("123", ValueError),
        ("TrueFalse", ValueError),
        ("True", True),
        ("true", True),
        ("1", True),
        ("False", False),
        ("false", False),
        ("0", False),
    ],
)
def test_parse_bool(input: str, expected: bool | type[Exception]):
    if isinstance(expected, type):
        with pytest.raises(expected):
            parse_bool(input)
    else:
        assert parse_bool(input) is expected
