import pytest

from cactus_client.action.mup import value_to_sep2


@pytest.mark.parametrize(
    "v, pow10, expected",
    [
        (0, 0, 0),
        (10.3, 0, 10),
        (4731.3, 3, 4),
        (4731.3, -1, 47313),
        (4731.3, -2, 473130),
    ],
)
def test_value_to_sep2(v: float, pow10: int, expected: int):
    actual = value_to_sep2(v, pow10)
    assert isinstance(actual, int)
    assert actual == expected
    assert value_to_sep2(-v, pow10) == -expected
