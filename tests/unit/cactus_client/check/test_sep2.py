import pytest

from cactus_client.check.sep2 import is_invalid_mrid


@pytest.mark.parametrize(
    "mrid, pen, expected_pass",
    [
        ("", 0, False),
        (None, 0, False),
        ("00004567", 4567, True),
        ("0ABCDEF0123456789000004567", 4567, True),
        ("FFFFFFFFFFFFFFFFFFFFFFFF00004567", 4567, True),
        ("FFFFFFFFFFFFFFFFFFFFF12399999999", 99999999, True),
        ("FFFFFFFFFFFFFFFFFFFFF12399999999", 999, False),  # wrong PEN
        ("BBAA402E1AD2D673BAE72163FE00000002", 2, False),  # Too long
        ("E1AD2D673BAE72163FE00000002", 2, False),  # Odd number of octets
        ("ffffffffffffffffffffffff00004567", 4567, False),  # Lowercase is not OK
    ],
)
def test_is_invalid_mrid(mrid: str | None, pen: int, expected_pass: bool):
    actual = is_invalid_mrid(mrid, pen)
    if expected_pass:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)
