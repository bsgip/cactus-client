import re


def is_invalid_mrid(mrid: str | None, expected_pen: int) -> str | None:
    """Is this mrid well formed and has the CSIP-Aus requirements of a PEN encoded in the last 10 digits? Returns
    a short (human readable) descriptor of the error or returns None if valid."""

    if not mrid:
        return "No value was found (empty/None string)."

    if re.search(r"[^A-Z0-9]", mrid):
        return "Only uppercase, hexadecimal characters should be encoded."

    if len(mrid) > 32:
        return "Must be contain at most 32 characters."

    if (len(mrid) % 2) != 0:
        return "Must be an even number of hexadecimal characters."

    try:
        pen_from_mrid = int(mrid[-8:])
    except ValueError:
        return f"The last 8 digits don't encode a base10 integer '{mrid[-8:]}'"

    if pen_from_mrid != expected_pen:
        return f"Mismatch on PEN. Found {pen_from_mrid} but expected {expected_pen}"

    return None
