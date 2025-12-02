import hashlib
from typing import TypeVar

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from envoy_schema.server.schema.sep2.identification import Resource

from cactus_client.constants import MAX_TIME_DRIFT_SECONDS


def sum_digits(n: int) -> int:
    """Sums all base10 digits in n and returns the results.
    Eg:
    11 -> 2
    456 -> 15"""
    n = abs(n)
    s = 0
    while n:
        s += n % 10
        n //= 10
    return s


def convert_lfdi_to_sfdi(lfdi: str) -> int:
    """This function generates the 2030.5-2018 sFDI (Short-form device identifier) from a
    2030.5-2018 lFDI (Long-form device identifier). More details on the sFDI can be found in
    section 6.3.3 of the IEEE Std 2030.5-2018.

    To generate the sFDI from the lFDI the following steps are performed:
        1- Left truncate the lFDI to 36 bits.
        2- From the result of Step (1), calculate a sum-of-digits checksum digit.
        3- Right concatenate the checksum digit to the result of Step (1).

    Args:
        lfdi: The 2030.5-2018 lFDI as string of 40 hex characters (eg '18aff1802d ... 12d')

    Return:
        The sFDI as integer.
    """
    if len(lfdi) != 40:
        raise ValueError(f"lfdi should be 40 hex characters. Received {len(lfdi)} chars")

    raw_sfdi = int(("0x" + lfdi[:9]), 16)
    sfdi_checksum = (10 - (sum_digits(raw_sfdi) % 10)) % 10
    return raw_sfdi * 10 + sfdi_checksum


def lfdi_from_cert_file(cert_file: str) -> str:
    with open(cert_file, "rb") as f:
        pem_data = f.read()
    cert = x509.load_pem_x509_certificate(pem_data)
    der_bytes = cert.public_bytes(serialization.Encoding.DER)

    # Compute SHA-256 hash
    sha256_hash = hashlib.sha256(der_bytes).hexdigest()
    return sha256_hash[:40].upper()


def hex_binary_equal(a: int | str | None, b: int | str | None) -> bool:
    """Returns true if two values are equivalent (regardless of a potential encoding to HexBinary or integer)"""
    if a is None or b is None:
        return a == b

    if isinstance(a, str):
        a = int(a, 16)
    if isinstance(b, str):
        b = int(b, 16)
    return a == b


AnyResource = TypeVar("AnyResource", bound=Resource)


def get_property_changes(source: AnyResource, returned: AnyResource) -> str | None:
    """Compares source to returned, all properties in source MUST exist in returned and MUST be the same value.

    A change from None to any other non None value will NOT be considered. Extra values in returned will NOT be
    considered.

    Returns a string with the human readable differences or None if they match.
    """
    differences: list[str] = []

    for key, source_val in source.__dict__.items():
        returned_val = getattr(returned, key, None)
        if source_val is not None and source_val != returned_val:

            # We are trying to avoid a series of Resource specific checks - so this is our attempt
            # to stay general
            if isinstance(source_val, list):
                continue  # We don't descend into list types for comparisons
            if isinstance(source_val, str) and returned_val is not None and isinstance(returned_val, str):
                # Comparisons on strings have to be done carefully as a hexbinary "0003" is equivalent to "03"
                if returned_val.endswith(source_val) or source_val.endswith(returned_val):
                    continue
            if (
                "time" in key.lower()
                and isinstance(source_val, int)
                and returned_val is not None
                and isinstance(returned_val, int)
            ):
                # We bake in a small amount of fat on time comparisons
                if abs(source_val - returned_val) <= MAX_TIME_DRIFT_SECONDS:
                    continue

            differences.append(f"{key} had {source_val} changed to {returned_val}")

    if differences:
        return ", ".join(differences)
    else:
        return None
