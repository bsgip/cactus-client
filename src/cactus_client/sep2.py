import hashlib

from cryptography import x509
from cryptography.hazmat.primitives import serialization


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
