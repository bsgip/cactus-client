from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization


def parse_bool(val: str) -> bool:
    """Attempts to parse a boolean variable from val - accepts a variety of yes/no options. Raises ValueError if
    a boolean value cannot be ascertained"""
    lower_val = val.lower()
    if lower_val in {"true", "t", "1", "y", "yes"}:
        return True
    elif lower_val in {"false", "f", "0", "n", "no"}:
        return False
    raise ValueError(f"{val} can't be mapped to a boolean.")


def is_certificate_file_invalid(cert_path: str | None) -> str | None:
    """Rudimentary check that the supplied path points to an x509 PEM encoded certificate

    Returns a human readable error string (with details) if there's an error, None otherwise"""

    if not cert_path:
        return "Certificate path is not set."

    path = Path(cert_path)
    if not path.exists():
        return f"'{cert_path}' should point to a PEM encoded certificate. File doesn't exist"

    try:
        with open(path, "rb") as fp:
            cert_bytes = fp.read()

        x509.load_pem_x509_certificate(cert_bytes)
        return None
    except Exception as exc:
        return f"'{cert_path}' raised an exception when reading it as an x509 PEM certificate: {exc}"


def is_key_file_invalid(key_path: str | None) -> str | None:
    """Rudimentary check that the supplied path points to a private key.

    Returns a human readable error string (with details) if there's an error, None otherwise."""
    if not key_path:
        return "Key path is not set."

    path = Path(key_path)
    if not path.exists():
        return f"'{key_path}' should point to a PEM encoded key file. File doesn't exist"

    try:
        with open(path, "rb") as fp:
            key_bytes = fp.read()

        serialization.load_pem_private_key(key_bytes, password=None)
        return None
    except Exception as exc:
        return f"'{key_path}' raised an exception when reading it: {exc}"


def rich_cert_file_value(cert_path: str | None, include_error: bool = True) -> str:
    if not cert_path:
        return "[b red]null[/b red]"
    cert_error = is_certificate_file_invalid(cert_path)
    if cert_error:
        if include_error:
            return f"{cert_path} [red]X[/] {cert_error}"
        else:
            return f"{cert_path} [red]X[/]"
    else:
        return f"{cert_path} [green]✓[/]"


def rich_key_file_value(key_path: str | None, include_error: bool = True) -> str:
    if not key_path:
        return "[b red]null[/b red]"
    key_error = is_key_file_invalid(key_path)
    if key_error:
        if include_error:
            return f"{key_path} [red]X[/] {key_error}"
        else:
            return f"{key_path} [red]X[/]"
    else:
        return f"{key_path} [green]✓[/]"
