from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from cactus_client.sep2 import convert_lfdi_to_sfdi, lfdi_from_cert_file, sum_digits


@pytest.mark.parametrize("n, expected", [(1, 1), (0, 0), (8, 8), (11, 2), (456, 15), (100001, 2)])
def test_sum_digits(n, expected):
    assert sum_digits(n) == expected
    assert sum_digits(-n) == expected, "Digit sum should be independent of sign"


@pytest.mark.parametrize(
    "lfdi, expected_sfdi",
    [
        ("3e4f45ab31edfe5b67e343e5e4562e31984e23e5", 167261211391),  # From 2030.5 standard example
        ("854d10a201ca99e5e90d3c3e1f9bc1c3bd075f3b", 357827241281),  # Calculated from envoy example
    ],
)
def test_convert_lfdi_to_sfdi(lfdi: str, expected_sfdi: int):
    assert convert_lfdi_to_sfdi(lfdi) == expected_sfdi
    assert convert_lfdi_to_sfdi(lfdi.upper()) == expected_sfdi


# Pulled from envoy: tests - certificate1.py
CERTIFICATE_CONTENTS = b"""-----BEGIN CERTIFICATE-----
MIIFszCCA5ugAwIBAgIUYUuu68R/soF/pE6Mrf1RCKT4brYwDQYJKoZIhvcNAQEL
BQAwaTELMAkGA1UEBhMCR0IxDzANBgNVBAgMBkxvbmRvbjEPMA0GA1UEBwwGTG9u
ZG9uMRAwDgYDVQQKDAdFeGFtcGxlMRAwDgYDVQQLDAdFeGFtcGxlMRQwEgYDVQQD
DAtleGFtcGxlLmNvbTAeFw0yMzAzMjIwNTI5MjdaFw0zNzAzMTgwNTI5MjdaMGkx
CzAJBgNVBAYTAkdCMQ8wDQYDVQQIDAZMb25kb24xDzANBgNVBAcMBkxvbmRvbjEQ
MA4GA1UECgwHRXhhbXBsZTEQMA4GA1UECwwHRXhhbXBsZTEUMBIGA1UEAwwLZXhh
bXBsZS5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDSyrwTUXHZ
LzRyKDBzjPhVxSmuWz7QYhdP/MQbRWPx38fz+Dmn3Q5GkBtOU/cgQ/wg4XWuKNF4
/j61272c39t6jW5TWBR1k1hRXa1wm0fnf56YjIjNSlH4TQla7tXpwFQjBuw+4Rd4
nd7xRcg1Uf3Er8R4zzsvL6e3HHtxCQz8IjTReR9jE6lu4vyhItYfhVjlt4G12HI9
os+jLpUgsIdt36cb4LjTVXEf+tw0XX0gy0Xp1Dm5ABQ38GQG8XCea/4WP0oSzBbg
z50fQaM48ewFdn9TpECJVX5mvye1LCIahor1zl10jJX7yVirSSgR197MMR0bUQyJ
04YPSIZxwszpO0hbnE7+Yz5ydVjwiMm+2lfZ3lUVJFY4X68Pvj25M4X5ysMRVhu2
lTr8ZQYHGPfK03FRolZO+1FoGD9ypqqCBp5KxjZ8NdwLfKiBVTLgLJlhBOOljqZF
Zuj6esBa9wzkU62StykWzqb+fqBW+jGMaL1RMgHZ3Ohm9DPAxEaTjDmK5cvlTskc
VYgR3gztKShMGSAs532vS5rOYi86BJpr8kEe+SG/1Dl04F0rK4qNk+aCBC/7miem
VnHMssj5BbtV3DJpF3NlFmCJ5n30icM2KR9HJFzpBbufO9DWhtFMyBfjFvWb06IQ
fkqldqqRbWhMgXfudhjOVyGEa6bpdqE3wwIDAQABo1MwUTAdBgNVHQ4EFgQUOolp
f90K/ERSaGDZSV7wEbVqnrowHwYDVR0jBBgwFoAUOolpf90K/ERSaGDZSV7wEbVq
nrowDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAgEAfuHAai8q8Bk2
1YaFY+VRxGvxW2unGnAicgiajShIeL9CLcV0VXqd/79SRdJaFBts8A6akXz3PHYF
9oM882FSWBSHSEztR3jOQHeJnFUNLk/e1aZJWQRRI7SwN9cDJrMdZOghPmkwl160
aKphkQtboUJs33BKpzUSDr2tsOLZXppVi22ngzcgiro5/3oIDvYIalTfZ3tQEMg9
OLmQFp1uyisXJ+q7AT04AHIC9Mh3va5zmpxxR68FppbFXRVl8Hhx+57CKrw4VDay
pcWUkIHiviEOSZdhp4+Vy/VEFVlkiJzF64dgFoDs9eoTqU4zfuNzc8zlWsl7/IEl
S3f35QZIKo99HfXWpjVyeT0kteDe5GEtMOsQp56VsYRKFZMdLDV/0WLr/0aD2RCr
Dl9Bfx2ewiBmmNcanicaxRgAJFutF8SKV8M62+4SC3PVvRTpC6a/Biwk1EB7fsk/
UijWxeL66Rk5vdnLHrUAKiKimsnl2DftP+fvG3xgfPF4/szAWMVjhnHPhFzPXSR+
ZpNVDGJQhYHMBEs2s90QL+hVnjBblfvhbqBbiqXcVg2+wNfTIOXbUK3m86vKW+aj
A39FiN7VYiEFY3dVZNkcjfRjED4D3nOSs0uT247VVP882UXa6mFecaM9u6hgZyyw
fSCR9LWxGyGL+iGNC19NciZUuY1kLr8=
-----END CERTIFICATE-----
"""


def test_lfdi_from_cert_file():
    with TemporaryDirectory() as tmp_dir:
        cert_file = Path(tmp_dir) / "my.cert"
        with open(cert_file, "wb") as fp:
            fp.write(CERTIFICATE_CONTENTS)

        assert lfdi_from_cert_file(cert_file) == "854D10A201CA99E5E90D3C3E1F9BC1C3BD075F3B"
