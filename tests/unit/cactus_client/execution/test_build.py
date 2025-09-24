import os
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from assertical.asserts.type import assert_dict_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusVersion
from cactus_test_definitions.server.test_procedures import ClientType, TestProcedureId
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from cactus_client.error import ConfigException
from cactus_client.execution.build import build_execution_context
from cactus_client.model.config import (
    ClientConfig,
    GlobalConfig,
    RunConfig,
    ServerConfig,
)
from cactus_client.model.context import ClientContext, ExecutionContext
from cactus_client.time import utc_now


def generate_testing_key_cert(key_file: Path, cert_file: Path):

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "AU"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "ACT"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Canberra"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Australian National University"),
            x509.NameAttribute(NameOID.COMMON_NAME, cert_file.name),
        ]
    )

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(utc_now())
        .not_valid_after(utc_now() + timedelta(hours=1))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .sign(private_key=key, algorithm=hashes.SHA256())
    )

    with open(key_file, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,  # PKCS#1 format
                encryption_algorithm=serialization.NoEncryption(),  # No password
            )
        )
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))


def generate_valid_config(
    output_dir: str, key_file: str, cert_file: str
) -> tuple[ClientConfig, GlobalConfig, RunConfig]:
    expected_client_config = ClientConfig(
        id="my-client1",
        type=ClientType.AGGREGATOR,
        certificate_file=cert_file,
        key_file=key_file,
        lfdi="abc123",
        sfdi=111,
        pen=222,
        pin=333,
        max_watts=5000,
    )

    user_config = GlobalConfig(
        output_dir=output_dir,
        server=ServerConfig(device_capability_uri="https://my.test.server:1234/my/path", verify_ssl=True),
        clients=[
            generate_class_instance(ClientConfig, seed=101),
            expected_client_config,
            generate_class_instance(ClientConfig, seed=202),
        ],
    )

    run_config = RunConfig(
        test_procedure_id=TestProcedureId.S_ALL_01,
        client_ids=["my-client1"],
        version=CSIPAusVersion.RELEASE_1_2,
    )

    return (expected_client_config, user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_s_all_01():
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        expected_client_config, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        result = await build_execution_context(user_config, run_config)
        assert isinstance(result, ExecutionContext)
        assert result.dcap_path == "/my/path"
        assert len(result.steps) > 0

        # Checkout the client context
        assert_dict_type(str, ClientContext, result.clients_by_alias, count=1)
        client_context = result.clients_by_alias["client"]
        assert client_context.client_config == expected_client_config
        assert client_context.test_procedure_alias == "client"
        assert str(client_context.session._base_url) == "https://my.test.server:1234/"


@pytest.mark.asyncio
async def test_build_execution_context_junk_certs():
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        with open(key_file, "wb") as f:
            f.write("clearly junk".encode())
        with open(cert_file, "wb") as f:
            f.write("clearly junk".encode())

        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_missing_certs():
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_bad_client_reference():
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        run_config = replace(run_config, client_ids=["bad-client-id"])

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_bad_test_id():
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        run_config = replace(run_config, test_procedure_id="foo")

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)
