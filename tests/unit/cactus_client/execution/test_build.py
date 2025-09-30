from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from assertical.asserts.type import assert_dict_type
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.csipaus import CSIPAusVersion
from cactus_test_definitions.server.test_procedures import ClientType, TestProcedureId

from cactus_client.error import ConfigException
from cactus_client.execution.build import build_execution_context
from cactus_client.model.config import (
    ClientConfig,
    GlobalConfig,
    RunConfig,
    ServerConfig,
)
from cactus_client.model.context import ClientContext, ExecutionContext


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
        csip_aus_version=CSIPAusVersion.RELEASE_1_2,
        headless=False,
    )

    return (expected_client_config, user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_s_all_01(generate_testing_key_cert):
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
async def test_build_execution_context_junk_certs(generate_testing_key_cert):
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
async def test_build_execution_context_bad_client_reference(generate_testing_key_cert):
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        run_config = replace(run_config, client_ids=["bad-client-id"])

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)


@pytest.mark.asyncio
async def test_build_execution_context_bad_test_id(generate_testing_key_cert):
    with TemporaryDirectory() as tempdirname:

        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        _, user_config, run_config = generate_valid_config(tempdirname, key_file, cert_file)

        run_config = replace(run_config, test_procedure_id="foo")

        with pytest.raises(ConfigException):
            await build_execution_context(user_config, run_config)
