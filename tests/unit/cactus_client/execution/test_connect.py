from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.server.test_procedures import ClientType

from cactus_client.execution.connect import check_client_connection
from cactus_client.model.config import ClientConfig, ServerConfig


def make_client_config(cert_file: str, key_file: str) -> ClientConfig:
    return ClientConfig(
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


@pytest.mark.asyncio
async def test_check_client_connection_success(aiohttp_client, generate_testing_key_cert):
    async def dcap_handler(request):
        return web.Response(body=b"<DeviceCapability/>", status=200)

    app = web.Application()
    app.router.add_get("/sep2/dcap", dcap_handler)
    client: TestClient = await aiohttp_client(app)

    with TemporaryDirectory() as tempdirname:
        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        server = ServerConfig(
            device_capability_uri=str(client.server.make_url("/sep2/dcap")),
            verify_ssl=False,
            verify_host_name=False,
        )
        client_config = make_client_config(str(cert_file), str(key_file))

        result = await check_client_connection(server, client_config)

        assert result.success
        assert result.client_id == "my-client1"
        assert result.status_code == 200
        assert result.detail is None


@pytest.mark.asyncio
async def test_check_client_connection_http_error_status(aiohttp_client, generate_testing_key_cert):
    async def dcap_handler(request):
        return web.Response(body=b"not found", status=404)

    app = web.Application()
    app.router.add_get("/sep2/dcap", dcap_handler)
    client: TestClient = await aiohttp_client(app)

    with TemporaryDirectory() as tempdirname:
        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        server = ServerConfig(
            device_capability_uri=str(client.server.make_url("/sep2/dcap")),
            verify_ssl=False,
            verify_host_name=False,
        )
        client_config = make_client_config(str(cert_file), str(key_file))

        result = await check_client_connection(server, client_config)

        assert not result.success
        assert result.status_code == 404
        assert result.detail is not None


@pytest.mark.asyncio
async def test_check_client_connection_connection_refused(generate_testing_key_cert):
    with TemporaryDirectory() as tempdirname:
        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        # Nothing is listening on this port - should fail to even connect
        server = ServerConfig(
            device_capability_uri="http://127.0.0.1:1/sep2/dcap", verify_ssl=False, verify_host_name=False
        )
        client_config = make_client_config(str(cert_file), str(key_file))

        result = await check_client_connection(server, client_config, timeout_seconds=2)

        assert not result.success
        assert result.status_code is None
        assert result.detail is not None


@pytest.mark.asyncio
async def test_check_client_connection_bad_client_cert():
    server = generate_class_instance(
        ServerConfig, verify_ssl=False, verify_host_name=False, device_capability_uri="http://localhost/dcap"
    )
    client_config = make_client_config("this-file-does-not-exist.cert", "this-file-does-not-exist.key")

    result = await check_client_connection(server, client_config)

    assert not result.success
    assert result.status_code is None
    assert "certificate" in result.summary.lower() or "key" in result.summary.lower()


@pytest.mark.asyncio
async def test_check_client_connection_bad_server_uri(generate_testing_key_cert):
    with TemporaryDirectory() as tempdirname:
        key_file = Path(tempdirname) / "my.key"
        cert_file = Path(tempdirname) / "my.cert"
        generate_testing_key_cert(key_file, cert_file)

        server = ServerConfig(device_capability_uri="not-a-valid-uri", verify_ssl=False, verify_host_name=False)
        client_config = make_client_config(str(cert_file), str(key_file))

        result = await check_client_connection(server, client_config)

        assert not result.success
        assert result.status_code is None
        assert "server" in result.summary.lower()
