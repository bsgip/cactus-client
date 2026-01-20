import tempfile
from pathlib import Path

import pytest
from assertical.fake.generator import generate_class_instance
from cactus_test_definitions.server.test_procedures import ClientType

from cactus_client.error import ConfigException
from cactus_client.model.config import ClientConfig, GlobalConfig, ServerConfig, load_config


def test_load_config_errors():
    with tempfile.TemporaryDirectory() as tempdirname:
        missing_path = Path(tempdirname) / Path("file.dne")
        with pytest.raises(ConfigException):
            load_config(missing_path)

        empty_path = Path(tempdirname) / Path("file.empty")
        empty_path.write_text("")
        with pytest.raises(ConfigException):
            load_config(empty_path)

        malformed_path = Path(tempdirname) / Path("file.mangled")
        malformed_path.write_text("abc=123\nthis is clearly not a yaml file\nlorem ipsum")
        with pytest.raises(ConfigException):
            load_config(malformed_path)


@pytest.mark.parametrize(
    "yaml, expected",
    [
        ("output_dir: foo.bar", GlobalConfig("foo.bar", None, None)),
        (
            """
output_dir: /foo/bar # This is a comment
server:
  device_capability_uri: http://example.com
  verify_ssl: true
""",
            GlobalConfig("/foo/bar", ServerConfig("http://example.com", True), None),
        ),
        (
            """
output_dir: /foo/bar # This is a comment
server:
  device_capability_uri: http://example.com
  verify_ssl: true
clients:
  - id: client1
    type: device
    certificate_file: /certs/cert.1
    key_file:
    lfdi: ABC123
    sfdi: 456
    pen: 789
    pin: 43210
    max_watts: 23000
  - id: client2
    type: aggregator
    certificate_file: /certs/cert.2
    key_file: /certs/key.2
    lfdi: abc123DEF
    sfdi: 111
    pen: 222
    pin: 333
    max_watts: 444
""",
            GlobalConfig(
                "/foo/bar",
                ServerConfig("http://example.com", True),
                [
                    ClientConfig("client1", ClientType.DEVICE, "/certs/cert.1", None, "ABC123", 456, 789, 43210, 23000),
                    ClientConfig(
                        "client2",
                        ClientType.AGGREGATOR,
                        "/certs/cert.2",
                        "/certs/key.2",
                        "abc123DEF",
                        111,
                        222,
                        333,
                        444,
                    ),
                ],
            ),
        ),
    ],
)
def test_load_config(yaml: str, expected: GlobalConfig):
    with tempfile.TemporaryDirectory() as tempdirname:
        path = Path(tempdirname) / Path("file.yaml")
        path.write_text(yaml)
        actual, path_used = load_config(path)
        assert isinstance(actual, GlobalConfig)
        assert isinstance(path_used, Path)
        assert expected == actual
        assert path == path_used


def assert_validation_error(cfg: GlobalConfig, valid: bool):
    actual = cfg.get_validation_error()
    if valid:
        assert actual is None
    else:
        assert actual and isinstance(actual, str)


def test_GlobalConfig_is_valid():

    assert_validation_error(GlobalConfig(None, None, None), False)

    # Check paths asserted
    with tempfile.TemporaryDirectory() as tempdirname:
        missing_dir = Path(tempdirname) / "missing_dir/"
        working_dir = Path(tempdirname) / "my_dir/"
        cert1_file = Path(tempdirname) / "cert1.cert"
        cert1_key = Path(tempdirname) / "cert1.key"
        cert2_file = Path(tempdirname) / "cert2.cert"
        missing_file = Path(tempdirname) / "missing"

        working_dir.mkdir()
        cert1_file.write_text("dummy content")
        cert1_key.write_text("dummy content")
        cert2_file.write_text("dummy content")

        c1_cfg = generate_class_instance(
            ClientConfig, seed=101, certificate_file=cert1_file.absolute(), key_file=cert1_key.absolute()
        )
        c2_cfg = generate_class_instance(ClientConfig, seed=202, certificate_file=cert2_file.absolute(), key_file=None)
        s_cfg = generate_class_instance(ServerConfig)

        assert_validation_error(GlobalConfig(working_dir.absolute(), None, None), False)
        assert_validation_error(GlobalConfig(working_dir.absolute(), None, [c1_cfg]), False)  # No server
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, []), False)  # No client
        assert_validation_error(GlobalConfig(cert1_file.absolute(), s_cfg, [c1_cfg]), False)  # Not a dir
        assert_validation_error(GlobalConfig(missing_dir.absolute(), s_cfg, [c1_cfg]), False)  # Not created dir

        # valid config
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, [c1_cfg, c2_cfg]), True)

        # Start removing files to validate failure
        cert1_file.rename(missing_file)
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, [c1_cfg, c2_cfg]), False)
        missing_file.rename(cert1_file)

        cert1_key.rename(missing_file)
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, [c1_cfg, c2_cfg]), False)
        missing_file.rename(cert1_key)

        cert2_file.rename(missing_file)
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, [c1_cfg, c2_cfg]), False)
        missing_file.rename(cert2_file)

        # should be back to valid now
        assert_validation_error(GlobalConfig(working_dir.absolute(), s_cfg, [c1_cfg, c2_cfg]), True)


@pytest.mark.parametrize("seed, optional_is_none", [(101, False), (202, False), (303, True)])
def test_GlobalConfig_yaml_roundtrip(seed: int, optional_is_none: bool):
    """Does the yaml encode/decode generate identical config objects?"""
    original = generate_class_instance(
        GlobalConfig, seed=seed, optional_is_none=optional_is_none, generate_relationships=True
    )

    with tempfile.TemporaryDirectory() as tempdirname:
        yaml_file = Path(tempdirname) / "cfg.yaml"
        original.to_yaml_file(yaml_file)

        after = GlobalConfig.from_yaml_file(yaml_file)

        assert original == after
