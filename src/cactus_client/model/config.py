from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from cactus_test_definitions.server.test_procedures import ClientType, TestProcedureId
from dataclass_wizard import YAMLWizard

from cactus_client.error import ConfigException

CONFIG_FILE_NAME = Path(".cactus.yaml")  # Name of the config


def strenum_representer(dumper: yaml.Dumper, data: Any) -> yaml.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data))


yaml.add_representer(ClientType, strenum_representer)


@dataclass(frozen=True)
class ServerConfig:
    """Top level "Server" config options that are related to the server under test. This object represents
    the values as stored on disk."""

    device_capability_uri: str
    verify_ssl: bool  # All connections will use the system default SSL validation for the server cert.


@dataclass(frozen=True)
class ClientConfig:
    """Defines the "global" config associated with a named client (it is expected for there to be multiple
    clients in any deployment). This object represents the values as stored on disk."""

    id: str  # Unique identifier for this client (used for referencing in tests - eg "myclient1")
    type: ClientType  # The type of client that this config is representing
    certificate_file: str  # File path to a PEM encoded certificate
    key_file: str | None  # File path to a PEM encoded key file (If None - key must be included in certificate_file)
    lfdi: str  # Current 2030.5 LFDI that will be used for registering an EndDevice for this client
    sfdi: int  # Current 2030.5 SFDI that will be used for registering an EndDevice for this client
    pen: int  # Private Enterprise Number that this client will utilise
    pin: int  # Registration PIN that this client will treat as "valid"
    max_watts: int  # How many watts will be registered by this client (eg setMaxW rtgMaxW) with the utility server


@dataclass(frozen=True)
class RunConfig:
    """Represents the config for a particular run (usually parsed from CLI)"""

    test_procedure_id: TestProcedureId  # What test procedure is being run?
    client_ids: list[str]  # What clients are being
    output_dir: str  # Working directory for all outputs associated with this test
    version: str  # What version of the tests are being evaluated?


@dataclass(frozen=True)
class GlobalConfig(YAMLWizard):  # type: ignore
    output_dir: str | None = None  # Directory where all outputs will be dumped
    server: ServerConfig | None = None  # The current server configuration
    clients: list[ClientConfig] | None = None  # All possible clients that have been previously configured

    def get_validation_error(self) -> str | None:
        """Attempts to identify whether this configuration is fully defined (and therefore capable of running tests)

        Returns a human readable error on failure or None if the file is valid."""
        if not self.output_dir:
            return "output_dir is not defined. It should point to a directory that exists"
        output_dir = Path(self.output_dir)
        if not output_dir.is_dir() or not output_dir.exists():
            return f"{output_dir} is either not a directory or doesn't exist"

        if self.server is None or not self.server.device_capability_uri:
            return f"No server configuration has been specified or device_capability_uri isn't set."

        if not self.clients:
            return f"No client configuration has been specified."

        for c in self.clients:
            if not Path(c.certificate_file).exists():
                return f"Client {c.id} references certificate_file {c.certificate_file} which does not exist."

            if c.key_file is not None and not Path(c.key_file).exists():
                return f"Client {c.id} references key_file {c.key_file} which does not exist."

        return None


def resolve_config_path() -> Path:
    """Attempts to resolve a config file path for the global config (or raises ConfigException on failure)"""

    # Check for a config file in the current working dir
    local_file = Path.cwd() / CONFIG_FILE_NAME
    if Path.exists(local_file):
        return local_file

    home_file = Path.home() / CONFIG_FILE_NAME
    if Path.exists(local_file):
        return home_file

    raise ConfigException(f"Couldn't find {CONFIG_FILE_NAME} in the current working dir / home dir.")


def load_config(config_file_path_override: str | None) -> GlobalConfig:
    """Main configuration entrypoint - if config_file_path_override is specified it will be used, otherwise local/home
    default locations will be checked"""

    if config_file_path_override is None:
        global_config_path = resolve_config_path()
    else:
        global_config_path = Path(config_file_path_override)

    try:
        config = GlobalConfig.from_yaml_file(global_config_path)
    except Exception as exc:
        raise ConfigException(f"Error reading config {exc}")

    if not isinstance(config, GlobalConfig):
        raise ConfigException(f"Received an invalid type for config: {type(config)}. This is likely a corrupted file.")

    return config
