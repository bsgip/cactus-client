import argparse
import sys
from dataclasses import replace
from enum import StrEnum, auto
from urllib.parse import urlparse

from rich.console import Console
from rich.table import Table

from cactus_client.cli.common import is_certificate_file_invalid, rich_cert_file_value
from cactus_client.error import ConfigException
from cactus_client.model.config import (
    CONFIG_CWD,
    CONFIG_HOME,
    GlobalConfig,
    ServerConfig,
    load_config,
)

COMMAND_NAME = "server"


class ServerConfigKey(StrEnum):
    DCAP = auto()
    VERIFY = auto()
    SERCA = auto()


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the server module"""

    server_parser = subparsers.add_parser(
        COMMAND_NAME, help="For listing/editing configuration of the server that will be tested"
    )
    server_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )
    server_parser.add_argument("config_key", help="The server setting to manage", nargs="?", choices=ServerConfigKey)
    server_parser.add_argument("new_value", help="The new value for config_key", nargs="?")


def update_server_key(
    console: Console, config: GlobalConfig, config_key: ServerConfigKey, new_value: str
) -> ServerConfig:

    server = config.server
    if server is None:
        server = ServerConfig(device_capability_uri="", verify_ssl=True, serca_pem_file=None)

    try:
        match config_key:
            case ServerConfigKey.DCAP:
                parsed = urlparse(new_value)
                if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                    raise ValueError(f"{new_value} doesn't appear to be a valid URI. Got: {parsed}")
                return replace(server, device_capability_uri=new_value)
            case ServerConfigKey.VERIFY:
                if new_value.lower() in {"true", "t", "1", "y", "yes"}:
                    return replace(server, verify_ssl=True)
                elif new_value.lower() in {"false", "f", "0", "n", "no"}:
                    return replace(server, verify_ssl=False)
                raise ValueError(f"{new_value} can't be mapped to a boolean.")
            case ServerConfigKey.SERCA:
                cert_error = is_certificate_file_invalid(new_value)
                if cert_error:
                    raise ValueError(cert_error)

                return replace(server, serca_pem_file=new_value)
            case _:
                console.print(f"[b]{config_key}[/b] can't be updated", style="red")
                sys.exit(1)
    except Exception:
        console.print_exception()
        sys.exit(1)


def print_server(console: Console, config: GlobalConfig) -> None:

    table = Table(title="Server Config")
    table.add_column("key")
    table.add_column("value")

    dcap = config.server.device_capability_uri if config.server else None
    verify = config.server.verify_ssl if config.server else None
    serca_pem_file = config.server.serca_pem_file if config.server else None

    table.add_row("dcap", dcap if dcap else "[b red]null[/b red]")
    table.add_row("verify", str(verify) if verify is not None else "[b red]null[/b red]")
    table.add_row("serca", rich_cert_file_value(serca_pem_file))
    console.print(table)


def run_action(args: argparse.Namespace) -> None:
    config_file_override: str | None = args.config_file
    config_key: ServerConfigKey | None = args.config_key
    new_value: str | None = args.new_value

    console = Console()

    try:
        config, config_path = load_config(config_file_override)
    except ConfigException:
        console.print("Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    if not config_key:
        print_server(console, config)
        sys.exit(0)

    if not new_value:
        print_server(console, config)
        sys.exit(0)

    new_server = update_server_key(console, config, config_key, new_value)
    config = replace(config, server=new_server)
    config.to_yaml_file(config_path)

    print_server(console, config)
