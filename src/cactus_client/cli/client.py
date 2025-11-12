import argparse
import sys
from dataclasses import replace
from enum import StrEnum, auto
from typing import Any

from cactus_test_definitions.server.test_procedures import ClientType
from rich.console import Console
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table

from cactus_client.cli.common import (
    is_certificate_file_invalid,
    is_key_file_invalid,
    rich_cert_file_value,
    rich_key_file_value,
)
from cactus_client.error import ConfigException
from cactus_client.model.config import (
    CONFIG_CWD,
    CONFIG_HOME,
    ClientConfig,
    GlobalConfig,
    load_config,
)
from cactus_client.sep2 import convert_lfdi_to_sfdi, lfdi_from_cert_file

COMMAND_NAME = "client"


class ClientConfigKey(StrEnum):
    CERTIFICATE = auto()
    KEY = auto()
    TYPE = auto()
    LFDI = auto()
    SFDI = auto()
    PEN = auto()
    PIN = auto()
    MAXW = auto()


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the client module"""

    client_parser = subparsers.add_parser(
        COMMAND_NAME, help="For listing/editing configuration of the testing clients used by this tool"
    )
    client_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )
    client_parser.add_argument("id", help="The id of the client to manage", nargs="?")
    client_parser.add_argument("config_key", help="The client setting to manage", nargs="?", choices=ClientConfigKey)
    client_parser.add_argument("new_value", help="The new value for config_key", nargs="?")


def find_client(config: GlobalConfig, client_id: str) -> ClientConfig | None:
    if not config.clients:
        return None

    for client in config.clients:
        if client.id == client_id:
            return client

    return None


def print_client_value(console: Console, client: ClientConfig | None, config_key: ClientConfigKey) -> None:
    if client is None:
        console.print("client does not exist", style="red")
        sys.exit(1)

    value: Any = None
    match config_key:
        case ClientConfigKey.CERTIFICATE:
            value = rich_cert_file_value(client.certificate_file)
        case ClientConfigKey.KEY:
            value = rich_key_file_value(client.key_file)
        case ClientConfigKey.LFDI:
            value = client.lfdi
        case ClientConfigKey.SFDI:
            value = client.sfdi
        case ClientConfigKey.TYPE:
            value = client.type
        case ClientConfigKey.MAXW:
            value = client.max_watts
        case ClientConfigKey.PEN:
            value = client.pen
        case ClientConfigKey.PIN:
            value = client.pin
        case _:
            console.print(f"[b]{config_key}[/b] can't be fetched", style="red")
            sys.exit(1)

    table = Table(title=client.id)
    table.add_column("key")
    table.add_column("value")

    table.add_row(config_key, str(value))
    console.print(table)


def update_client_value(
    console: Console, client: ClientConfig, config_key: ClientConfigKey, new_value: str
) -> ClientConfig:
    try:
        match config_key:
            case ClientConfigKey.CERTIFICATE:
                cert_error = is_certificate_file_invalid(new_value)
                if cert_error:
                    console.print(cert_error, style="red")
                    sys.exit(1)
                return replace(client, certificate_file=new_value)
            case ClientConfigKey.KEY:
                key_error = is_key_file_invalid(new_value)
                if key_error:
                    console.print(key_error, style="red")
                    sys.exit(1)
                return replace(client, key_file=new_value)
            case ClientConfigKey.LFDI:
                return replace(client, lfdi=new_value)
            case ClientConfigKey.SFDI:
                return replace(client, sfdi=int(new_value))
            case ClientConfigKey.TYPE:
                return replace(client, type=ClientType(new_value))
            case ClientConfigKey.MAXW:
                return replace(client, max_watts=int(new_value))
            case ClientConfigKey.PEN:
                return replace(client, pen=int(new_value))
            case ClientConfigKey.PIN:
                return replace(client, pin=int(new_value))
            case _:
                console.print(f"[b]{config_key}[/b] can't be updated", style="red")
                sys.exit(1)
    except Exception:
        console.print_exception()
        sys.exit(1)


def print_client(console: Console, client: ClientConfig) -> None:
    table = Table(title=client.id)
    table.add_column("key")
    table.add_column("value")

    table.add_row("type", client.type)
    table.add_row("certificate_file", rich_cert_file_value(client.certificate_file))
    table.add_row("key_file", rich_key_file_value(client.key_file))
    table.add_row("lfdi", client.lfdi)
    table.add_row("sfdi", str(client.sfdi))
    table.add_row("max_watts", str(client.max_watts))
    table.add_row("pen", str(client.pen))
    table.add_row("pin", str(client.pin))

    console.print(table)


def prompt_new_client(console: Console, new_client_id: str) -> ClientConfig:
    create = Confirm.ask(f"Would you like to create a new client with id '{new_client_id}'", console=console)
    if not create:
        console.print("No changes made.")
        sys.exit(0)

    client = ClientConfig(
        id=new_client_id,
        type=ClientType.DEVICE,
        certificate_file="",
        key_file="",
        lfdi="",
        sfdi=0,
        pen=0,
        pin=0,
        max_watts=0,
    )

    client_type = Prompt.ask(
        "What sort of client will this act as?",
        choices=[c.value for c in ClientType],
        case_sensitive=True,
        console=console,
    )
    client = update_client_value(console, client, ClientConfigKey.TYPE, client_type)

    cert_file = Prompt.ask("File path to PEM encoded client certificate", console=console)
    client = update_client_value(console, client, ClientConfigKey.CERTIFICATE, cert_file)

    key_file = Prompt.ask("File path to PEM encoded client key")
    client = update_client_value(console, client, ClientConfigKey.KEY, key_file)

    if client_type == ClientType.DEVICE:
        auto_calculate = Confirm.ask("Auto calculate lfdi/sfdi from certificate?", console=console)
    else:
        auto_calculate = False

    if auto_calculate:
        lfdi = lfdi_from_cert_file(cert_file)
        sfdi = convert_lfdi_to_sfdi(lfdi)
        console.print(f"[b]lfdi[/b]={lfdi}")
        console.print(f"[b]sfdi[/b]={sfdi}")
    else:
        lfdi = Prompt.ask("Client LFDI", console=console)
        sfdi = IntPrompt.ask("Client SFDI", console=console)

    client = update_client_value(console, client, ClientConfigKey.LFDI, lfdi)
    client = update_client_value(console, client, ClientConfigKey.SFDI, str(sfdi))

    pen = IntPrompt.ask("Client Private Enterprise Number (PEN) (used for mrid generation)", console=console)
    client = update_client_value(console, client, ClientConfigKey.PEN, str(pen))

    pin = IntPrompt.ask("Client PIN (used for matching EndDevice.Registration)", console=console)
    client = update_client_value(console, client, ClientConfigKey.PIN, str(pin))

    max_watts = IntPrompt.ask(
        "The DERSetting.setMaxW and DERCapability.rtgMaxW value to use (in Watts)", console=console
    )
    return update_client_value(console, client, ClientConfigKey.MAXW, str(max_watts))


def print_clients(console: Console, config: GlobalConfig) -> None:
    if not config.clients:
        console.print("No clients have been registered. Try running [b]cactus client newclientid[/b]")
    else:
        table = Table(title="Registered Clients")
        table.add_column("id", style="red")
        table.add_column("type")
        table.add_column("certificate file")
        table.add_column("key file")
        for client in config.clients:
            table.add_row(
                client.id,
                client.type,
                rich_cert_file_value(client.certificate_file, include_error=False),
                rich_key_file_value(client.key_file, include_error=False),
            )

        console.print(table)


def run_action(args: argparse.Namespace) -> None:
    config_file_override: str | None = args.config_file
    client_id: str | None = args.id
    config_key: ClientConfigKey | None = args.config_key
    new_value: str | None = args.new_value

    console = Console()

    try:
        config, config_path = load_config(config_file_override)
    except ConfigException:
        console.print("Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    if not client_id:
        # We are just printing registered clients
        print_clients(console, config)
        sys.exit(0)

    if not config_key:
        # We are either printing a specific client OR starting a new client prompt
        client = find_client(config, client_id)
        if client is None:
            client = prompt_new_client(console, client_id)
            config = replace(config, clients=(config.clients or []) + [client])
            config.to_yaml_file(config_path)
            console.print(f"{config_path} has been updated with a new client.")

        print_client(console, client)
        sys.exit(0)

    if not new_value:
        print_client_value(console, find_client(config, client_id), config_key)
        sys.exit(0)

    # Update a single value
    client = find_client(config, client_id)
    if client is None:
        console.print(f"client [b]{client_id}[/b] does not exist.")
        sys.exit(1)

    current_clients_list = config.clients or []
    updated_client = update_client_value(console, client, config_key, new_value)
    new_clients_list = current_clients_list.copy()
    new_clients_list[current_clients_list.index(client)] = updated_client
    config = replace(config, clients=new_clients_list)
    config.to_yaml_file(config_path)

    print_client_value(console, updated_client, config_key)
