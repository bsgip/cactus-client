import argparse
from enum import StrEnum, auto
from pathlib import Path

from cactus_client.model.config import CONFIG_CWD, CONFIG_FILE_NAME, CONFIG_HOME

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


def run_action(args: argparse.Namespace) -> None:

    print(args)
