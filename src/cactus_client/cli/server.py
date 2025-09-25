import argparse
from enum import StrEnum, auto

from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME

COMMAND_NAME = "server"


class ServerConfigKey(StrEnum):
    DCAP = auto()
    VERIFY = auto()


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
    server_parser.add_argument("id", help="The id of the client to manage", nargs="?")
    server_parser.add_argument("config_key", help="The server setting to manage", nargs="?", choices=ServerConfigKey)
    server_parser.add_argument("new_value", help="The new value for config_key", nargs="?")


def run_action(args: argparse.Namespace) -> None:
    pass
