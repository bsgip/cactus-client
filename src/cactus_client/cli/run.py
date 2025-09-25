import argparse

from cactus_test_definitions.server.test_procedures import TestProcedureId

from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME

COMMAND_NAME = "run"


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the run module"""

    run_parser = subparsers.add_parser(COMMAND_NAME, help="For executing a specific test procedure.")
    run_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )
    run_parser.add_argument("id", help="The id of the test procedure to execute", choices=TestProcedureId)
    run_parser.add_argument("clientid", help="The ID's of configured client(s) to be used in this run.", nargs="+")


def run_action(args: argparse.Namespace) -> None:
    pass
