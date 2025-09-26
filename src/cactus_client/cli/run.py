import argparse
import asyncio
import sys

from cactus_test_definitions.csipaus import CSIPAusVersion
from cactus_test_definitions.server.test_procedures import (
    ClientType,
    TestProcedure,
    TestProcedureConfig,
    TestProcedureId,
)
from rich.console import Console
from rich.table import Table

from cactus_client.cli.server import ServerConfigKey
from cactus_client.error import ConfigException
from cactus_client.execution.run import run_entrypoint
from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME, RunConfig, load_config

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
    run_parser.add_argument("id", help="The id of the test procedure to execute (To list ids run 'cactus tests')")
    run_parser.add_argument("clientid", help="The ID's of configured client(s) to be used in this run.", nargs="*")


def run_action(args: argparse.Namespace) -> None:

    config_file_override: str | None = args.config_file
    test_id: str = args.id
    client_ids: list[str] = args.clientid

    console = Console()

    try:
        global_config, _ = load_config(config_file_override)
    except ConfigException:
        console.print("Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    if test_id not in TestProcedureId:
        console.print(
            f"[b]{test_id}[/b] isn't a recognised test procedure id. Try running [b]cactus tests[/b]", style="red"
        )
        sys.exit(1)

    run_config = RunConfig(
        test_procedure_id=TestProcedureId(test_id), client_ids=client_ids, version=CSIPAusVersion.RELEASE_1_2
    )

    try:
        asyncio.run(run_entrypoint(console=console, global_config=global_config, run_config=run_config))
    except ConfigException as exc:
        console.print(f"There is a problem with your configuration and the test couldn't start: {exc}.", style="red")
        sys.exit(1)
    except Exception:
        console.print_exception()
        sys.exit(1)
