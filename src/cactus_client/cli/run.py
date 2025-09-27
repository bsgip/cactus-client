import argparse
import asyncio
import sys

from cactus_test_definitions.csipaus import CSIPAusVersion
from cactus_test_definitions.server.test_procedures import (
    TestProcedureId,
)
from rich.console import Console

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
    run_parser.add_argument(
        "--headless",
        required=False,
        action="store_true",
        help="Stops terminal UI from running - execution logs will instead display via stderr",
    )
    run_parser.add_argument("id", help="The id of the test procedure to execute (To list ids run 'cactus tests')")
    run_parser.add_argument("clientid", help="The ID's of configured client(s) to be used in this run.", nargs="*")


def run_action(args: argparse.Namespace) -> None:

    config_file_override: str | None = args.config_file
    test_id: str = args.id
    client_ids: list[str] = args.clientid
    headless = True if args.headless else False

    try:
        global_config, _ = load_config(config_file_override)
    except ConfigException:
        Console().print("Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    if test_id not in TestProcedureId:
        Console().print(
            f"[b]{test_id}[/b] isn't a recognised test procedure id. Try running [b]cactus tests[/b]", style="red"
        )
        sys.exit(1)

    run_config = RunConfig(
        test_procedure_id=TestProcedureId(test_id),
        client_ids=client_ids,
        csip_aus_version=CSIPAusVersion.RELEASE_1_2,
        headless=headless,
    )

    try:
        test_passed = asyncio.run(run_entrypoint(global_config=global_config, run_config=run_config))
    except ConfigException as exc:
        Console().print(f"There is a problem with your configuration and the test couldn't start: {exc}.", style="red")
        sys.exit(1)
    except Exception:
        Console().print_exception()
        sys.exit(1)

    if test_passed:
        sys.exit(0)
    else:
        sys.exit(2)
