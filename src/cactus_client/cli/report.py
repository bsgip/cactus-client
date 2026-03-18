import argparse
import sys
from pathlib import Path

from rich.console import Console

from cactus_client.error import ConfigException
from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME, load_config
from cactus_client.results.compliance import render_compliance_report

COMMAND_NAME = "report"


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    report_parser = subparsers.add_parser(
        COMMAND_NAME, help="Print a compliance report showing the latest result for each test procedure."
    )
    report_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )


def run_action(args: argparse.Namespace) -> None:
    console = Console()

    try:
        global_config, _ = load_config(args.config_file)
    except ConfigException:
        console.print("Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    if not global_config.output_dir:
        console.print("output_dir is not configured. Have you run [b]cactus setup[/b]", style="red")
        sys.exit(1)

    render_compliance_report(console, Path(global_config.output_dir))
