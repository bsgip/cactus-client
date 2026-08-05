import argparse
import asyncio
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cactus_client.error import ConfigError
from cactus_client.execution.connect import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ConnectCheckResult,
    check_client_connection,
)
from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME, ClientConfig, ServerConfig, load_config

COMMAND_NAME = "connect"


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the connect module"""

    connect_parser = subparsers.add_parser(
        COMMAND_NAME,
        help="Checks that registered clients can establish a connection with the server under test.",
    )
    connect_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )
    connect_parser.add_argument(
        "--timeout",
        required=False,
        type=float,
        default=DEFAULT_CONNECT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help=f"How long to wait for a response before giving up. Defaults to {DEFAULT_CONNECT_TIMEOUT_SECONDS}s.",
    )
    connect_parser.add_argument(
        "id",
        help="Only check this specific client id (defaults to checking every registered client)",
        nargs="?",
    )


async def check_all_clients(
    console: Console, server: ServerConfig, clients: list[ClientConfig], timeout_seconds: float
) -> list[ConnectCheckResult]:
    results: list[ConnectCheckResult] = []
    with console.status("Checking client connections...") as status:
        for client in clients:
            status.update(f"Checking [b]{client.id}[/b]...")
            results.append(await check_client_connection(server, client, timeout_seconds))
    return results


def print_results(console: Console, server: ServerConfig, results: list[ConnectCheckResult]) -> None:
    table = Table(title=f"Connectivity to {server.device_capability_uri}")
    table.add_column("id")
    table.add_column("result")
    table.add_column("time")
    table.add_column("detail")
    for result in results:
        outcome = "[green]✓[/]" if result.success else "[red]✗[/]"
        table.add_row(
            result.client_id,
            f"{outcome} {result.summary}",
            f"{result.elapsed_seconds:.2f}s",
            "" if result.success else (result.detail or ""),
            style=None if result.success else "red",
        )
    console.print(table)

    for result in results:
        if not result.success:
            console.print(
                Panel(
                    result.detail or result.summary,
                    title=f"[red b]{result.client_id}[/red b] diagnostics",
                    border_style="red",
                    expand=False,
                )
            )


def run_action(args: argparse.Namespace) -> None:
    config_file_override: str | None = args.config_file
    client_id: str | None = args.id
    timeout_seconds: float = args.timeout

    console = Console()

    try:
        config, _ = load_config(config_file_override)
    except ConfigError:
        console.print(
            "Error loading CACTUS configuration file. Have you run [b]cactus setup[/b]",
            style="red",
        )
        sys.exit(1)

    if config.server is None or not config.server.device_capability_uri:
        console.print(
            "No server has been configured. Try running [b]cactus server dcap <uri>[/b]",
            style="red",
        )
        sys.exit(1)

    if not config.clients:
        console.print("No clients have been registered. Try running [b]cactus client newclientid[/b]", style="red")
        sys.exit(1)

    if client_id:
        clients = [c for c in config.clients if c.id == client_id]
        if not clients:
            console.print(f"client [b]{client_id}[/b] does not exist.", style="red")
            sys.exit(1)
    else:
        clients = config.clients

    try:
        results = asyncio.run(check_all_clients(console, config.server, clients, timeout_seconds))
    except Exception:
        console.print_exception()
        sys.exit(1)

    print_results(console, config.server, results)

    if all(r.success for r in results):
        sys.exit(0)
    else:
        sys.exit(1)
