import argparse
import asyncio
import sys
from pathlib import Path

from rich.console import Console

from cactus_client.error import ConfigException
from cactus_client.execution.autorun import AutorunStatus, autorun_entrypoint
from cactus_client.model.config import CONFIG_CWD, CONFIG_HOME, load_config
from cactus_client.results.compliance import render_compliance_report

COMMAND_NAME = "autorun"


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    autorun_parser = subparsers.add_parser(
        COMMAND_NAME, help="Run multiple test procedures sequentially with automatic client assignment."
    )
    autorun_parser.add_argument(
        "-c",
        "--config-file",
        required=False,
        help=f"Override the config location. Defaults to {CONFIG_CWD} and then {CONFIG_HOME}",
    )
    autorun_parser.add_argument(
        "--headless",
        required=False,
        action="store_true",
        help="Disable terminal UI — execution logs will display via stderr instead.",
    )
    autorun_parser.add_argument(
        "--timeout",
        required=False,
        type=int,
        default=None,
        metavar="SECONDS",
        help="Per-test timeout in seconds. A test exceeding this limit is marked as failed and the run stops.",
    )
    autorun_parser.add_argument(
        "--include",
        required=False,
        nargs="+",
        metavar="ID",
        help="Only run these test procedure IDs. Overrides runner.include from config.",
    )
    autorun_parser.add_argument(
        "--include-file",
        required=False,
        metavar="PATH",
        help="Path to a text file listing test IDs to include (one per line, # = comment). "
        "Merged with --include. Overrides runner.include_file from config.",
    )
    autorun_parser.add_argument(
        "--exclude",
        required=False,
        nargs="+",
        metavar="ID",
        help="Skip these test procedure IDs. Overrides runner.exclude from config.",
    )
    autorun_parser.add_argument(
        "--strict",
        required=False,
        action="store_true",
        default=None,
        help="Treat warnings as failures. Overrides runner.strict from config.",
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

    runner_cfg = global_config.runner

    # CLI args take precedence over persistent config; fall back to config values when not supplied
    cli_include: list[str] | None = args.include or None
    cli_include_file: str | None = args.include_file
    cli_exclude: list[str] | None = args.exclude or None
    cli_timeout: int | None = args.timeout
    cli_strict: bool | None = args.strict

    include = cli_include if cli_include is not None else (runner_cfg.include or None if runner_cfg else None)
    include_file = (
        cli_include_file if cli_include_file is not None else (runner_cfg.include_file if runner_cfg else None)
    )
    exclude = cli_exclude if cli_exclude is not None else (runner_cfg.exclude or None if runner_cfg else None)
    timeout = cli_timeout if cli_timeout is not None else (runner_cfg.timeout if runner_cfg else None)
    strict = cli_strict if cli_strict is not None else (runner_cfg.strict if runner_cfg else False)

    headless: bool = bool(args.headless)

    try:
        records = asyncio.run(
            autorun_entrypoint(
                global_config=global_config,
                include=include,
                include_file=include_file,
                exclude=exclude,
                headless=headless,
                timeout=timeout,
                strict=strict,
            )
        )
    except ConfigException as exc:
        console.print(f"Configuration error: {exc}", style="red")
        sys.exit(1)
    except Exception:
        console.print_exception()
        sys.exit(1)

    render_compliance_report(console, Path(global_config.output_dir), include=[r.test_id for r in records])

    passed = sum(1 for r in records if r.status == AutorunStatus.PASSED)
    failed = sum(1 for r in records if r.status == AutorunStatus.FAILED)
    skipped = sum(1 for r in records if r.status == AutorunStatus.SKIPPED)
    errors = sum(1 for r in records if r.status == AutorunStatus.ERROR)

    console.print(
        f"\n[bold]Autorun complete:[/bold] {len(records)} attempted | "
        f"[green]{passed} passed[/green] | [red]{failed} failed[/red] | "
        f"[yellow]{skipped} skipped[/yellow] | [red]{errors} errors[/red]"
    )

    if failed > 0 or errors > 0:
        sys.exit(2)
    sys.exit(0)
