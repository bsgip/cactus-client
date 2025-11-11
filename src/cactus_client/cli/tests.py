import argparse
import sys

from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
    TestProcedureConfig,
)
from rich.console import Console
from rich.table import Table

COMMAND_NAME = "tests"


def add_sub_commands(subparsers: argparse._SubParsersAction) -> None:
    """Adds the sub command options for the tests module"""

    test_parser = subparsers.add_parser(COMMAND_NAME, help="For printing information about test procedures.")
    test_parser.add_argument("id", help="The id of the test procedure to display", nargs="?")


def print_tests(console: Console) -> None:

    test_procedures = TestProcedureConfig.from_resource()

    table = Table(title="Available Test Procedures")
    table.add_column("Id", style="red")
    table.add_column("Category")
    table.add_column("Description")
    table.add_column("Required Clients")

    sorted_tps: list[tuple[str, TestProcedure]] = list(
        sorted(test_procedures.test_procedures.items(), key=lambda item: item[0])
    )
    for tp_id, tp in sorted_tps:
        client_types: list[str] = [f"[b]{c.client_type or 'any'}[/b]" for c in tp.preconditions.required_clients]
        table.add_row(
            tp_id,
            tp.category,
            tp.description,
            f"[b]{len(client_types)}[/b] client(s) with type(s): {', '.join(client_types)}",
        )

    console.print(table)


def print_test(console: Console, tp_id: str) -> None:

    test_procedures = TestProcedureConfig.from_resource()
    tp = test_procedures.test_procedures.get(tp_id, None)

    if tp is None:
        console.print(
            f"Test procedure [b]{tp_id}[/b] doesn't exist. Run [b]cactus tests[/b] to list available test procedures.",
            style="red",
        )
        sys.exit(1)

    metadata_table = Table(title=f"[b]{tp_id}[/b] Metadata")
    metadata_table.add_column("key")
    metadata_table.add_column("value")

    metadata_table.add_row("Description", tp.description)
    metadata_table.add_row("Category", tp.category)
    metadata_table.add_row("Classes", ", ".join(tp.classes) if tp.classes else "[b]None[/b]")
    metadata_table.add_row("Target Versions", ", ".join(tp.target_versions))

    for idx, client in enumerate(tp.preconditions.required_clients):
        metadata_table.add_row(f"Client [b]{idx}[/b]", f"[b red]{client.id}[/b red] {client.client_type or 'any'}")

    console.print(metadata_table)

    steps_table = Table(title=f"[b]{tp_id}[/b] Steps")
    steps_table.add_column("id", style="red")
    steps_table.add_column("Client")
    steps_table.add_column("Client Context")
    steps_table.add_column("Action")
    steps_table.add_column("Checks")
    steps_table.add_column("Repeat till Pass")

    for step in tp.steps:
        client_id = step.client if step.client else tp.preconditions.required_clients[0].id
        client_context = step.use_client_context if step.use_client_context else client_id
        checks: list[str] = [c.type for c in step.checks] if step.checks else []
        steps_table.add_row(
            step.id,
            client_id,
            client_context,
            step.action.type,
            ", ".join(checks) if checks else "[b]None[b]",
            str(step.repeat_until_pass),
        )

    console.print(steps_table)


def run_action(args: argparse.Namespace) -> None:

    test_id: str | None = args.id

    console = Console()

    if not test_id:
        print_tests(console)
        sys.exit(0)

    print_test(console, test_id)
    sys.exit(0)
