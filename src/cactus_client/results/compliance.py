import re
from dataclasses import dataclass
from pathlib import Path

from cactus_test_definitions.server.test_procedures import TestProcedureId
from rich.console import Console
from rich.table import Table

from cactus_client.model.output import RunOutputFile

_RUN_DIR_PATTERN = re.compile(r"^run (\d+) - (.+)$")


@dataclass
class RunRecord:
    run_number: int
    test_id: TestProcedureId
    result: str  # "PASS" or "FAIL"
    run_dir: Path


def scan_output_dir(output_dir: Path) -> dict[TestProcedureId, RunRecord]:
    """Scan the output directory and return the most recent run record per test ID."""
    latest: dict[TestProcedureId, RunRecord] = {}

    if not output_dir.is_dir():
        return latest

    for entry in output_dir.iterdir():
        if not entry.is_dir():
            continue

        m = _RUN_DIR_PATTERN.match(entry.name)
        if not m:
            continue

        run_number = int(m.group(1))
        tp_id_file = entry / RunOutputFile.TestProcedureId
        result_file = entry / RunOutputFile.Result

        if not tp_id_file.exists() or not result_file.exists():
            continue

        try:
            tp_id = TestProcedureId(tp_id_file.read_text().strip())
        except ValueError:
            continue  # Directory belongs to an unknown / removed test ID

        result = result_file.read_text().strip()

        existing = latest.get(tp_id)
        if existing is None or run_number > existing.run_number:
            latest[tp_id] = RunRecord(run_number=run_number, test_id=tp_id, result=result, run_dir=entry)

    return latest


def render_compliance_report(console: Console, output_dir: Path, include: list[TestProcedureId] | None = None) -> None:
    """Print a Rich table showing the latest result for each test procedure.

    If *include* is given, only those IDs are shown (in the order supplied).
    Otherwise every known test procedure is listed."""
    latest_runs = scan_output_dir(output_dir)

    table = Table(title="Compliance Report")
    table.add_column("Test ID", style="bold")
    table.add_column("Result")
    table.add_column("Run #")

    for tp_id in (include if include is not None else TestProcedureId):
        record = latest_runs.get(tp_id)
        if record is None:
            table.add_row(str(tp_id), "[dim]NOT RUN[/dim]", "-")
        elif record.result == "PASS":
            table.add_row(str(tp_id), "[green]PASS[/green]", str(record.run_number))
        else:
            table.add_row(str(tp_id), "[red]FAIL[/red]", str(record.run_number))

    console.print(table)
