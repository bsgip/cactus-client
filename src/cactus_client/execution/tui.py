import asyncio
import logging
from datetime import timedelta

from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.spinner import SPINNERS, Spinner
from rich.table import Table
from rich.text import Text

from cactus_client.model.context import ExecutionContext
from cactus_client.results.common import context_relative_time

logger = logging.getLogger(__name__)


def generate_header(context: ExecutionContext, run_id: int) -> RenderableType:
    """Generates the highlighted header at the top of the UI"""
    if context.progress.current_step_execution:
        instructions = ". ".join(context.progress.current_step_execution.source.instructions or [])
    else:
        instructions = ""

    grid = Table.grid(expand=True)
    grid.add_column(justify="left")
    grid.add_column(justify="right", ratio=1)
    grid.add_row(
        f"Run #{run_id} [b]{context.test_procedure_id}[/b]",
        instructions,
    )
    return Panel(grid, style="white on blue")


def generate_requests(context: ExecutionContext, height: int) -> RenderableType:
    """Generates the requests panel showing recent / current requests"""

    max_requests_to_show = height - 4
    table_responses = Table(title="Requests", show_header=False, expand=True, title_justify="left")

    for response in context.responses.responses[-max_requests_to_show:]:
        if response.body:
            xsd = "\n".join(response.xsd_errors) if response.xsd_errors else "valid"
        else:
            xsd = ""
        success = response.is_success() and not response.xsd_errors
        table_responses.add_row(
            context_relative_time(context, response.requested_at),
            response.method,
            response.url,
            str(response.status),
            xsd,
            style="green" if success else "red",
        )

    req = context.responses.active_request
    if req is None:
        active_request_line: RenderableType = "No request is currently active."
    else:
        body = f"{len(req.body)} bytes sent" if req.body else "No body"
        active_request_line = Columns(
            [Spinner("dots"), context_relative_time(context, req.created_at), req.method, req.url, body]
        )

    return Group(table_responses, active_request_line)


def generate_step_progress(context: ExecutionContext) -> RenderableType:
    step_grid = Table(
        title=f"[b]{context.test_procedure_id}[/] Steps",
        caption=f"[b]{len(context.steps._items)}[/] steps in queue.",
        caption_justify="left",
        show_header=False,
        expand=True,
        title_justify="left",
    )
    for step in context.test_procedure.steps:
        step_progress = context.progress.progress_by_step_id.get(step.id, None)
        step_result = step_progress.result if step_progress else None
        step_style = None
        started = ""
        dot: RenderableType = "·"
        if step_result is not None and step_result.is_passed():
            dot = "✓"
            step_style = "green"
        elif step_result is not None and not step_result.is_passed():
            dot = "x"
            step_style = "red"
        elif (
            context.progress.current_step_execution is not None
            and context.progress.current_step_execution.source.id == step.id
        ):
            dot = Spinner("dots")

        if step_progress is not None:
            started = context_relative_time(context, step_progress.created_at)

        step_grid.add_row(dot, f"[b]{step.id}[/]", started, style=step_style)

    return step_grid


def generate_warnings(context: ExecutionContext) -> RenderableType:
    if not context.warnings.warnings:
        return Panel(Align("[i]No warnings to show[/]", vertical="middle", align="center"))

    warnings_table = Table(title="Warnings", title_justify="left", style="red", expand=True, show_header=False)
    for warning in context.warnings.warnings:
        warnings_table.add_row(warning)
    return warnings_table


def render_tui(context: ExecutionContext, run_id: int) -> RenderableType:
    layout = Layout(name="root")
    footer_height = 7

    layout.split(
        Layout(generate_header(context, run_id), name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(generate_requests(context, footer_height), name="requests", size=footer_height),
    )
    layout["main"].split_row(
        Layout(name="steps"),
        Layout(name="active-step", ratio=2, minimum_size=60),
    )
    layout["steps"].split(
        Layout(generate_step_progress(context), name="step-progress", ratio=2),
        Layout(generate_warnings(context), name="warnings-list"),
    )

    return layout


async def run_tui(console: Console, context: ExecutionContext, run_id: int, refresh_rate_ms: int = 500) -> None:
    """Runs the terminal user interface - expected to run in an infinite loop"""

    refresh_rate = timedelta(milliseconds=refresh_rate_ms).total_seconds()
    with Live(console=console, screen=True, transient=True, auto_refresh=False) as live:

        while True:
            try:
                # Ideally this would be a wait on the progress tracker that only returns when the progress has updated
                # In a future update - we might just do that for more efficient/responsive drawing
                live.update(render_tui(context, run_id), refresh=True)

                await asyncio.sleep(refresh_rate)
            except asyncio.CancelledError:
                logger.info("Shutting down TUI")
                break
            except Exception as exc:
                logger.info("Unhandled TUI exception", exc_info=exc)
                break
