import asyncio
import logging
from datetime import timedelta
from enum import StrEnum, auto
from typing import Any, Callable, TypeVar

import yaml
from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.spinner import Spinner
from rich.syntax import Syntax
from rich.table import Column, Table

from cactus_client.execution import keypress
from cactus_client.model.context import ExecutionContext
from cactus_client.model.http import ServerResponse
from cactus_client.results.common import context_relative_time
from cactus_client.time import relative_time, utc_now

logger = logging.getLogger(__name__)

AnyType = TypeVar("AnyType")


class PanelFocus(StrEnum):
    Logs = auto()
    Warnings = auto()
    Requests = auto()


CURRENT_FOCUS: PanelFocus | None = None

HEADER_HEIGHT = 3


def generate_scrolling_table(
    title: str,
    columns: list[Column],
    style: str,
    items: list[AnyType] | None,
    add_row: Callable[[Table, AnyType], None],
    height: int,
) -> Table:
    """Generates a fixed height table that populates from the bottom upwards (representing a scrolling series of
    recent things)"""
    table = Table(*columns, title=title, title_justify="left", style=style, expand=True, show_header=False)

    max_rows = height - 3

    if items is None:
        items_to_render = []
    else:
        items_to_render = items[-max_rows:]

    for i in range(max_rows - len(items_to_render)):

        if not items_to_render and i == (max_rows // 2):
            table.add_row("There isn't anything here...")
        else:
            table.add_row()

    for item in items_to_render:
        add_row(table, item)

    return table


def generate_header(context: ExecutionContext, run_id: int) -> RenderableType:
    """Generates the highlighted header at the top of the UI"""
    if context.progress.current_step_execution and context.progress.current_step_execution.source.instructions:
        raw_instructions = ". ".join(context.progress.current_step_execution.source.instructions)
        instructions = f"[blink on red]***[/][on red] {raw_instructions} [/][blink on red]***[/]"
    else:
        instructions = ""

    grid = Table.grid(expand=True)
    grid.add_column(justify="left")
    grid.add_column(justify="right", ratio=1)
    grid.add_row(
        f"🌵 Run #{run_id} [b]{context.test_procedure_id}[/b] {context_relative_time(context, utc_now())}",
        instructions,
    )
    return Panel(grid, style="white on blue")


def generate_requests(context: ExecutionContext, height: int) -> RenderableType:
    """Generates the requests panel showing recent / current requests"""

    def add_row(table: Table, response: ServerResponse) -> None:
        if response.body:
            xsd = "\n".join(response.xsd_errors) if response.xsd_errors else "valid"
        else:
            xsd = ""
        success = response.is_success() and not response.xsd_errors
        table.add_row(
            context_relative_time(context, response.request.created_at),
            response.method,
            response.url,
            str(response.status),
            xsd,
            style="green" if success else "red",
        )

    table_responses = generate_scrolling_table(
        "Requests",
        [Column(overflow="ellipsis", no_wrap=True)],
        "none",
        context.responses.responses,
        add_row,
        height=height - 1,
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


def generate_warnings(context: ExecutionContext, height: int) -> RenderableType:

    return generate_scrolling_table(
        "Warnings",
        [Column(overflow="ellipsis", no_wrap=True)],
        "red",
        context.warnings.warnings,
        lambda tbl, log: tbl.add_row(f"[b]{log.step_execution.source.id}[/] {log.message}"),
        height=height,
    )


def generate_active_step(context: ExecutionContext) -> RenderableType:
    se = context.progress.current_step_execution
    if se is None:
        return Panel(Align("[i]No step is active...[/]", vertical="middle", align="center"))

    if se.client_alias == se.client_resources_alias:
        description = f"Using [b]{context.clients_by_alias[se.client_alias].client_config.id}[/]"
    else:
        description = (
            f"Using [b]{context.clients_by_alias[se.client_alias].client_config.id}[/] connection and"
            + f" [b]{context.clients_by_alias[se.client_resources_alias].client_config.id}[/] resources."
        )

    if se.attempts:
        description += f", Attempt [b]#{se.attempts + 1}[/]"
    if se.repeat_number:
        description += f", Repeat [b]#{se.repeat_number}[/]"

    if se.not_before is not None and se.not_before > utc_now():
        description += f" Waiting for [b]{relative_time(se.not_before - utc_now())}[/]"

    action_raw: dict[str, Any] = {
        "type": se.source.action.type,
        "parameters": se.source.action.parameters,
    }
    yaml_columns = [
        Group(
            "[b]Action[/]",
            "",
            Syntax(code=yaml.dump(action_raw, sort_keys=False), lexer="yaml", background_color="black"),
        )
    ]
    for check in se.source.checks or []:
        check_raw = {"type": check.type, "parameters": check.parameters}
        yaml_columns.append(
            Group(
                "[b]Check[/]",
                "",
                Syntax(code=yaml.dump(check_raw, sort_keys=False), lexer="yaml", background_color="black"),
            )
        )

    return Panel(
        Group(
            description,
            Rule(title="YAML"),
            Columns(
                # table,
                yaml_columns  # Syntax(code=yaml.dump(asdict(se.source)), lexer="yaml"),
            ),
        ),
        title=f"Active Step [b]{se.source.id}[/b]",
        style="on black",
    )


def generate_active_step_logs(context: ExecutionContext, height: int) -> RenderableType:
    current_step = context.progress.current_step_execution
    progress = context.progress.progress_by_step_id.get(current_step.source.id, None) if current_step else None

    return generate_scrolling_table(
        f"Logs for [b]{current_step.source.id if current_step else ''}[/]",
        [Column(overflow="ellipsis", no_wrap=True)],
        "blue",
        None if progress is None else progress.log_entries,
        lambda tbl, log: tbl.add_row(f"[b]{context_relative_time(context, log.created_at)}[/] {log.message}"),
        height=height,
    )


def render_unfocused_tui(context: ExecutionContext, run_id: int, console_height: int) -> RenderableType:
    """Renders the TUI with ALL panels (the default display)"""
    layout = Layout(name="root")

    # Shenanigans around trying to do dynamic vertical sizing
    footer_height = 7
    warnings_height = 7
    logs_height = 7
    if console_height > 30:
        footer_height += (console_height - 30) // 3
        warnings_height += (console_height - 30) // 3
        logs_height += (console_height - 30) // 3

    layout.split(
        Layout(generate_header(context, run_id), name="header", size=HEADER_HEIGHT),
        Layout(name="main", ratio=1),
        Layout(generate_requests(context, footer_height), name="requests", size=footer_height),
    )
    layout["main"].split_row(
        Layout(name="steps"),
        Layout(name="active-step", ratio=2, minimum_size=60),
    )
    layout["steps"].split(
        Layout(generate_step_progress(context), name="step-progress", ratio=2),
        Layout(generate_warnings(context, warnings_height), name="warnings-list", size=warnings_height),
    )

    layout["active-step"].split(
        Layout(generate_active_step(context), name="active-step-main", ratio=2),
        Layout(generate_active_step_logs(context, logs_height), name="active-step-logs", size=logs_height),
    )

    return layout


def render_focused_panel(context: ExecutionContext, run_id: int, panel: RenderableType) -> RenderableType:
    """Renders a particular panel in "full screen" mode"""
    layout = Layout(name="root")
    layout.split(
        Layout(generate_header(context, run_id), name="header", size=HEADER_HEIGHT),
        Layout(panel, name="focus-body", ratio=1),
    )
    return layout


def render_tui(context: ExecutionContext, run_id: int, console_height: int) -> RenderableType:
    if CURRENT_FOCUS == PanelFocus.Requests:
        return render_focused_panel(context, run_id, generate_requests(context, console_height - HEADER_HEIGHT))
    elif CURRENT_FOCUS == PanelFocus.Logs:
        return render_focused_panel(context, run_id, generate_active_step_logs(context, console_height - HEADER_HEIGHT))
    elif CURRENT_FOCUS == PanelFocus.Warnings:
        return render_focused_panel(context, run_id, generate_warnings(context, console_height - HEADER_HEIGHT))
    return render_unfocused_tui(context, run_id, console_height)


async def run_tui(console: Console, context: ExecutionContext, run_id: int, refresh_rate_ms: int = 500) -> None:
    """Runs the terminal user interface - expected to run in an infinite loop"""
    global CURRENT_FOCUS

    refresh_rate = timedelta(milliseconds=refresh_rate_ms).total_seconds()
    with Live(console=console, screen=True, transient=True, auto_refresh=False) as live, keypress.activate_keypress():

        while True:

            try:
                key = keypress.key_pressed()
                if key == "q":
                    CURRENT_FOCUS = None
                elif key == "w":
                    CURRENT_FOCUS = PanelFocus.Warnings
                elif key == "l":
                    CURRENT_FOCUS = PanelFocus.Logs
                elif key == "r":
                    CURRENT_FOCUS = PanelFocus.Requests

                # Ideally this would be a wait on the progress tracker that only returns when the progress has updated
                # In a future update - we might just do that for more efficient/responsive drawing
                live.update(render_tui(context, run_id, console.size.height), refresh=True)

                await asyncio.sleep(refresh_rate)
            except KeyboardInterrupt:
                logger.info("Shutting down TUI - Keyboard Interrupt")
                break
            except asyncio.CancelledError:
                logger.info("Shutting down TUI")
                break
            except Exception as exc:
                logger.info("Unhandled TUI exception", exc_info=exc)
                break
