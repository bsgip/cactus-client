from typing import Any

from rich.columns import Columns
from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table

from cactus_client.model.config import GlobalConfig, RunConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult
from cactus_client.time import utc_now


def style_str(success: bool, content: Any) -> str:
    color = "green" if success else "red"
    return f"[{color}]{content}[/{color}]"


async def render_console(console: Console, context: ExecutionContext, execute_result: ExecutionResult) -> None:

    all_steps_passed = all((sr.is_passed() for sr in context.progress.step_results))
    total_warnings = len(context.warnings.warnings)
    total_xsd_errors = sum((bool(r.xsd_errors) for r in context.responses.responses))
    exception_steps = [sr for sr in context.progress.step_results if sr.exc]

    success = execute_result.completed and all_steps_passed and total_warnings == 0 and total_xsd_errors == 0
    successful_steps = sum([sr.is_passed() for sr in context.progress.step_results])
    success_color = "green" if success else "red"

    panel_items: list[RenderableType] = [
        "",
        f"[{success_color}][b]{context.test_procedure_id}[/b] {context.test_procedure.description}[/{success_color}]",
        "",
    ]

    metadata_table = Table(show_header=False, expand=True)
    metadata_table.add_column(style="b")
    metadata_table.add_column()
    metadata_table.add_row("Completed", style_str(execute_result.completed, execute_result.completed))
    metadata_table.add_row(
        "Steps", style_str(all_steps_passed, f"{successful_steps}/{len(context.test_procedure.steps)} passed")
    )
    metadata_table.add_row("Warnings", style_str(total_warnings == 0, f"[b]{total_warnings}[/b]"))
    metadata_table.add_row("XSD Errors", style_str(total_xsd_errors == 0, f"[b]{total_xsd_errors}[/b]"))
    metadata_table.add_row("Started", context.created_at.strftime("%Y-%m-%d %H:%M:%S"))
    metadata_table.add_row("Duration", str(execute_result.created_at - context.created_at))
    panel_items.append(metadata_table)

    server_table = Table(title="Server", title_justify="left", show_header=False, expand=True)
    server_table.add_column(style="b")
    server_table.add_column()
    server_table.add_row("dcap", context.server_config.device_capability_uri)
    server_table.add_row("verify", str(context.server_config.verify_ssl))
    panel_items.append(server_table)

    client_table = Table(title="Client(s)", title_justify="left", show_header=False, expand=True)
    client_table.add_column(style="b")
    client_table.add_column()
    for client_alias, client in sorted(context.clients_by_alias.items()):
        client_table.add_row(f"{client_alias}", client.client_config.lfdi)
    panel_items.append(client_table)

    if context.warnings.warnings:
        warnings_table = Table(title="Warnings", title_justify="left", show_header=False)
        for warning in context.warnings.warnings:
            warnings_table.add_row(warning, style="red")
        panel_items.append(warnings_table)

    requests_table = Table(title="Requests", title_justify="left", show_header=False, expand=True)
    for response in context.responses.responses:

        xsd = "\n".join(response.xsd_errors) if response.xsd_errors else "valid"
        requests_table.add_row(
            response.method,
            response.url,
            str(response.status),
            xsd,
            style="red" if response.xsd_errors else "green",
        )
    panel_items.append(requests_table)

    if exception_steps:
        exc_table = Table(title="Exceptions", title_justify="left", show_header=False, expand=True, style="red")
        for step_result in exception_steps:
            exc_table.add_row(step_result.step.id, str(step_result.exc))
        panel_items.append(exc_table)

    cert_panel = Panel(
        Group(*panel_items),
        title=f"{'[green]success[/green]' if success else '[red]failed[/red]'}",
        expand=False,
    )

    console.print(cert_panel)
