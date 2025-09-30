from typing import Any

from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.table import Table

from cactus_client.constants import (
    CACTUS_CLIENT_VERSION,
    CACTUS_TEST_DEFINITIONS_VERSION,
)
from cactus_client.model.context import ExecutionContext
from cactus_client.model.output import RunOutputManager
from cactus_client.results.common import (
    ResultsEvaluation,
    context_relative_time,
)


def style_str(success: bool, content: Any) -> str:
    color = "green" if success else "red"
    return f"[{color}]{content}[/{color}]"


def render_console(
    console: Console, context: ExecutionContext, results: ResultsEvaluation, output_manager: RunOutputManager
) -> None:
    """Renders a "results report" to the console output"""

    exception_steps = [sr for sr in context.progress.all_results if sr.exc]

    success = results.has_passed()
    success_color = "green" if success else "red"

    panel_items: list[RenderableType] = [
        "",
        f"[{success_color} b]Run #{output_manager.run_id}",
        f"[{success_color}][b]{context.test_procedure_id}[/b] {context.test_procedure.description}[/{success_color}]",
        "",
        f"[b]Output:[/b] {output_manager.run_output_dir.absolute()}",
        "",
    ]

    metadata_table = Table(show_header=False, expand=True)
    metadata_table.add_column(style="b")
    metadata_table.add_column()
    metadata_table.add_row("Completed", style_str(results.execution_complete, results.execution_complete))
    metadata_table.add_row(
        "Steps", style_str(results.all_steps_passed, f"{results.total_steps_passed}/{results.total_steps} passed")
    )
    metadata_table.add_row("Warnings", style_str(results.no_warnings, f"[b]{results.total_warnings}[/b]"))
    metadata_table.add_row("XSD Errors", style_str(results.no_xsd_errors, f"[b]{results.total_xsd_errors}[/b]"))
    metadata_table.add_row("Started", context.created_at.strftime("%Y-%m-%d %H:%M:%S"))
    metadata_table.add_row("Duration", str(results.created_at - context.created_at))
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
        warnings_table = Table(title="Warnings", title_justify="left", show_header=False, expand=True)
        for warning in context.warnings.warnings:
            warnings_table.add_row(
                context_relative_time(context, warning.created_at),
                warning.step_execution.source.id,
                warning.message,
                style="red",
            )
        panel_items.append(warnings_table)

    # Steps table - show the results of any step executions grouped by their parent step
    steps_table = Table(title="Steps", title_justify="left", show_header=False, expand=True)

    for step in context.test_procedure.steps:
        progress = context.progress.progress_by_step_id.get(step.id, None)

        # "Header" row
        if progress is None or not progress.step_execution_completions:
            steps_table.add_row(step.id, "Not Executed", style="b yellow")
        elif progress.result and progress.result.is_passed():
            steps_table.add_row(step.id, "Success", style="b green")
        else:
            steps_table.add_row(step.id, "Failed", style="b red")

        # Then show each attempt
        completions = [] if progress is None else progress.step_execution_completions
        for step_completion in completions:
            if step_completion.exc:
                progress_result = f"Exception: {step_completion.exc}"
            elif step_completion.check_result and not step_completion.check_result.passed:
                progress_result = f"Check Failure: {step_completion.check_result.description}"
            else:
                progress_result = "Passed"
            steps_table.add_row(
                context_relative_time(context, step_completion.created_at),
                progress_result,
                style="green" if step_completion.is_success() else "red",
            )

        steps_table.add_section()
    panel_items.append(steps_table)

    if context.responses.responses:
        requests_table = Table(title="Requests", title_justify="left", show_header=False, expand=True)
        for response in context.responses.responses:

            if response.body:
                xsd = "\n".join(response.xsd_errors) if response.xsd_errors else "valid"
            else:
                xsd = ""
            requests_table.add_row(
                context_relative_time(context, response.request.created_at),
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
        border_style="green" if success else "red",
        expand=False,
        subtitle=f"🌵 cactus {CACTUS_CLIENT_VERSION} test definitions {CACTUS_TEST_DEFINITIONS_VERSION} 🌵",
    )

    console.print(cert_panel)
