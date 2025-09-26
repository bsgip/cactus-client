from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cactus_client.model.config import RunConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult


async def render_console(console: Console, context: ExecutionContext, execute_result: ExecutionResult) -> None:
    console.clear()

    success = execute_result.completed and all([sr.is_passed() for sr in context.progress.step_results])
    success_text = "success" if success else "failed"

    panel = Panel(
        f"{context.test_procedure.description}\n{context.created_at}",
        title=f"[b]{context.test_procedure_id}[/b] {success_text}",
        style="green" if success else "red",
    )

    console.print(panel)
