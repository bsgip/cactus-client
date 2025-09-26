from rich.console import Console
from rich.table import Table

from cactus_client.model.context import ExecutionContext


async def render_console(console: Console, context: ExecutionContext) -> None:
    console.clear()

    # context.progress
