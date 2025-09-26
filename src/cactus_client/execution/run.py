import logging

from rich.console import Console

from cactus_client.execution.build import build_execution_context
from cactus_client.execution.execute import execute_for_context
from cactus_client.model.config import GlobalConfig, RunConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.results.console import render_console


async def run_entrypoint(console: Console, global_config: GlobalConfig, run_config: RunConfig) -> None:
    """Handles running a full test procedure execution"""

    logging.basicConfig(level=logging.DEBUG)  # TODO - rework this

    async with await build_execution_context(global_config, run_config) as context:
        execute_result = await execute_for_context(context)

        await render_console(console, context, execute_result)
