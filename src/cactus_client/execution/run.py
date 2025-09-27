import asyncio
import logging

from rich.console import Console
from rich.live import Live

from cactus_client.error import CactusClientException, ConfigException
from cactus_client.execution.build import build_execution_context
from cactus_client.execution.execute import execute_for_context
from cactus_client.execution.tui import run_tui
from cactus_client.model.config import GlobalConfig, RunConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.output import RunOutputFile, RunOutputManager
from cactus_client.results.console import render_console

logger = logging.getLogger(__name__)


async def run_entrypoint(global_config: GlobalConfig, run_config: RunConfig) -> None:
    """Handles running a full test procedure execution"""

    if not global_config.output_dir:
        raise ConfigException("The output_dir configuration setting is missing.")

    async with await build_execution_context(global_config, run_config) as context:

        # We're clear to start - generate the output directory
        output_manager = RunOutputManager(global_config.output_dir, run_config)

        # redirect all logs from the console to the run output file
        logging.basicConfig(
            filename=output_manager.file_path(RunOutputFile.ConsoleLogs),
            filemode="w",
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s %(funcName)s - %(message)s",
        )

        console = Console(record=False)

        # Do the execution - start the TUI and execute task to run at the same time
        execute_task = asyncio.create_task(execute_for_context(context))
        tui_task = asyncio.create_task(run_tui(console=console, context=context, run_id=output_manager.run_id))

        # Wait until any of the tasks is completed (the TUI task doesn't normally exit so it should be the execute task)
        done, pending = await asyncio.wait([execute_task, tui_task], return_when=asyncio.FIRST_COMPLETED)
        if execute_task not in done:
            raise CactusClientException("It appears that the UI has exited prematurely. Aborting test run.")
        for task in pending:
            task.cancel()
            await task
        execute_result = execute_task.result()

        # Print the results to the console
        console.record = True
        render_console(console, context, execute_result, output_manager)
        console.save_html(str(output_manager.file_path(RunOutputFile.Report).absolute()))
