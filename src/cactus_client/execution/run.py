import logging

from rich.console import Console

from cactus_client.error import ConfigException
from cactus_client.execution.build import build_execution_context
from cactus_client.execution.execute import execute_for_context
from cactus_client.model.config import GlobalConfig, RunConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.output import RunOutputFile, RunOutputManager
from cactus_client.results.console import render_console


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

        console = Console(record=True)

        # Do the execution
        execute_result = await execute_for_context(context)

        # t1 = asyncio.create_task(execute_for_context(context))
        # t2 = asyncio.create_task(task2())

        # # Wait until any of the tasks is completed
        # done, pending = await asyncio.wait(
        #     [t1, t2],
        #     return_when=asyncio.FIRST_COMPLETED
        # )

        # Print the results to the console
        console.clear()
        await render_console(console, context, execute_result, output_manager)
        console.save_html(str(output_manager.file_path(RunOutputFile.Report).absolute()))
