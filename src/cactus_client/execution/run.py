import asyncio
import logging
import logging.config

from rich.console import Console

from cactus_client.error import CactusClientException, ConfigException
from cactus_client.execution.build import build_execution_context
from cactus_client.execution.execute import execute_for_context
from cactus_client.execution.tui import run_tui
from cactus_client.model.config import GlobalConfig, RunConfig
from cactus_client.model.output import RunOutputFile, RunOutputManager
from cactus_client.results.common import ResultsEvaluation
from cactus_client.results.console import render_console
from cactus_client.results.requests import persist_all_request_data

logger = logging.getLogger(__name__)


async def run_entrypoint(global_config: GlobalConfig, run_config: RunConfig) -> bool:
    """Handles running a full test procedure execution - returns True if the test passes, False otherwise"""

    if not global_config.output_dir:
        raise ConfigException("The output_dir configuration setting is missing.")

    async with await build_execution_context(global_config, run_config) as context:

        # We're clear to start - generate the output directory
        output_manager = RunOutputManager(global_config.output_dir, run_config)

        # redirect all logs from the console to the run output file
        log_format = "%(asctime)s %(levelname)s %(name)s %(funcName)s - %(message)s"
        log_file_path = output_manager.file_path(RunOutputFile.ConsoleLogs)
        if run_config.headless:
            # Headless config also echoes the logs via stderr
            logging.config.dictConfig(
                {
                    "version": 1,
                    "disable_existing_loggers": False,
                    "formatters": {
                        "standard": {"format": log_format},
                    },
                    "handlers": {
                        "file_handler": {
                            "class": "logging.FileHandler",
                            "level": "DEBUG",
                            "formatter": "standard",
                            "filename": log_file_path,
                            "encoding": "utf8",
                        },
                        "stderr_handler": {
                            "class": "logging.StreamHandler",
                            "level": "DEBUG",
                            "formatter": "standard",
                            "stream": "ext://sys.stderr",
                        },
                    },
                    "root": {"level": "DEBUG", "handlers": ["file_handler", "stderr_handler"]},
                }
            )
        else:
            # When we have the TUI up - just write logs to the output file
            logging.basicConfig(
                filename=log_file_path,
                filemode="w",
                level=logging.DEBUG,
                format=log_format,
            )

        console = Console(record=False)

        # Do the execution - start the TUI and execute task to run at the same time
        execute_task = asyncio.create_task(execute_for_context(context))
        tasks: list[asyncio.Task] = [execute_task]
        if not run_config.headless:
            tasks.append(asyncio.create_task(run_tui(console=console, context=context, run_id=output_manager.run_id)))

        # Wait until any of the tasks is completed (the TUI task doesn't normally exit so it should be the execute task)
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if execute_task not in done:
                raise CactusClientException(
                    "It appears that the UI has exited prematurely. Aborting test run."
                    + f"Details at {log_file_path.absolute()}"
                )
            for task in pending:
                task.cancel()
                await task

            results = ResultsEvaluation(context, execute_task.result())
        except asyncio.CancelledError as exc:
            logger.error("Aborting test due to cancellation.", exc_info=exc)
            return False
        logger.info(f"Test passed: {results.has_passed()}")
        logger.debug(f"ResultsEvaluation: {results}")

        # Print the results to the console
        console.record = True
        render_console(console, context, results, output_manager)
        console.save_html(str(output_manager.file_path(RunOutputFile.Report).absolute()))
        console.print(f"Results stored at {output_manager.run_output_dir.absolute()}")

        # Generate other "results" outputs in the output directory
        persist_all_request_data(context, output_manager)

        return results.has_passed()
