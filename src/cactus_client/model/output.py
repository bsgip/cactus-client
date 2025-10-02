import os
import typing
from enum import StrEnum
from pathlib import Path

from cactus_client.constants import (
    CACTUS_CLIENT_VERSION,
    CACTUS_TEST_DEFINITIONS_VERSION,
    ENVOY_SCHEMA_VERSION,
)
from cactus_client.error import ConfigException
from cactus_client.model.config import RunConfig

RUN_ID_FILE_NAME = Path(".runid")  # Stores an incrementing integer


class RunOutputFile(StrEnum):
    #
    # General metadata files - used by the cactus tools but useful for end users too
    #
    VersionsMetadata = ".versions"  # Pure metadata dump of compiled versions
    TestProcedureId = ".testprocedureid"  # Raw text of the TestProcedureId that was run
    CSIPAusVersion = ".csipaustarget"  # Raw text of the CSIPAusVersion that was run
    ClientIds = ".clientids"  # Raw text of the client ID's supplied to the run command

    #
    # Output files - Only created by the cactus tool (never read) - These are the primary files a user will care about
    #
    ConsoleLogs = "cactus.log"  # Logs from the python logging API
    Report = "report.html"  # Dump of the console "results" in a portable format (eg HTML)

    #
    # Output subdirectories
    #
    RequestsDirectory = "requests/"  # The base directory for all logging of request data


# Depending on the OS - implement a lock/unlock function that can force exclusive use of a file
if os.name == "nt":
    # import msvcrt

    def lock_file(file: typing.IO) -> None:
        # Disabled - requires additional windows testing
        # msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, os.path.getsize(file) or 1)
        pass

    def unlock_file(file: typing.IO) -> None:
        # Disabled - requires additional windows testing
        # try:
        #     msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, os.path.getsize(file) or 1)
        # except OSError:
        #     pass  # file might be closed already
        pass

else:
    import fcntl

    def lock_file(file: typing.IO) -> None:
        fcntl.flock(file, fcntl.LOCK_EX)

    def unlock_file(file: typing.IO) -> None:
        fcntl.flock(file, fcntl.LOCK_UN)


def increment_run_id_counter(run_id_path: Path) -> int:
    """Given a reference to a run_id file - read the current value (defaulting to 1) - increment it and return the value

    The incremented value will be written back to the file (with attempts being made to lock the file to prevent race
    conditions)"""

    # base case -
    if not os.path.exists(run_id_path):
        with open(run_id_path, "w") as f:
            f.write("1")
            return 1

    # The file exists - lets open it, lock it and read the current value before updating it again
    with open(run_id_path, "r+") as f:
        try:
            lock_file(f)

            f.seek(0)
            content = f.read().strip()

            try:
                current_value = int(content)
            except (ValueError, TypeError):
                current_value = 0

            new_value = current_value + 1

            f.seek(0)
            f.truncate()
            f.write(str(new_value))
            f.flush()

        finally:
            unlock_file(f)

    return new_value


class RunOutputManager:
    """A collection of outputs / paths associated with a single execution of a run"""

    base_output_dir: Path  # The configured working directory for all outputs
    run_id: int  # Will be assigned at generation
    run_output_dir: Path

    def __init__(self, base_output_dir: str, run_config: RunConfig):
        """Creates a new run output file manager from the specified output directory"""

        self.base_output_dir = Path(base_output_dir)
        run_id_file = self.base_output_dir / RUN_ID_FILE_NAME

        self.run_id = increment_run_id_counter(run_id_file)
        self.run_output_dir = self.base_output_dir / Path(f"run{self.run_id:03}")

        # Start initialising the run output directory with the default metadata files
        if self.run_output_dir.exists():
            raise ConfigException(
                f"{self.run_output_dir.absolute()} already exists. Check {run_id_file.absolute()} value. Aborting!"
            )

        self.run_output_dir.mkdir()
        with open(self.file_path(RunOutputFile.TestProcedureId), "w") as fp:
            fp.write(str(run_config.test_procedure_id))
        with open(self.file_path(RunOutputFile.CSIPAusVersion), "w") as fp:
            fp.write(str(run_config.csip_aus_version))
        with open(self.file_path(RunOutputFile.VersionsMetadata), "w") as fp:
            fp.writelines(
                [
                    f"CACTUS_TEST_DEFINITIONS_VERSION={CACTUS_TEST_DEFINITIONS_VERSION}\n",
                    f"CACTUS_CLIENT_VERSION={CACTUS_CLIENT_VERSION}\n",
                    f"ENVOY_SCHEMA_VERSION={ENVOY_SCHEMA_VERSION}\n",
                ]
            )
        with open(self.file_path(RunOutputFile.ClientIds), "w") as fp:
            fp.write("\n".join(run_config.client_ids))

    def file_path(self, file: RunOutputFile) -> Path:
        """Gets the path to a file in run output directory (no protection is made for relative paths)"""
        return self.run_output_dir / Path(file.value)
