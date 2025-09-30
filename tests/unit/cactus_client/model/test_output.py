from pathlib import Path
from tempfile import TemporaryDirectory

from assertical.fake.generator import generate_class_instance

from cactus_client.model.config import RunConfig
from cactus_client.model.output import (
    RunOutputFile,
    RunOutputManager,
    increment_run_id_counter,
)


def test_increment_run_id_counter():
    with TemporaryDirectory() as tmpdir:
        for expected in range(1, 20):
            actual_1 = increment_run_id_counter(Path(tmpdir) / "my.counter")
            actual_2 = increment_run_id_counter(Path(tmpdir) / "my.other.counter")
            assert isinstance(actual_1, int)
            assert isinstance(actual_2, int)
            assert expected == actual_1
            assert expected == actual_2


def test_RunOutputManager():
    with TemporaryDirectory() as tmpdir:
        run_config_1 = generate_class_instance(RunConfig, seed=1)
        run_config_2 = generate_class_instance(RunConfig, seed=2)
        om_1 = RunOutputManager(tmpdir, run_config_1)
        om_2 = RunOutputManager(tmpdir, run_config_2)
        om_1_copy = RunOutputManager(tmpdir, run_config_1)

        # Each output manager should segment the files seperately
        for file in RunOutputFile:
            assert om_1.file_path(file) != om_2.file_path(file)
            assert om_1_copy.file_path(file) != om_1.file_path(file)

        # version metadata should be constant
        with open(om_1.file_path(RunOutputFile.VersionsMetadata), "r") as fp1:
            with open(om_2.file_path(RunOutputFile.VersionsMetadata), "r") as fp2:
                assert fp1.read() == fp2.read()

        # We write the expected test procedure id
        with open(om_1.file_path(RunOutputFile.TestProcedureId), "r") as fp1:
            assert fp1.read() == run_config_1.test_procedure_id

        with open(om_2.file_path(RunOutputFile.TestProcedureId), "r") as fp1:
            assert fp1.read() == run_config_2.test_procedure_id
