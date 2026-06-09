import zipfile
from pathlib import Path

from cactus_test_definitions.server.test_procedures import TestProcedureId

from cactus_client.model.output import RunOutputFile
from cactus_client.results.compliance import create_bundle


def make_run(output_dir: Path, run_number: int, test_id: TestProcedureId, result: str) -> Path:
    """Create a fake run directory matching what RunOutputManager produces."""
    run_dir = output_dir / f"run {run_number:03} - {test_id}"
    run_dir.mkdir()
    (run_dir / RunOutputFile.TestProcedureId).write_text(str(test_id))
    (run_dir / RunOutputFile.Result).write_text(result)
    (run_dir / RunOutputFile.ConsoleLogs).write_text("log line\n")
    return run_dir


def test_create_bundle_all_passed(tmp_path: Path):
    make_run(tmp_path, 1, TestProcedureId.S_ALL_01, "PASS")
    make_run(tmp_path, 2, TestProcedureId.S_ALL_02, "PASS")
    targets = [TestProcedureId.S_ALL_01, TestProcedureId.S_ALL_02]

    zip_path, all_passed = create_bundle(tmp_path, targets)

    assert all_passed is True
    assert zip_path == tmp_path / "cactus-bundle.passed.zip"
    assert zip_path.exists()

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        summary = zf.read("compliance-report.html").decode()
    # Every target's run dir contents are bundled
    assert "run 001 - S-ALL-01/cactus.log" in names
    assert "run 002 - S-ALL-02/cactus.log" in names
    # A top-level HTML summary of the targets is included
    assert "compliance-report.html" in names
    assert "S-ALL-01" in summary and "PASS" in summary


def test_create_bundle_failed_on_not_run(tmp_path: Path):
    make_run(tmp_path, 1, TestProcedureId.S_ALL_01, "PASS")
    targets = [TestProcedureId.S_ALL_01, TestProcedureId.S_ALL_02]  # S_ALL_02 never run

    zip_path, all_passed = create_bundle(tmp_path, targets)

    assert all_passed is False
    assert zip_path == tmp_path / "cactus-bundle.failed.zip"
    assert zip_path.exists()


def test_create_bundle_failed_on_fail_result(tmp_path: Path):
    make_run(tmp_path, 1, TestProcedureId.S_ALL_01, "FAIL")

    _, all_passed = create_bundle(tmp_path, [TestProcedureId.S_ALL_01])

    assert all_passed is False


def test_create_bundle_uses_latest_run(tmp_path: Path):
    make_run(tmp_path, 1, TestProcedureId.S_ALL_01, "FAIL")
    make_run(tmp_path, 5, TestProcedureId.S_ALL_01, "PASS")  # latest passes

    zip_path, all_passed = create_bundle(tmp_path, [TestProcedureId.S_ALL_01])

    assert all_passed is True
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    assert "run 005 - S-ALL-01/cactus.log" in names
    assert "run 001 - S-ALL-01/cactus.log" not in names


def test_create_bundle_removes_stale_bundle(tmp_path: Path):
    stale = tmp_path / "cactus-bundle.failed.zip"
    stale.write_text("old")
    make_run(tmp_path, 1, TestProcedureId.S_ALL_01, "PASS")

    create_bundle(tmp_path, [TestProcedureId.S_ALL_01])

    assert not stale.exists()
    assert (tmp_path / "cactus-bundle.passed.zip").exists()
