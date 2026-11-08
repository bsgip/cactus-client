import subprocess
import sys

import pytest

from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION, CACTUS_CLIENT_VERSION


@pytest.mark.parametrize("flag", ["-v", "--version"])
def test_version_flag(flag: str):
    # Run in a subprocess - cactus_client.cli.main can't be imported under pytest's stdin capture (see keypress.py)
    result = subprocess.run(
        [sys.executable, "-m", "cactus_client.cli.main", flag], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr

    lines = result.stdout.strip().splitlines()

    # Sanity check expected contents
    assert any(ACTIVE_CSIP_AUS_VERSION.value in line for line in lines)
    assert any(CACTUS_CLIENT_VERSION in line for line in lines)
