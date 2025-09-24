import pytest

from cactus_client.model.execution import CheckResult


@pytest.fixture
def assert_check_result():
    def _assert_check_result(v: CheckResult, expected: bool):
        assert isinstance(v, CheckResult)
        assert v.passed is expected
        if not v.passed:
            assert v.description, "Description must be set on failure"

    return _assert_check_result
