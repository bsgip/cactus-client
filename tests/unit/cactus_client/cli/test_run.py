import argparse
import sys

import pytest


class _FakeStdin:
    """Minimal stdin stub for importing CLI modules that inspect stdin at import time."""

    def fileno(self) -> int:
        return 0


def _build_run_parser(monkeypatch: pytest.MonkeyPatch) -> argparse.ArgumentParser:
    """Build an isolated parser with only the `cactus run` subcommand registered."""
    monkeypatch.setattr(sys, "stdin", _FakeStdin())

    from cactus_client.cli import run

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    run.add_sub_commands(subparsers)
    return parser


def test_run_refetch_delay_ms_is_parsed_as_an_int(monkeypatch: pytest.MonkeyPatch) -> None:
    """`--refetch-delay-ms` should be coerced to an integer before the run action sees it."""
    args = _build_run_parser(monkeypatch).parse_args(["run", "--refetch-delay-ms", "250", "procedure-id", "client-a"])

    assert args.refetch_delay_ms == 250
    assert isinstance(args.refetch_delay_ms, int)


def test_run_refetch_delay_ms_rejects_negative_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Negative millisecond values should fail fast during CLI parsing."""
    with pytest.raises(SystemExit):
        _build_run_parser(monkeypatch).parse_args(["run", "--refetch-delay-ms", "-1", "procedure-id", "client-a"])
