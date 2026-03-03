from pathlib import Path
from unittest.mock import MagicMock

from assertical.fake.generator import generate_class_instance
from multidict import CIMultiDict
from rich.console import Console

from cactus_test_definitions.server.actions import Action
from cactus_test_definitions.server.test_procedures import Step, TestProcedure, TestProcedureId

from cactus_client.model.config import ServerConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ExecutionResult, StepExecutionList
from cactus_client.model.http import ServerRequest, ServerResponse
from cactus_client.model.progress import ProgressTracker, ResponseTracker, WarningTracker
from cactus_client.model.resource import CSIPAusResourceTree
from cactus_client.results.common import ResultsEvaluation
from cactus_client.results.console import render_console

from tests.conftest import make_client_context


def _make_context(clients_by_alias: dict) -> ExecutionContext:
    # Need at least one step so the steps table has columns (Rich bug with expand=True on empty tables)
    step = generate_class_instance(Step, action=Action(type="dummy"))
    tree = CSIPAusResourceTree()
    return ExecutionContext(
        test_procedure_id=TestProcedureId.S_ALL_01,
        test_procedure=generate_class_instance(TestProcedure, steps=[step]),
        test_procedures_version="vtest",
        output_directory=Path("."),
        dcap_path="/dcap",
        server_config=generate_class_instance(ServerConfig),
        clients_by_alias=clients_by_alias,
        steps=StepExecutionList(),
        warnings=WarningTracker(),
        progress=ProgressTracker(),
        responses=ResponseTracker(),
        resource_tree=tree,
    )


def _make_response(url: str, client_alias: str) -> ServerResponse:
    return ServerResponse(
        url=url,
        body="",
        content_type=None,
        location=None,
        method="GET",
        request=ServerRequest(url, "GET", None, {}),
        headers=CIMultiDict(),
        status=200,
        xsd_errors=None,
        client_alias=client_alias,
    )


def _make_output_manager() -> MagicMock:
    m = MagicMock()
    m.run_id = 1
    m.run_output_dir.absolute.return_value = Path("/tmp/test")
    return m


def _render(context: ExecutionContext) -> str:
    console = Console(record=True, width=120, force_terminal=True)
    render_console(console, context, ResultsEvaluation(context, ExecutionResult(True)), _make_output_manager())
    output = console.export_text()
    print(output)  # visible with pytest -s
    return output


def test_render_console_url_stripped(assertical_extensions):
    """URLs in the requests table should show only the path, not scheme+host."""
    tree = CSIPAusResourceTree()
    context = _make_context({"clienta": make_client_context("clienta", tree)})
    context.responses.responses.append(_make_response("http://localhost:8080/dcap/edev", "clienta"))

    output = _render(context)

    assert "/dcap/edev" in output
    assert "http://localhost" not in output


def test_render_console_multi_client_shows_alias_column(assertical_extensions):
    """When more than one client is present, each row in the requests table should show the client alias."""
    tree = CSIPAusResourceTree()
    context = _make_context(
        {
            "clienta": make_client_context("clienta", tree),
            "clientb": make_client_context("clientb", tree),
        }
    )
    context.responses.responses.append(_make_response("http://localhost/dcap", "clienta"))
    context.responses.responses.append(_make_response("http://localhost/dcap/edev", "clientb"))

    output = _render(context)

    assert "clienta" in output
    assert "clientb" in output
