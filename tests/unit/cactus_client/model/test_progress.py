import re
from unittest import mock

import pytest
from assertical.fake.generator import generate_class_instance
from multidict import CIMultiDict

from cactus_client.model.http import ServerRequest, ServerResponse
from cactus_client.model.progress import ResponseTracker


def _generate_test_server_response(*, xsd_errors: list[str] | None) -> ServerResponse:
    """Server response factory."""
    return ServerResponse(
        url="some-url",
        method="GET",
        status=123,
        body="",
        location=None,
        content_type=None,
        xsd_errors=xsd_errors,
        headers=CIMultiDict(),
        request=generate_class_instance(ServerRequest),
        client_alias=""
    )


@pytest.mark.parametrize("xsd_errors,strict", [([], True), (None, False), ([], False), (None, True)])
@pytest.mark.asyncio
async def test_response_tracker_log_response_body_no_xsd_errors(xsd_errors: list[str] | None, strict: bool):
    """Ensures appropriate actions taken when no xsd errors."""
    server_response = _generate_test_server_response(xsd_errors=xsd_errors)
    client_alias = "some_client"
    response_tracker = ResponseTracker()

    with mock.patch("cactus_client.model.progress.logger") as mock_logger:
        await response_tracker.log_response_body(r=server_response, client_alias=client_alias, strict=strict)

        mock_logger.warning.assert_not_called()
        mock_logger.error.assert_not_called()


@pytest.mark.parametrize("xsd_errors", [["some error 123", "another error 456"], ["a single error 45645"]])
@pytest.mark.asyncio
async def test_response_tracker_log_response_body_strict_with_xsd_errors(xsd_errors: list[str]):
    """Ensures appropriate actions taken when strict mode in effect with xsd errors."""
    server_response = _generate_test_server_response(xsd_errors=xsd_errors)
    client_alias = "some_client"
    response_tracker = ResponseTracker()

    with mock.patch("cactus_client.model.progress.logger") as mock_logger:
        await response_tracker.log_response_body(r=server_response, client_alias=client_alias, strict=True)

        mock_logger.warning.assert_not_called()
        mock_logger.error.assert_called_once()
        logged_message = mock_logger.error.call_args.args[0]
        for err_text in xsd_errors:
            assert re.search(err_text, logged_message)


@pytest.mark.parametrize("xsd_errors", [["some error 123", "another error 456"], ["a single error 45645"]])
@pytest.mark.asyncio
async def test_response_tracker_log_response_body_not_strict_with_xsd_errors(xsd_errors: list[str]):
    """Ensures appropriate actions taken when strict mode not in effect and with xsd errors."""
    server_response = _generate_test_server_response(xsd_errors=xsd_errors)
    client_alias = "some_client"
    response_tracker = ResponseTracker()

    with mock.patch("cactus_client.model.progress.logger") as mock_logger:
        await response_tracker.log_response_body(r=server_response, client_alias=client_alias, strict=False)

        mock_logger.warning.assert_called_once()
        mock_logger.error.assert_not_called()
        logged_message = mock_logger.warning.call_args.args[0]
        for err_text in xsd_errors:
            assert re.search(err_text, logged_message)


