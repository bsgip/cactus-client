import logging
from dataclasses import dataclass
from datetime import datetime
from http import HTTPMethod, HTTPStatus

from aiohttp import ClientResponse
from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.model.execution import StepExecution
from cactus_client.model.http import ServerResponse
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


class WarningTracker:
    """A warning represents some form of (minor) failure of a test that doesn't block the execution but should be
    reported at the end. Example warnings could include a non critical XSD error."""

    warnings: list[str]

    def __init__(self) -> None:
        self.warnings = []

    def log_resource_warning(self, type: CSIPAusResource, message: str) -> None:
        warning = f"{type}: {message}"
        self.warnings.append(warning)
        logger.warning(warning)


class ProgressTracker:
    """A utility for allowing step execution operations to update the user facing progress of those operations"""

    def __init__(self) -> None:
        pass

    async def log_step_progress(self, step: StepExecution, message: str) -> None:
        """Updates the progress information for a specific step"""
        logger.info(f"{step.source.name}[{step.repeat_number}] Attempt {step.attempts}: {message}")


class ResponseTracker:
    """A utility for tracking raw responses received from the utility server and their validity"""

    responses: list[ServerResponse]

    def __init__(self):
        self.responses = []

    async def log_response_body(self, r: ServerResponse):
        self.responses.append(r)
        logger.info(f"{r.method} {r.uri} Yielded {r.status}: Received body of length {len(r.body)}.")
