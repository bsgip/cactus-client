import logging
from http import HTTPMethod, HTTPStatus
from typing import Any, cast

from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource

from cactus_client.action.server import (
    AcceptVersion,
    client_error_or_empty_list_request_for_step,
    client_error_request_for_step,
    get_resource_for_step,
    request_for_step,
)
from cactus_client.error import CactusClientError, RequestError
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import StoredResource

logger = logging.getLogger(__name__)


def generate_accept_versions(
    versions: list[str] | None, quality_factors: list[float] | None
) -> list[AcceptVersion] | None:
    if versions is None:
        return None

    if quality_factors is None:
        return [AcceptVersion(v, None) for v in versions]
    else:
        return [AcceptVersion(v, qf) for v, qf in zip(versions, quality_factors, strict=True)]


async def action_refresh_resource(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """Refresh a resource from the server using the resources href and update the resource store"""

    # Retrieve params
    resource_type: CSIPAusResource = CSIPAusResource(resolved_parameters["resource"])
    expect_rejection: bool | None = resolved_parameters.get("expect_rejection", None)
    expect_rejection_or_empty: bool = resolved_parameters.get("expect_rejection_or_empty", False)
    expect_not_acceptable: bool = resolved_parameters.get("expect_not_acceptable", False)
    expect_not_acceptable_or_v12: bool = resolved_parameters.get("expect_not_acceptable_or_v12", False)
    version_value_overrides: list[str] | None = resolved_parameters.get("version_value_overrides", None)
    version_qf_overrides: list[float] | None = resolved_parameters.get("version_qf_overrides", None)

    resource_store = context.discovered_resources(step)
    matching_resources: list[StoredResource] = resource_store.get_for_type(resource_type)
    accept_versions = generate_accept_versions(version_value_overrides, version_qf_overrides)

    if len(matching_resources) == 0:
        raise CactusClientError(f"Expected matching resources to refresh for resource {resource_type}. None found.")

    for sr in matching_resources:
        href = sr.resource.href

        if href is None:  # Skip resources without a href
            continue

        try:
            if expect_not_acceptable:
                # ONLY a NotAcceptable response is allowed - no evaluation of body
                server_response = await request_for_step(
                    step,
                    context,
                    href,
                    HTTPMethod.GET,
                    accept_versions=accept_versions,
                )
                if server_response.status != HTTPStatus.NOT_ACCEPTABLE:
                    raise CactusClientError(
                        f"Received HTTP status {server_response.status} but expected 406 with GET {href}."
                        + " Likely an issue with the server not interpreting supplied Accept: header correctly."
                    )
            elif expect_not_acceptable_or_v12:
                # ONLY a NotAcceptable OR a v1.2 schema valid response is allowed - no evaluation of body
                server_response = await request_for_step(
                    step,
                    context,
                    href,
                    HTTPMethod.GET,
                    accept_versions=accept_versions,
                    xml_validation_version=CSIPAusVersion.RELEASE_1_2,
                )
                if not server_response.is_success() and server_response.status != HTTPStatus.NOT_ACCEPTABLE:
                    raise CactusClientError(
                        f"Received HTTP status {server_response.status} but expected either a v1.2 success or a 406"
                        + f" with GET {href}. Likely an issue with the server interpretation of the Accept: header."
                    )
            elif expect_rejection:
                # Expect a 4XX and a specific sep2 ErrorResponse
                await client_error_request_for_step(
                    step, context, href, HTTPMethod.GET, accept_versions=accept_versions
                )
            elif expect_rejection_or_empty:
                # Expect a 4XX and a specific sep2 ErrorResponse or empty list
                if is_list_resource(resource_type):
                    await client_error_or_empty_list_request_for_step(
                        cast(Any, type(sr.resource)),
                        step,
                        context,
                        href,
                        HTTPMethod.GET,
                        accept_versions=accept_versions,
                    )
                else:
                    await client_error_request_for_step(
                        step, context, href, HTTPMethod.GET, accept_versions=accept_versions
                    )
            else:
                # If not expected to fail, actually request the resource and upsert in the resource store
                fetched_resource = await get_resource_for_step(
                    type(sr.resource), step, context, href, accept_versions=accept_versions
                )
                resource_store.upsert_resource(resource_type, sr.id.parent_id(), fetched_resource)

        except RequestError as exc:
            # We will bundle up RequestError as a "retryable" failure
            logger.error(f"Request error refreshing {href}", exc_info=exc)
            return ActionResult.failed(f"Request error: {exc}")

    return ActionResult.done()
