from typing import Any
from urllib.parse import urljoin

from cactus_test_definitions.csipaus import (
    CSIPAusResource,
    CSIPAusVersion,
    is_list_resource,
)

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


def do_resources_check(
    resources: list[CSIPAusResource], step: StepExecution, context: ExecutionContext
) -> CheckResult | None:
    """Checks whether specific resources exist in the resource store"""
    resource_store = context.discovered_resources(step)
    missing_resources: list[CSIPAusResource] = []
    for resource in resources:
        if not resource_store.get_for_type(resource):
            missing_resources.append(resource)
    if missing_resources:
        return CheckResult(False, f"Couldn't find resources: {','.join(missing_resources)}")

    return None  # Check passed


def do_links_check(links: list[CSIPAusResource], step: StepExecution, context: ExecutionContext) -> CheckResult | None:
    resource_store = context.discovered_resources(step)

    missing_parent_lists: list[CSIPAusResource] = []
    missing_parent_links: list[CSIPAusResource] = []
    for resource in links:
        parent_resource = context.resource_tree.parent_resource(resource)
        if parent_resource is None:
            return CheckResult(
                False,
                f"Resource {resource} has no known way to link to it (likely a test error).",
            )

        # Depending on the type of parent - we need to check the link existence in different ways.
        parent_stored_resources = resource_store.get_for_type(parent_resource)
        if is_list_resource(parent_resource):
            # List resources are easy - If we have a list, then we have a link to child (i.e. we paginate the parent)
            # eg: EndDeviceList will have EndDevice resources (or at least a means for finding them)
            if not parent_stored_resources:
                missing_parent_lists.append(resource)

        elif not any(resource in sr.resource_link_hrefs for sr in parent_stored_resources):
            # Other resources need to check for any number of Link.href references that point to our type
            # eg: DER.DERSettingsLink.href -> DERSettings
            missing_parent_links.append(resource)

    if missing_parent_links:
        return CheckResult(
            False,
            f"Couldn't find any Link references to: {','.join(missing_parent_links)}",
        )

    if missing_parent_lists:
        return CheckResult(
            False,
            f"Couldn't find any parent List's containing: {','.join(missing_parent_lists)}",
        )

    return None  # Check passed


def check_discovered(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified resources/links exist in the resource store"""
    resources: list[CSIPAusResource] = resolved_parameters.get("resources", [])
    links: list[CSIPAusResource] = resolved_parameters.get("links", [])

    failed_check = do_resources_check(resources, step, context)
    if failed_check is not None:
        return failed_check

    failed_check = do_links_check(links, step, context)
    if failed_check is not None:
        return failed_check

    return CheckResult(True, None)


def well_known_schema_version(version: CSIPAusVersion) -> str:
    """Generates the supportedSchemaVersions key that a .well-known/csipaus document will use for version"""
    if version == CSIPAusVersion.RELEASE_1_2:
        return "https://csipaus.org/ns"  # v1.2 predates versioned namespaces
    return f"https://csipaus.org/ns/{version}"


def check_well_known_discovered(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Filters/counts the supportedSchemaVersions entries from the most recent .well-known/csipaus discovery"""
    minimum_count: int | None = resolved_parameters.get("minimum_count", None)
    maximum_count: int | None = resolved_parameters.get("maximum_count", None)
    versions: list[CSIPAusVersion] | None = resolved_parameters.get("versions", None)
    matches_dcap: bool = resolved_parameters.get("matches_dcap", False)

    well_known = context.discovered_well_known(step)
    entries = well_known.supported_schema_versions if well_known else {}
    configured_dcap_uri = context.server_config.device_capability_uri
    schema_versions = None if versions is None else {well_known_schema_version(v) for v in versions}

    total_matches = 0
    rejection_details: list[str] = []
    for schema_version, entry in entries.items():
        if schema_versions is not None and schema_version not in schema_versions:
            rejection_details.append(f"{schema_version}: not a requested version")
            continue

        # dcap URIs may be relative to the server or absolute - resolving them both allows a direct comparison
        if matches_dcap and not any(urljoin(configured_dcap_uri, dcap) == configured_dcap_uri for dcap in entry.dcap):
            rejection_details.append(f"{schema_version}: dcap {entry.dcap} doesn't include {configured_dcap_uri}")
            continue

        total_matches += 1

    metadata = f"Found {len(entries)} .well-known entries, {total_matches} matched criteria"
    rejection_info = f" Rejections: {'; '.join(rejection_details)}" if rejection_details else ""

    if minimum_count is not None and total_matches < minimum_count:
        return CheckResult(False, f"{metadata}. Expected at least {minimum_count}.{rejection_info}")

    if maximum_count is not None and total_matches > maximum_count:
        return CheckResult(False, f"{metadata}. Expected at most {maximum_count}")

    return CheckResult(True, metadata)
