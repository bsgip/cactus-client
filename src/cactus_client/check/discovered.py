from typing import Any

from cactus_test_definitions.csipaus import CSIPAusResource, is_list_resource

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution


def do_resources_check(
    resources: list[CSIPAusResource], step: StepExecution, context: ExecutionContext
) -> CheckResult | None:
    """Checks whether specific resources exist in the resource store"""
    resource_store = context.discovered_resources(step)
    missing_resources: list[CSIPAusResource] = []
    for resource in resources:
        if not resource_store.get(resource):
            missing_resources.append(resource)
    if missing_resources:
        return CheckResult(False, f"Couldn't find resources: {','.join(resources)}")

    return None  # Check passed


def do_links_check(links: list[CSIPAusResource], step: StepExecution, context: ExecutionContext) -> CheckResult | None:
    resource_store = context.discovered_resources(step)

    missing_parent_lists: list[CSIPAusResource] = []
    missing_parent_links: list[CSIPAusResource] = []
    for resource in links:
        parent_resource = context.resource_tree.parent_resource(resource)
        if parent_resource is None:
            return CheckResult(False, f"Resource {resource} has no known way to link to it (likely a test error).")

        # Depending on the type of parent - we need to check the link existence in different ways.
        parent_stored_resources = resource_store.get(parent_resource)
        if is_list_resource(parent_resource):
            # List resources are easy - If we have a list, then we have a link to child (i.e. we paginate the parent)
            # eg: EndDeviceList will have EndDevice resources (or at least a means for finding them)
            if not parent_stored_resources:
                missing_parent_lists.append(resource)

        elif not any((resource in sr.resource_link_hrefs for sr in parent_stored_resources)):
            # Other resources need to check for any number of Link.href references that point to our type
            # eg: DER.DERSettingsLink.href -> DERSettings
            missing_parent_links.append(resource)

    if missing_parent_links:
        return CheckResult(False, f"Couldn't find any Link references to: {','.join(missing_parent_links)}")

    if missing_parent_lists:
        return CheckResult(False, f"Couldn't find any parent List's containing: {','.join(missing_parent_lists)}")

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
