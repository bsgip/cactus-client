from cactus_test_definitions.csipaus import CSIPAusResource
from treelib import Tree


def get_resource_tree() -> Tree:
    """Returns the tree of CSIPAusResource relationships with DeviceCapability forming the root"""

    tree = Tree()
    tree.create_node(identifier=CSIPAusResource.DeviceCapability, parent=None)
    tree.create_node(identifier=CSIPAusResource.Time, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePointList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.EndDeviceList, parent=CSIPAusResource.DeviceCapability)
    tree.create_node(identifier=CSIPAusResource.MirrorUsagePoint, parent=CSIPAusResource.MirrorUsagePointList)
    tree.create_node(identifier=CSIPAusResource.MirrorMeterReadingList, parent=CSIPAusResource.MirrorUsagePoint)
    tree.create_node(identifier=CSIPAusResource.MirrorMeterReading, parent=CSIPAusResource.MirrorMeterReadingList)
    tree.create_node(identifier=CSIPAusResource.EndDevice, parent=CSIPAusResource.EndDeviceList)
    tree.create_node(identifier=CSIPAusResource.FunctionSetAssignmentsList, parent=CSIPAusResource.EndDevice)
    tree.create_node(
        identifier=CSIPAusResource.FunctionSetAssignments, parent=CSIPAusResource.FunctionSetAssignmentsList
    )
    tree.create_node(identifier=CSIPAusResource.DERProgramList, parent=CSIPAusResource.FunctionSetAssignments)
    tree.create_node(identifier=CSIPAusResource.DERProgram, parent=CSIPAusResource.DERProgramList)
    tree.create_node(identifier=CSIPAusResource.DefaultDERControl, parent=CSIPAusResource.DERProgram)
    tree.create_node(identifier=CSIPAusResource.DERControlList, parent=CSIPAusResource.DERProgram)
    tree.create_node(identifier=CSIPAusResource.DERControl, parent=CSIPAusResource.DERControlList)
    tree.create_node(identifier=CSIPAusResource.DERList, parent=CSIPAusResource.EndDevice)
    tree.create_node(identifier=CSIPAusResource.DER, parent=CSIPAusResource.DERList)
    tree.create_node(identifier=CSIPAusResource.DERCapability, parent=CSIPAusResource.DER)
    tree.create_node(identifier=CSIPAusResource.DERSettings, parent=CSIPAusResource.DER)
    tree.create_node(identifier=CSIPAusResource.DERStatus, parent=CSIPAusResource.DER)

    return tree


def discover_resource_plan(tree: Tree, target_resources: list[CSIPAusResource]) -> list[CSIPAusResource]:
    """Given a list of resource targets and their hierarchy - calculate the ordered sequence of requests required
    to "walk" the tree such that all target_resources are hit (and nothing is double fetched)"""

    visit_order: list[CSIPAusResource] = []
    visited_nodes: set[CSIPAusResource] = set()
    for target in target_resources:
        for step in reversed(list(tree.rsearch(target))):
            if step in visited_nodes:
                continue
            visited_nodes.add(step)
            visit_order.append(step)

    return visit_order


# "discovery": {
#         "resources": ParameterSchema(True, ParameterType.ListCSIPAusResource),  # What resources to try and resolve?
#         "next_polling_window": ParameterSchema(
#             False, ParameterType.Boolean
#         ),  # If set - delay this until the upcoming polling window (eg- wait for the next whole minute)
#     }
