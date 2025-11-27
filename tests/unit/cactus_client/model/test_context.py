import unittest.mock as mock
from typing import Callable

from aiohttp import ClientSession

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.resource import StoredResourceId


def test_ExecutionContext_resource_annotations(
    testing_contexts_factory: Callable[[ClientSession], tuple[ExecutionContext, StepExecution]],
):
    """Sanity checking the persistence behaviour of resource_annotations"""
    context, step = testing_contexts_factory(mock.Mock())

    # These IDs are all distinct
    id1 = StoredResourceId.from_parent(None, "/id1")
    id2 = StoredResourceId.from_parent(None, "/id2")
    id3 = StoredResourceId.from_parent(id1, "/id1")
    id4 = StoredResourceId.from_parent(id1, "/id2")
    id4_clone = StoredResourceId.from_parent(StoredResourceId.from_parent(None, "/id1"), "/id2")

    assert id4 == id4_clone

    id1_resources = context.resource_annotations(step, id1)
    assert id1_resources.alias is None
    assert id1_resources.tag_creations == {}
    id1_resources.alias = "alias1"

    assert id1_resources is context.resource_annotations(step, id1), "Refetching should yield the new value"

    id2_resources = context.resource_annotations(step, id2)
    assert id2_resources.alias is None
    assert id2_resources.tag_creations == {}
    assert id1_resources.alias == "alias1"

    id4_resources = context.resource_annotations(step, id4)
    id4_resources.alias = "alias4"
    assert context.resource_annotations(step, id4) is not context.resource_annotations(step, id1)
    assert context.resource_annotations(step, id4) is not context.resource_annotations(step, id2)
    assert context.resource_annotations(step, id4) is not context.resource_annotations(step, id3)
    assert context.resource_annotations(step, id4_clone).alias == "alias4"
