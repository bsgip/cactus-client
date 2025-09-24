from datetime import datetime, timedelta, timezone

from assertical.fake.generator import generate_class_instance

from cactus_client.model.execution import StepExecution, StepExecutionList


def test_StepExecutionList():
    se_list = StepExecutionList()
    now = datetime(2025, 1, 2, tzinfo=timezone.utc)

    # Empty list should still work OK
    assert se_list.pop(now) is None
    assert se_list.peek(now) is None
    assert se_list.time_until_next(now) is None
    assert len(se_list) == 0

    se1 = generate_class_instance(StepExecution, seed=101, primacy=10, not_before=now + timedelta(seconds=10))
    se_list.add(se1)
    assert se_list.peek(now) is None, "Not before is in the future"
    assert se_list.pop(now) is None, "Not before is in the future"
    assert se_list.time_until_next(now) == timedelta(seconds=10)
    assert se_list.time_until_next(now + timedelta(seconds=20)) == timedelta(seconds=0)
    assert len(se_list) == 1

    se2 = generate_class_instance(StepExecution, seed=202, primacy=12, not_before=now + timedelta(seconds=5))
    se_list.add(se2)
    assert se_list.time_until_next(now) == timedelta(seconds=5)
    assert se_list.peek(now) is None, "Not before is in the future"
    assert se_list.pop(now) is None, "Not before is in the future"
    assert len(se_list) == 2
    assert se_list.peek(now + timedelta(seconds=5)) == se2
    assert se_list.pop(now + timedelta(seconds=5)) == se2
    assert len(se_list) == 1
    se_list.add(se2)  # re-add se2

    se3 = generate_class_instance(StepExecution, seed=303, primacy=15, not_before=None)
    se4 = generate_class_instance(StepExecution, seed=404, primacy=11, not_before=None)
    se5 = generate_class_instance(StepExecution, seed=505, primacy=12, not_before=None)
    se6 = generate_class_instance(StepExecution, seed=505, primacy=11, not_before=None)
    se_list.add(se3)
    se_list.add(se4)
    se_list.add(se5)
    se_list.add(se6)
    assert se_list.time_until_next(now) == timedelta(seconds=0)
    assert len(se_list) == 6
    assert se_list.peek(now) == se4
    assert se_list.pop(now) == se4
    assert se_list.peek(now) == se6
    assert se_list.pop(now) == se6
    assert se_list.peek(now) == se5
    assert se_list.pop(now) == se5

    assert se_list.peek(now + timedelta(seconds=20)) == se1
    assert se_list.pop(now + timedelta(seconds=20)) == se1
    assert se_list.peek(now + timedelta(seconds=20)) == se2
    assert se_list.pop(now + timedelta(seconds=20)) == se2
    assert se_list.time_until_next(now) == timedelta(0)
    assert se_list.peek(now + timedelta(seconds=20)) == se3
    assert se_list.pop(now + timedelta(seconds=20)) == se3
    assert se_list.time_until_next(now) is None
    assert len(se_list) == 0
