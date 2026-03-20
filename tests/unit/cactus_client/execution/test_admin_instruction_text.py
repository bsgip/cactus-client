import pytest
from cactus_test_definitions.server.admin_instructions import AdminInstruction

from cactus_client.execution.admin_instruction_text import describe_admin_instructions


def instr(type_: str, client: str | None = None, **params) -> AdminInstruction:
    return AdminInstruction(type=type_, client=client, parameters=params)


@pytest.mark.parametrize(
    "instructions, expected",
    [
        # ensure-end-device
        ([instr("ensure-end-device", registered=True)], "Register EndDevice"),
        ([instr("ensure-end-device", registered=True, has_der_list=True)], "Register EndDevice, with DER list"),
        ([instr("ensure-end-device", registered=False)], "Remove EndDevice registration"),
        ([instr("ensure-end-device", registered=True, client="dev1")], "Register EndDevice for dev1"),
        # ensure-mup-list-empty
        ([instr("ensure-mup-list-empty")], "Clear all MirrorUsagePoints"),
        # ensure-fsa
        ([instr("ensure-fsa")], "Ensure FunctionSetAssignment"),
        ([instr("ensure-fsa", annotation="fsa1")], "Ensure FunctionSetAssignment'fsa1'"),
        ([instr("ensure-fsa", annotation="fsa1", primacy=1)], "Ensure FunctionSetAssignment'fsa1' primacy=1"),
        ([instr("ensure-fsa", primacy=0)], "Ensure FunctionSetAssignment primacy=0"),
        # ensure-der-program
        ([instr("ensure-der-program")], "Ensure DERProgram"),
        ([instr("ensure-der-program", fsa_annotation="fsa1")], "Ensure DERProgram'fsa1'"),
        ([instr("ensure-der-program", fsa_annotation="fsa1", primacy=2)], "Ensure DERProgram'fsa1' primacy=2"),
        # set-client-access
        ([instr("set-client-access", granted=True)], "Grant client access"),
        ([instr("set-client-access", granted=False)], "Revoke client access"),
        ([instr("set-client-access", granted=True, client="dev2")], "Grant client access for dev2"),
        # ensure-der-control-list
        ([instr("ensure-der-control-list")], "Ensure DERControlList accessible"),
        ([instr("ensure-der-control-list", subscribable=True)], "Ensure DERControlList accessible, subscribable"),
        # create-der-control
        (
            [instr("create-der-control", status="active", opModExpLimW=1000.0)],
            "Create active DERControl opModExpLimW=1000.0",
        ),
        (
            [instr("create-der-control", status="scheduled", opModExpLimW=500.0)],
            "Create scheduled DERControl opModExpLimW=500.0",
        ),
        # create-default-der-control
        ([instr("create-default-der-control", opModExpLimW=200.0)], "Create DefaultDERControl opModExpLimW=200.0"),
        # clear-der-controls
        ([instr("clear-der-controls", all=True)], "Cancel all active DERControls"),
        ([instr("clear-der-controls", all=False)], "Cancel latest DERControl"),
        ([instr("clear-der-controls")], "Cancel latest DERControl"),
        # set-poll-rate
        (
            [instr("set-poll-rate", resource="DERProgramList", rate_seconds=60)],
            "Set poll rate for DERProgramList to 60s",
        ),
        # set-post-rate
        (
            [instr("set-post-rate", resource="MirrorUsagePoint", rate_seconds=300)],
            "Set post rate for MirrorUsagePoint to 300s",
        ),
        # unknown fallback
        ([instr("some-unknown-type")], "some-unknown-type"),
        # multiple instructions joined
        (
            [instr("ensure-mup-list-empty"), instr("ensure-end-device", registered=True)],
            "Clear all MirrorUsagePoints. Register EndDevice",
        ),
    ],
)
def test_describe_admin_instructions(instructions: list[AdminInstruction], expected: str) -> None:
    assert describe_admin_instructions(instructions) == expected
