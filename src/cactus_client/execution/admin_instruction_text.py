from cactus_test_definitions.server.admin_instructions import AdminInstruction, AdminInstructionType
from cactus_test_definitions.variable_expressions import BaseExpression


def _fmt(v: object) -> str:
    """Format a parameter value, using expression_representation() for variable expressions."""
    if isinstance(v, BaseExpression):
        return v.expression_representation()
    return str(v)


def describe_admin_instructions(instructions: list[AdminInstruction]) -> str:  # noqa: C901
    """Return a concise human-readable summary of the given admin instructions."""
    parts = []
    for instr in instructions:
        p = instr.parameters
        client_suffix = f" for {instr.client}" if instr.client else ""

        match instr.type:
            case AdminInstructionType.ENSURE_END_DEVICE:
                if p.get("registered", True):
                    detail = "Register EndDevice"
                    if p.get("has_der_list"):
                        detail += ", with DER list"
                else:
                    detail = "Remove EndDevice registration"
                parts.append(detail + client_suffix)
            case AdminInstructionType.ENSURE_MUP_LIST_EMPTY:
                parts.append("Clear all MirrorUsagePoints")
            case AdminInstructionType.ENSURE_FSA:
                detail = "Ensure FunctionSetAssignment"
                if p.get("annotation"):
                    detail += f"'{p['annotation']}'"
                if p.get("primacy") is not None:
                    detail += f" primacy={p['primacy']}"
                parts.append(detail + client_suffix)
            case AdminInstructionType.ENSURE_DER_PROGRAM:
                detail = "Ensure DERProgram"
                if p.get("fsa_annotation"):
                    detail += f"'{p['fsa_annotation']}'"
                if p.get("primacy") is not None:
                    detail += f" primacy={p['primacy']}"
                parts.append(detail + client_suffix)
            case AdminInstructionType.SET_CLIENT_ACCESS:
                detail = "Grant client access" if p.get("granted", True) else "Revoke client access"
                parts.append(detail + client_suffix)
            case AdminInstructionType.ENSURE_DER_CONTROL_LIST:
                detail = "Ensure DERControlList accessible"
                if p.get("subscribable"):
                    detail += ", subscribable"
                parts.append(detail + client_suffix)
            case AdminInstructionType.CREATE_DER_CONTROL:
                detail = f"Create {p['status']} DERControl"
                detail += "".join(f" {k}={_fmt(v)}" for k, v in p.items() if k != "status")
                parts.append(detail + client_suffix)
            case AdminInstructionType.CREATE_DEFAULT_DER_CONTROL:
                parts.append(
                    "Create DefaultDERControl" + "".join(f" {k}={_fmt(v)}" for k, v in p.items()) + client_suffix
                )
            case AdminInstructionType.CLEAR_DER_CONTROLS:
                parts.append("Cancel all active DERControls" if p.get("all") else "Cancel latest DERControl")
            case AdminInstructionType.SET_POLL_RATE:
                parts.append(f"Set poll rate for {p['resource']} to {p['rate_seconds']}s")
            case AdminInstructionType.SET_POST_RATE:
                parts.append(f"Set post rate for {p['resource']} to {p['rate_seconds']}s")

    return ". ".join(parts)
