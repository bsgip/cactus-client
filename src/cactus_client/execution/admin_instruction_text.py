from cactus_test_definitions.server.admin_instructions import AdminInstruction


def describe_admin_instructions(instructions: list[AdminInstruction]) -> str:  # noqa: C901
    """Return a concise human-readable summary of the given admin instructions."""
    parts = []
    for instr in instructions:
        p = instr.parameters
        client_suffix = f" for {instr.client}" if instr.client else ""

        match instr.type:
            case "ensure-end-device":
                if p.get("registered", True):
                    detail = "Register EndDevice"
                    if p.get("has_der_list"):
                        detail += ", with DER list"
                else:
                    detail = "Remove EndDevice registration"
                parts.append(detail + client_suffix)
            case "ensure-mup-list-empty":
                parts.append("Clear all MirrorUsagePoints")
            case "ensure-fsa":
                detail = "Ensure FunctionSetAssignment"
                if p.get("annotation"):
                    detail += f"'{p['annotation']}'"
                if p.get("primacy") is not None:
                    detail += f" primacy={p['primacy']}"
                parts.append(detail + client_suffix)
            case "ensure-der-program":
                detail = "Ensure DERProgram"
                if p.get("fsa_annotation"):
                    detail += f"'{p['fsa_annotation']}'"
                if p.get("primacy") is not None:
                    detail += f" primacy={p['primacy']}"
                parts.append(detail + client_suffix)
            case "set-client-access":
                detail = "Grant client access" if p.get("granted", True) else "Revoke client access"
                parts.append(detail + client_suffix)
            case "ensure-der-control-list":
                detail = "Ensure DERControlList accessible"
                if p.get("subscribable"):
                    detail += ", subscribable"
                parts.append(detail + client_suffix)
            case "create-der-control":
                detail = f"Create {p['status']} DERControl"
                detail += "".join(f" {k}={v}" for k, v in p.items() if k != "status")
                parts.append(detail + client_suffix)
            case "create-default-der-control":
                parts.append("Create DefaultDERControl" + "".join(f" {k}={v}" for k, v in p.items()) + client_suffix)
            case "clear-der-controls":
                parts.append("Cancel all active DERControls" if p.get("all") else "Cancel latest DERControl")
            case "set-poll-rate":
                parts.append(f"Set poll rate for {p['resource']} to {p['rate_seconds']}s")
            case "set-post-rate":
                parts.append(f"Set post rate for {p['resource']} to {p['rate_seconds']}s")
            case _:
                parts.append(instr.type)

    return ". ".join(parts)
