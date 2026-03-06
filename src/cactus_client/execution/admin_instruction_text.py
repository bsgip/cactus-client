from cactus_test_definitions.server.admin_instructions import AdminInstruction


def describe_admin_instructions(instructions: list[AdminInstruction]) -> str:
    """Return a concise human-readable summary of the given admin instructions."""
    parts = []
    for instr in instructions:
        p = instr.parameters
        client_suffix = f" for {instr.client}" if instr.client else ""

        if instr.type == "ensure-end-device":
            if p.get("registered", True):
                detail = "Register EndDevice"
                if p.get("has_der_list"):
                    detail += ", with DER list"
            else:
                detail = "Remove EndDevice registration"
            parts.append(detail + client_suffix)

        elif instr.type == "ensure-mup-list-empty":
            parts.append("Clear all MirrorUsagePoints")

        elif instr.type == "ensure-fsa":
            detail = "Ensure FunctionSetAssignment"
            if p.get("annotation"):
                detail += f"'{p['annotation']}'"
            if p.get("primacy") is not None:
                detail += f" primacy={p['primacy']}"
            parts.append(detail + client_suffix)

        elif instr.type == "ensure-der-program":
            detail = "Ensure DERProgram"
            if p.get("fsa_annotation"):
                detail += f"'{p['fsa_annotation']}'"
            if p.get("primacy") is not None:
                detail += f" primacy={p['primacy']}"
            parts.append(detail + client_suffix)

        elif instr.type == "set-client-access":
            detail = "Grant client access" if p.get("granted", True) else "Revoke client access"
            parts.append(detail + client_suffix)

        elif instr.type == "ensure-der-control-list":
            detail = "Ensure DERControlList accessible"
            if p.get("subscribable"):
                detail += ", subscribable"
            parts.append(detail + client_suffix)

        elif instr.type == "create-der-control":
            detail = f"Create {p['status']} DERControl"
            detail += "".join(f" {k}={v}" for k, v in p.items() if k != "status")
            parts.append(detail + client_suffix)

        elif instr.type == "create-default-der-control":
            parts.append("Create DefaultDERControl" + "".join(f" {k}={v}" for k, v in p.items()) + client_suffix)

        elif instr.type == "clear-der-controls":
            parts.append("Cancel all active DERControls" if p.get("all") else "Cancel latest DERControl")

        elif instr.type == "set-poll-rate":
            parts.append(f"Set poll rate for {p['resource']} to {p['rate_seconds']}s")

        elif instr.type == "set-post-rate":
            parts.append(f"Set post rate for {p['resource']} to {p['rate_seconds']}s")

        else:
            parts.append(instr.type)

    return ". ".join(parts)
