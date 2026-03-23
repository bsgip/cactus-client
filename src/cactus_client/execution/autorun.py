import logging
from dataclasses import dataclass
from enum import auto, Enum
from pathlib import Path

from cactus_test_definitions.csipaus import CSIPAusVersion
from cactus_test_definitions.server.test_procedures import (
    ClientType,
    RequiredClient,
    TestProcedure,
    TestProcedureId,
    get_test_procedure,
)

from cactus_client.error import ConfigException
from cactus_client.execution.run import run_entrypoint
from cactus_client.model.config import ClientConfig, GlobalConfig, RunConfig

logger = logging.getLogger(__name__)


class AutorunStatus(Enum):
    PASSED = auto()
    FAILED = auto()
    SKIPPED = auto()  # Not enough matching clients — test not attempted
    ERROR = auto()  # Unexpected exception during setup or execution


@dataclass
class AutorunRecord:
    test_id: TestProcedureId
    status: AutorunStatus
    note: str | None = None  # Reason for SKIPPED or ERROR


def _load_id_file(path: str) -> list[str]:
    """Read test IDs from a text file — one per line, lines starting with # are ignored."""
    lines = Path(path).read_text().splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


def resolve_test_list(
    include: list[str] | None,
    include_file: str | None,
    exclude: list[str] | None,
) -> list[TestProcedureId]:
    """Return the ordered list of TestProcedureIds to run after applying include/exclude filters.

    Include sources are merged and deduplicated (preserving order). If no include source is
    provided, all known test IDs are used in enum definition order. Exclude is always applied
    last. Raises ConfigException for any unrecognised test ID."""

    # Build the raw include list
    include_ids: list[str] = []
    if include:
        include_ids.extend(include)
    if include_file:
        try:
            include_ids.extend(_load_id_file(include_file))
        except OSError as exc:
            raise ConfigException(f"Could not read include file '{include_file}': {exc}")

    # Validate all supplied IDs up-front
    all_supplied = include_ids + (exclude or [])
    unknown = [id_str for id_str in all_supplied if id_str not in TestProcedureId]
    if unknown:
        raise ConfigException(f"Unrecognised test procedure ID(s): {', '.join(unknown)}")

    if include_ids:
        # Deduplicate while preserving order
        seen: set[str] = set()
        result: list[TestProcedureId] = []
        for id_str in include_ids:
            if id_str not in seen:
                seen.add(id_str)
                result.append(TestProcedureId(id_str))
    else:
        result = list(TestProcedureId)

    exclude_set: set[str] = set(exclude) if exclude else set()
    return [tp_id for tp_id in result if tp_id not in exclude_set]


def _assign_clients(
    required_clients: list[RequiredClient],
    configured_clients: list[ClientConfig],
) -> list[str] | None:
    """Assign configured clients to required client slots.

    Iterates required_clients in order. For each slot, picks the first unused configured
    client whose type satisfies the requirement (None = any type). Returns the ordered list
    of client IDs, or None if any slot cannot be filled."""
    used: set[str] = set()
    result: list[str] = []

    for req in required_clients:
        matched = next(
            (
                c
                for c in configured_clients
                if c.id not in used and (req.client_type is None or c.type == req.client_type)
            ),
            None,
        )
        if matched is None:
            return None
        used.add(matched.id)
        result.append(matched.id)

    return result


def _skip_reason(required: list[RequiredClient], configured: list[ClientConfig]) -> str:
    need_devices = sum(1 for r in required if r.client_type != ClientType.AGGREGATOR)
    need_aggs = sum(1 for r in required if r.client_type == ClientType.AGGREGATOR)
    have_devices = sum(1 for c in configured if c.type == ClientType.DEVICE)
    have_aggs = sum(1 for c in configured if c.type == ClientType.AGGREGATOR)
    return (
        f"requires {need_devices} device(s) and {need_aggs} aggregator(s), "
        f"but only {have_devices} device(s) and {have_aggs} aggregator(s) are configured"
    )


async def autorun_entrypoint(
    global_config: GlobalConfig,
    include: list[str] | None,
    include_file: str | None,
    exclude: list[str] | None,
    headless: bool,
    timeout: int | None,
    strict: bool = False,
) -> list[AutorunRecord]:
    """Run selected test procedures sequentially with automatic client assignment.

    Stops on the first FAILED or ERROR result. SKIPPED tests (insufficient clients) are
    logged and execution continues. Returns a list of AutorunRecord for each attempted test."""

    if not global_config.clients:
        raise ConfigException("No clients are configured.")

    test_ids = resolve_test_list(include, include_file, exclude)
    records: list[AutorunRecord] = []

    for test_id in test_ids:
        tp: TestProcedure = get_test_procedure(test_id)
        client_ids = _assign_clients(tp.preconditions.required_clients, global_config.clients)

        if client_ids is None:
            reason = _skip_reason(tp.preconditions.required_clients, global_config.clients)
            logger.warning("Skipping %s: %s", test_id, reason)
            records.append(AutorunRecord(test_id=test_id, status=AutorunStatus.SKIPPED, note=reason))
            continue

        run_config = RunConfig(
            test_procedure_id=test_id,
            client_ids=client_ids,
            csip_aus_version=CSIPAusVersion.RELEASE_1_2,
            headless=headless,
            timeout=timeout,
            strict=strict,
        )

        try:
            passed = await run_entrypoint(global_config, run_config)
        except ConfigException as exc:
            logger.error("Config error running %s: %s", test_id, exc)
            records.append(AutorunRecord(test_id=test_id, status=AutorunStatus.ERROR, note=str(exc)))
            continue
        except Exception as exc:
            logger.error("Unexpected error running %s", test_id, exc_info=exc)
            records.append(AutorunRecord(test_id=test_id, status=AutorunStatus.ERROR, note=str(exc)))
            continue

        if passed:
            records.append(AutorunRecord(test_id=test_id, status=AutorunStatus.PASSED))
        else:
            records.append(AutorunRecord(test_id=test_id, status=AutorunStatus.FAILED))

    return records
