import hashlib
from dataclasses import dataclass
from typing import Any, cast

from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)
from envoy_schema.server.schema.sep2.metering_mirror import (
    MirrorUsagePoint,
)
from envoy_schema.server.schema.sep2.types import (
    DataQualifierType,
    KindType,
    RoleFlagsType,
    UomType,
)

from cactus_client.error import CactusClientException
from cactus_client.model.config import ClientConfig
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import CheckResult, StepExecution
from cactus_client.model.resource import ResourceStore, StoredResource
from cactus_client.sep2 import hex_binary_equal


@dataclass(frozen=True)
class MirrorUsagePointMrids:

    mup_mrid: str
    mmr_mrids: dict[CSIPAusReadingType, str]


def generate_hashed_mrid(seed: str, pen: int) -> str:
    """Generates a 32 character mrid with the last 8 characters being the pen (0 padded)"""
    hash = hashlib.md5(seed.encode(), usedforsecurity=False)
    return f"{hash.hexdigest()[:24]}{pen:08}".upper()


def generate_mmr_mrid(mup_mrid: str, rt: CSIPAusReadingType, pen: int) -> str:
    """Generates an mrid for a MirrorMeterReading that lives under a MirrorUsagePoint with mup_mrid"""
    return generate_hashed_mrid(mup_mrid + str(rt), pen)


def generate_mup_mrids(
    location: CSIPAusReadingLocation,
    reading_types: list[CSIPAusReadingType],
    mmr_mrids: list[str] | None,
    client: ClientConfig,
) -> MirrorUsagePointMrids:
    """A deterministic set of calculations that always yields the same MRIDs for the same inputs but also varies
    all values for any variance (basically hash derived)"""
    mup_mrid = generate_hashed_mrid(str(location) + client.id + "|".join(sorted(reading_types)), client.pen)

    # If we have defined mrids for the MirrorMeterReadings - just apply them (with the client pen encoded)
    if mmr_mrids:
        if len(mmr_mrids) != len(reading_types):
            raise CactusClientException(
                "Test definition error. Parameter mmr_mrids has a different length to reading_types"
            )
        return MirrorUsagePointMrids(
            mup_mrid=mup_mrid,
            mmr_mrids=dict(((rt, f"{raw_mrid[:24]}{client.pen:08}") for rt, raw_mrid in zip(reading_types, mmr_mrids))),
        )

    # Otherwise continue to derive more hashed mrids
    return MirrorUsagePointMrids(
        mup_mrid=mup_mrid,
        mmr_mrids=dict(((rt, generate_mmr_mrid(mup_mrid, rt, client.pen)) for rt in reading_types)),
    )


def generate_reading_type_values(rt: CSIPAusReadingType) -> tuple[UomType, KindType, DataQualifierType]:
    """Generates a CSIP-Aus compliant set of reading type values based on the associated test definition enum"""
    match rt:
        case CSIPAusReadingType.ActivePowerAverage:
            return (UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.AVERAGE)
        case CSIPAusReadingType.ActivePowerInstantaneous:
            return (UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.STANDARD)
        case CSIPAusReadingType.ActivePowerMaximum:
            return (UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.MAXIMUM)
        case CSIPAusReadingType.ActivePowerMinimum:
            return (UomType.REAL_POWER_WATT, KindType.POWER, DataQualifierType.MINIMUM)

        case CSIPAusReadingType.ReactivePowerAverage:
            return (UomType.REACTIVE_POWER_VAR, KindType.POWER, DataQualifierType.AVERAGE)
        case CSIPAusReadingType.ReactivePowerInstantaneous:
            return (UomType.REACTIVE_POWER_VAR, KindType.POWER, DataQualifierType.STANDARD)
        case CSIPAusReadingType.ReactivePowerMaximum:
            return (UomType.REACTIVE_POWER_VAR, KindType.POWER, DataQualifierType.MAXIMUM)
        case CSIPAusReadingType.ReactivePowerMinimum:
            return (UomType.REACTIVE_POWER_VAR, KindType.POWER, DataQualifierType.MINIMUM)

        case CSIPAusReadingType.FrequencyAverage:
            return (UomType.FREQUENCY_HZ, KindType.POWER, DataQualifierType.AVERAGE)
        case CSIPAusReadingType.FrequencyInstantaneous:
            return (UomType.FREQUENCY_HZ, KindType.POWER, DataQualifierType.STANDARD)
        case CSIPAusReadingType.FrequencyMaximum:
            return (UomType.FREQUENCY_HZ, KindType.POWER, DataQualifierType.MAXIMUM)
        case CSIPAusReadingType.FrequencyMinimum:
            return (UomType.FREQUENCY_HZ, KindType.POWER, DataQualifierType.MINIMUM)

        case CSIPAusReadingType.VoltageSinglePhaseAverage:
            return (UomType.VOLTAGE, KindType.POWER, DataQualifierType.AVERAGE)
        case CSIPAusReadingType.VoltageSinglePhaseInstantaneous:
            return (UomType.VOLTAGE, KindType.POWER, DataQualifierType.STANDARD)
        case CSIPAusReadingType.VoltageSinglePhaseMaximum:
            return (UomType.VOLTAGE, KindType.POWER, DataQualifierType.MAXIMUM)
        case CSIPAusReadingType.VoltageSinglePhaseMinimum:
            return (UomType.VOLTAGE, KindType.POWER, DataQualifierType.MINIMUM)

        case _:
            raise CactusClientException(f"No ReadingType mapping configured for {rt}. This is a test definition error.")


def generate_role_flags(location: CSIPAusReadingLocation) -> RoleFlagsType:
    match location:
        case CSIPAusReadingLocation.Device:
            return RoleFlagsType.IS_MIRROR | RoleFlagsType.IS_DER | RoleFlagsType.IS_SUBMETER
        case CSIPAusReadingLocation.Site:
            return RoleFlagsType.IS_MIRROR | RoleFlagsType.IS_PREMISES_AGGREGATION_POINT

        case _:
            raise CactusClientException(
                f"No CSIPAusReadingLocation mapping configured for {location}. This is a test definition error."
            )


def find_mrids_matching(
    store: ResourceStore,
    role_flags: RoleFlagsType | None,
    mrids: MirrorUsagePointMrids | None,
    reading_type_vals: list[tuple[UomType, KindType, DataQualifierType]] | None,
    post_rate_seconds: int | None,
) -> list[StoredResource]:
    """Finds all MirrorUsagePoints in the resource store that match the specified criteria (None means no assertion)"""
    all_matches: list[StoredResource] = []
    for mup in store.get(CSIPAusResource.MirrorUsagePoint):
        resource = cast(MirrorUsagePoint, mup.resource)

        # Look to disqualify mups as matches by checking things one by one
        if post_rate_seconds is not None and post_rate_seconds != resource.postRate:
            continue

        if role_flags is not None and not hex_binary_equal(resource.roleFlags, role_flags):
            continue

        if mrids is not None:
            # Check top level mrid
            if mrids.mup_mrid != resource.mRID:
                continue

            # Check mmr's across all MirrorMeterReadings (they should match perfectly)
            mmr_mrids = [mmr.mRID for mmr in resource.mirrorMeterReadings] if resource.mirrorMeterReadings else []
            if set(mmr_mrids) != set(mrids.mmr_mrids.values()):
                continue

        # Check the readingType values for each MirrorMeterReading - we want an exact match
        if reading_type_vals is not None:
            comparison_reading_type_value: list[tuple[UomType, KindType, DataQualifierType]] = []
            for mmr in resource.mirrorMeterReadings or []:
                if mmr.readingType is None:
                    raise CactusClientException(f"MirrorMeterReading {mmr.href} {mmr.mRID} has no readingType")
                comparison_reading_type_value.append(
                    (
                        mmr.readingType.uom or UomType.NOT_APPLICABLE,
                        mmr.readingType.kind or KindType.NOT_APPLICABLE,
                        mmr.readingType.dataQualifier or DataQualifierType.NOT_APPLICABLE,
                    )
                )
            if set(comparison_reading_type_value) != set(reading_type_vals):
                continue
        all_matches.append(mup)
    return all_matches


def check_mirror_usage_point(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> CheckResult:
    """Checks whether the specified EndDevice's in the resource store match the check criteria"""

    matches: bool = resolved_parameters["matches"]  # mandatory param
    location: CSIPAusReadingLocation | None = resolved_parameters.get("location", None)
    reading_types: list[CSIPAusReadingType] | None = resolved_parameters.get("reading_types", None)
    mmr_mrids: list[str] | None = resolved_parameters.get("mmr_mrids", None)
    post_rate_seconds: int | None = resolved_parameters.get("post_rate_seconds", None)

    resource_store = context.discovered_resources(step)
    client_config = context.client_config(step)

    # Figure out our match criteria
    target_role_flags = generate_role_flags(location) if location is not None else None
    target_mrids = (
        generate_mup_mrids(location=location, reading_types=reading_types, mmr_mrids=mmr_mrids, client=client_config)
        if location is not None and reading_types is not None
        else None
    )
    target_reading_type_values = (
        [generate_reading_type_values(rt) for rt in reading_types] if reading_types is not None else None
    )

    # Do the matching - check it meets our expectations
    all_matches = find_mrids_matching(
        resource_store, target_role_flags, target_mrids, target_reading_type_values, post_rate_seconds
    )

    criteria_descriptions: list[str] = []
    if location is not None:
        criteria_descriptions.append(str(location))
    if reading_types is not None:
        criteria_descriptions.append(f"Readings [{', '.join(reading_types)}]")
    if mmr_mrids is not None:
        criteria_descriptions.append("Has specific mrids")
    if post_rate_seconds is not None:
        criteria_descriptions.append(f"postRate {post_rate_seconds}")

    if not criteria_descriptions:
        criteria_descriptions = ["No matching criteria"]

    if matches and len(all_matches) == 0:

        return CheckResult(
            False, f"Couldn't find a MirrorUsagePoint that matches criteria. {', '.join(criteria_descriptions)}"
        )
    elif not matches and len(all_matches) > 0:

        return CheckResult(
            False,
            f"Found {len(all_matches)} MirrorUsagePoint(s) but expected 0. Criteria: {', '.join(criteria_descriptions)}",
        )

    return CheckResult(True, None)
