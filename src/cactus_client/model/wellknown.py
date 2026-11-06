from dataclasses import dataclass

from dataclass_wizard import JSONWizard


@dataclass(frozen=True)
class WellKnownSchemaVersion(JSONWizard):
    """The entry points that a server offers for a single supported schema version"""

    dcap: list[str]  # URIs of the DeviceCapability resources available for this schema version


@dataclass(frozen=True)
class WellKnown(JSONWizard):
    """Represents a parsed well-known JSON document. Parsing is lenient (eg a string will be coerced to a list of
    characters), so documents should be checked with cactus_client.schema.validator.validate_well_known_json first."""

    supported_schema_versions: dict[str, WellKnownSchemaVersion]  # Keyed by schema version
