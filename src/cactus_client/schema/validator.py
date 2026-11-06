import json
import logging
from functools import lru_cache
from pathlib import Path

from cactus_test_definitions import CSIPAusVersion
from jsonschema.protocols import Validator
from jsonschema.validators import validator_for
from lxml import etree

import cactus_client.schema.csipaus12 as v12
import cactus_client.schema.csipaus13 as v13
from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION

logger = logging.getLogger(__name__)


class LocalXsdResolver(etree.Resolver):
    """Finds specific XSD files in our local schema directory"""

    sep_xsd: Path
    schema_dir: Path

    def __init__(self, sep_xsd: Path, schema_dir: Path) -> None:
        self.sep_xsd = sep_xsd
        self.schema_dir = schema_dir

    def resolve(self, url, id, context):  # noqa: ANN001,ANN201 # type: ignore # lxml stubs are faulty
        if url == "sep.xsd":
            return self.resolve_filename(str(self.sep_xsd), context)  # type: ignore
        elif url == "csipaus-core.xsd":
            return self.resolve_filename(  # type: ignore # lxml stubs are faulty
                str(self.schema_dir / "csipaus-core.xsd"), context
            )
        elif url == "csipaus-ext.xsd":
            return self.resolve_filename(  # type: ignore # lxml stubs are faulty
                str(self.schema_dir / "csipaus-ext.xsd"), context
            )
        return None


@lru_cache
def csip_aus_schema(version: CSIPAusVersion) -> etree.XMLSchema:
    """Generates a etree.XMLSchema that's loaded with the CSIP Aus XSD document (which incorporates sep2)"""

    sep_xsd = Path(__file__).parent / "sep.xsd"  # sep.xsd is unversioned and shared across all csipaus1x dirs
    match version:
        case CSIPAusVersion.RELEASE_1_2:
            schema_dir = Path(v12.__file__).parent
        case CSIPAusVersion.RELEASE_1_3:
            schema_dir = Path(v13.__file__).parent
        case _:
            raise ValueError(f"No XSD for {version}")

    # Register the custom resolver
    parser = etree.XMLParser(load_dtd=True, no_network=True)
    parser.resolvers.add(LocalXsdResolver(sep_xsd=sep_xsd, schema_dir=schema_dir))

    # Load schema
    with open(schema_dir / "csipaus-core.xsd") as fp:
        xsd_content = fp.read()
    schema_root = etree.XML(xsd_content, parser)
    return etree.XMLSchema(schema_root)


def validate_xml(xml: str, version: CSIPAusVersion = ACTIVE_CSIP_AUS_VERSION) -> list[str]:
    """Validates an xml document / snippet as a valid CSIP Aus XML snippet. Returns a list of any human
    readable schema validation errors. Empty list means that xml is schema valid"""

    try:
        # bytes, as lxml only accepts an <?xml encoding=...?> declaration on bytes input
        xml_doc = etree.fromstring(xml.encode())
    except Exception as exc:
        preview = xml[:32]
        logger.error(
            f"validate_xml: Failure parsing string starting '{preview}'... as XML",
            exc_info=exc,
        )
        return [f"The provided body '{preview}'... does NOT parse as XML"]

    schema = csip_aus_schema(version)

    # Validate
    is_valid = schema.validate(xml_doc)
    if is_valid:
        return []
    else:
        return [f"{e.line}: {e.message}" for e in schema.error_log]


@lru_cache
def _wellknown_validator() -> Validator:
    wellknown_json_xsd = Path(__file__).parent / "wellknown.schema.json"
    with open(wellknown_json_xsd, "rb") as fp:
        schema = json.load(fp)

    validator_cls = validator_for(schema)
    validator_cls.check_schema(schema)  # fail loudly if the schema itself is broken
    return validator_cls(schema)


def validate_well_known_json(json_body: str | bytes) -> list[str]:
    """Validate a well-known JSON document.

    Returns a list of human-readable error strings; an empty list means valid.
    """
    try:
        instance = json.loads(json_body)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return [f"Invalid JSON: {e}"]

    validator = _wellknown_validator()
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    return [f"{e.json_path}: {e.message}" for e in errors]


def to_hex_binary(v: int) -> str:
    """Convert integer to hexBinary string with minimal pairs (even length)"""

    hex_str = f"{v:X}"  # Uppercase hex without padding
    # Ensure even length by padding with single leading zero if needed
    if len(hex_str) % 2 == 1:
        hex_str = "0" + hex_str

    return hex_str
