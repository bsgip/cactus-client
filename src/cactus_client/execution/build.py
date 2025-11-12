import ssl
import urllib
import urllib.parse
from pathlib import Path
from ssl import SSLContext

from aiohttp import ClientSession, TCPConnector
from cactus_test_definitions.server.test_procedures import (
    TestProcedure,
    TestProcedureConfig,
)

from cactus_client.error import ConfigException
from cactus_client.model.config import (
    ClientConfig,
    GlobalConfig,
    RunConfig,
    ServerConfig,
)
from cactus_client.model.context import ClientContext, ExecutionContext
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import CSIPAusResourceTree, ResourceStore


def build_clients_by_alias(
    resource_tree: CSIPAusResourceTree,
    base_uri: str,
    configured_clients: list[ClientConfig] | None,
    verify_ssl: bool,
    serca_pem_path: str | None,
    run_client_ids: list[str],
    tp: TestProcedure,
) -> dict[str, ClientContext]:
    if not configured_clients:
        raise ConfigException("No clients have been created (client config is empty).")

    client_config_by_id = dict(((cfg.id, cfg) for cfg in configured_clients))

    if len(run_client_ids) != len(tp.preconditions.required_clients):
        raise ConfigException(
            f"This test expects {len(tp.preconditions.required_clients)} client(s)."
            + f" You have supplied {len(run_client_ids)} client id(s)"
        )

    clients_by_alias: dict[str, ClientContext] = {}
    for tp_client_precondition, client_config_id in zip(tp.preconditions.required_clients, run_client_ids):
        client_config = client_config_by_id.get(client_config_id, None)
        if client_config is None:
            raise ConfigException(f"The supplied client id '{client_config_id}' doesn't exist in your configuration.")

        if tp_client_precondition.client_type is not None and client_config.type != tp_client_precondition.client_type:
            raise ConfigException(
                f"The supplied client id '{client_config_id}' is the wrong type of client for this test."
                + f" Test expects a {tp_client_precondition.client_type} client but got a {client_config.type} client."
            )

        # Load the client certs into a SSLContext
        ssl_context = SSLContext(ssl.PROTOCOL_TLSv1_2)  # TLS 1.2 required by 2030.5
        ssl_context.check_hostname = verify_ssl
        ssl_context.verify_mode = ssl.CERT_REQUIRED if verify_ssl else ssl.CERT_NONE
        if verify_ssl and serca_pem_path:
            try:
                ssl_context.load_verify_locations(cafile=serca_pem_path)
            except Exception:
                raise ConfigException(
                    f"Failure loading SERCA certificate for {client_config_id} from SERCA PEM file '{serca_pem_path}'"
                )

        try:
            ssl_context.load_cert_chain(client_config.certificate_file, client_config.key_file)
        except Exception:
            raise ConfigException(
                f"Failure loading client certificate chain for {client_config_id} from"
                + f"cert file {client_config.certificate_file} and key file {client_config.key_file}."
            )

        clients_by_alias[tp_client_precondition.id] = ClientContext(
            test_procedure_alias=tp_client_precondition.id,
            client_config=client_config,
            discovered_resources=ResourceStore(resource_tree),
            session=ClientSession(base_url=base_uri, connector=TCPConnector(ssl=ssl_context)),
        )

    return clients_by_alias


def build_dcap_parts(server: ServerConfig) -> tuple[str, str]:
    """Extracts the (base_uri, dcap_path) from the server device_capability_uri"""
    dcap_host: str | None = None
    dcap_path: str | None = None
    dcap_scheme: str | None = None
    try:
        url = urllib.parse.urlparse(server.device_capability_uri)
    except Exception:
        raise ConfigException(f"device_capability_uri '{server.device_capability_uri}' couldn't be parsed.")
    dcap_host = url.netloc
    dcap_path = url.path
    dcap_scheme = url.scheme
    if not dcap_path:
        dcap_path = "/"
    if dcap_scheme not in {"https", "http"}:
        raise ConfigException(f"Unsupported scheme {dcap_scheme} for '{server.device_capability_uri}'.")
    return (f"{dcap_scheme}://{dcap_host}/", dcap_path)


def build_initial_step_execution_list(tp: TestProcedure) -> StepExecutionList:
    """Creates a step execution list from a test procedure definition"""
    result = StepExecutionList()
    client_aliases: list[str] = [c.id for c in tp.preconditions.required_clients]
    if not client_aliases:
        raise ConfigException("Expected at least one client in the test definition. This is a test definition bug.")

    for idx, step in enumerate(tp.steps):
        client_alias: str | None = step.client
        if not client_alias:
            client_alias = client_aliases[0]  # By convention - an unspecified client_alias means the first client

        client_resource_alias = client_alias
        if step.use_client_context:
            client_resource_alias = step.use_client_context

        result.add(
            StepExecution(
                source=step,
                client_alias=client_alias,
                client_resources_alias=client_resource_alias,
                primacy=idx,  # Use index as the primacy so that the steps execute in order
                repeat_number=0,
                not_before=None,
                attempts=0,
            )
        )
    return result


async def build_execution_context(user_config: GlobalConfig, run_config: RunConfig) -> ExecutionContext:
    """Takes all the information from the user's configuration AND the supplied config for this run and generates
    an ExecutionContext that's ready to start a run.

    Raises a ConfigException if there are any problems."""

    tp_id = run_config.test_procedure_id

    all_test_procedures = TestProcedureConfig.from_resource()
    tp_version = all_test_procedures.version
    tp = all_test_procedures.test_procedures.get(tp_id, None)
    if tp is None:
        raise ConfigException(f"Test Procedure ID '{tp_id}' isn't recognised for version {tp_version}.")

    if run_config.csip_aus_version not in tp.target_versions:
        raise ConfigException(f"The requested version {run_config.csip_aus_version} is not supported by {tp_id}")

    if not user_config.output_dir:
        raise ConfigException("output_dir has not been specified.")
    try:
        output_dir = Path(user_config.output_dir)
    except Exception:
        raise ConfigException(f"output_dir value '{user_config.output_dir}' doesn't appear to be valid.")
    if not output_dir.exists() or not output_dir.is_dir():
        raise ConfigException(f"output_dir '{user_config.output_dir}' should exist and be a directory.")

    # Pull info from the server config
    if not user_config.server:
        raise ConfigException("Missing server configuration element.")
    base_uri, dcap_path = build_dcap_parts(user_config.server)

    # Parse the supplied clients and map them to the real underlying config
    resource_tree = CSIPAusResourceTree()
    clients_by_alias = build_clients_by_alias(
        resource_tree,
        base_uri,
        user_config.clients,
        user_config.server.verify_ssl,
        user_config.server.serca_pem_file,
        run_config.client_ids,
        tp,
    )

    return ExecutionContext(
        test_procedure_id=tp_id,
        test_procedure=tp,
        test_procedures_version=tp_version,
        output_directory=output_dir,
        dcap_path=dcap_path,
        server_config=user_config.server,
        clients_by_alias=clients_by_alias,
        steps=build_initial_step_execution_list(tp),
        progress=ProgressTracker(),
        resource_tree=resource_tree,
        responses=ResponseTracker(),
        warnings=WarningTracker(),
    )
