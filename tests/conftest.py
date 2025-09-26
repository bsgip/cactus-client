from datetime import timedelta
from pathlib import Path
from typing import Callable

import pytest
from aiohttp import ClientSession
from assertical.fake.generator import generate_class_instance, register_value_generator
from assertical.fixtures.generator import generator_registry_snapshot
from cactus_test_definitions.server.test_procedures import (
    Preconditions,
    RequiredClient,
    TestProcedure,
    TestProcedureId,
)
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from cactus_client.model.config import (
    ClientConfig,
    ServerConfig,
)
from cactus_client.model.context import ClientContext, ExecutionContext, ResourceStore
from cactus_client.model.execution import StepExecution, StepExecutionList
from cactus_client.model.progress import (
    ProgressTracker,
    ResponseTracker,
    WarningTracker,
)
from cactus_client.model.resource import CSIPAusResourceTree
from cactus_client.time import utc_now


@pytest.fixture
def dummy_client_alias_1():
    return "my-client-1"


@pytest.fixture
def assertical_extensions():
    with generator_registry_snapshot():
        register_value_generator(dict, lambda _: {})
        yield


@pytest.fixture
def dummy_test_procedure(dummy_client_alias_1, assertical_extensions) -> TestProcedure:
    return generate_class_instance(
        TestProcedure,
        optional_is_none=True,
        generate_relationships=True,
        preconditions=generate_class_instance(
            Preconditions,
            optional_is_none=True,
            required_clients=[generate_class_instance(RequiredClient, id=dummy_client_alias_1)],
        ),
    )


@pytest.fixture
def testing_contexts_factory(dummy_test_procedure) -> Callable[[ClientSession], tuple[ExecutionContext, StepExecution]]:
    """Returns a callable(session: ClientSession) that when executed will yield a tuple
    containing a fully populated ExecutionContext and StepExcecution"""

    def create_testing_contexts(client_session) -> tuple[ExecutionContext, StepExecution]:
        tree = CSIPAusResourceTree()
        client_alias = dummy_test_procedure.preconditions.required_clients[0].id
        client_context = ClientContext(
            test_procedure_alias=client_alias,
            client_config=generate_class_instance(ClientConfig, optional_is_none=True),
            discovered_resources=ResourceStore(tree),
            session=client_session,
        )

        execution_context = ExecutionContext(
            TestProcedureId.S_ALL_01,
            dummy_test_procedure,
            "1.2.3.4.5",
            Path("."),  # Just a dummy value
            "/my/dcap/path",
            generate_class_instance(ServerConfig),
            {client_alias: client_context},
            StepExecutionList(),
            WarningTracker(),
            ProgressTracker(),
            ResponseTracker(),
            tree,
        )

        # attempts: int  # How many times has this step been attempted
        step_execution = generate_class_instance(
            StepExecution,
            optional_is_none=True,
            generate_relationships=True,
            client_alias=client_alias,
            client_resources_alias=client_alias,
        )

        return (execution_context, step_execution)

    return create_testing_contexts


@pytest.fixture
def generate_testing_key_cert():
    """Shared function for generating a self signed PEM encoded RSA key + cert to a location on disk"""

    def _generate_testing_key_cert(key_file: Path, cert_file: Path):

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "AU"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "ACT"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Canberra"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Australian National University"),
                x509.NameAttribute(NameOID.COMMON_NAME, cert_file.name),
            ]
        )

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(utc_now())
            .not_valid_after(utc_now() + timedelta(hours=1))
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )
            .sign(private_key=key, algorithm=hashes.SHA256())
        )

        with open(key_file, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,  # PKCS#1 format
                    encryption_algorithm=serialization.NoEncryption(),  # No password
                )
            )
        with open(cert_file, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

    return _generate_testing_key_cert
