"""Shared fixtures for curator metadata-task integration tests."""

import uuid
from typing import Any, Callable, Generator

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Folder, JSONSchema, Project, SchemaOrganization


def _test_name() -> str:
    random_string = "".join(c for c in str(uuid.uuid4()) if c.isalpha())
    return f"SYNPY.TEST.{random_string}"


@pytest.fixture(scope="session")
def unique_name() -> Callable[[], str]:
    """Factory returning unique SYNPY.TEST.* names.

    Session-scoped so that module- and class-scoped fixtures can depend on it
    (a narrower scope would forbid those wider-scoped fixtures from using it).
    """
    return _test_name


@pytest.fixture(scope="module")
def patient_schema_uri(syn: Synapse, request: pytest.FixtureRequest) -> str:
    """
    Create a SchemaOrganization and a Patient JSON schema for the module.
    Returns the schema URI.
    """
    org_name = _test_name()
    schema_name = "test.schematic.Patient"

    org = SchemaOrganization(name=org_name)
    org.store(synapse_client=syn)

    schema = JSONSchema(name=schema_name, organization_name=org_name)
    schema_body = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": f"https://example.com/schema/{org_name}-{schema_name}.json",
        "title": "Patient",
        "type": "object",
        "properties": {
            "PatientID": {"type": "string"},
            "Sex": {"type": "string", "enum": ["Male", "Female", "Other"]},
            "Age": {"type": "integer", "minimum": 0},
        },
        "required": ["PatientID"],
    }
    schema.store(schema_body=schema_body, synapse_client=syn)

    def cleanup():
        for js in org.get_json_schemas(synapse_client=syn):
            js.delete(synapse_client=syn)
        org.delete(synapse_client=syn)

    request.addfinalizer(cleanup)

    return schema.uri


@pytest.fixture(scope="function")
def folder(
    syn: Synapse,
    project: Project,
    schedule_for_cleanup: Callable[[Any], None],
) -> Generator[Folder, None, None]:
    """Returns a Synapse Folder"""
    folder = Folder(name=_test_name(), parent_id=project.id).store(synapse_client=syn)
    schedule_for_cleanup(folder.id)
    yield folder
    # Unbind any JSON schema from the folder before teardown so the module-scoped
    # schema cleanup (which runs before the session-scoped folder deletion) is not
    # blocked by "Cannot delete a schema that is bound to an object".
    try:
        folder.unbind_schema(synapse_client=syn)
    except SynapseHTTPError:
        # No schema bound to this folder; nothing to unbind.
        pass
