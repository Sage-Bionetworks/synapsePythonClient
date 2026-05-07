"""Integration tests for create_record_based_metadata_task."""

import uuid

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator.record_based_metadata_task import (
    create_record_based_metadata_task,
)
from synapseclient.models import Folder, JSONSchema, Project, SchemaOrganization


def _test_name() -> str:
    random_string = "".join(c for c in str(uuid.uuid4()) if c.isalpha())
    return f"SYNPY.TEST.{random_string}"


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
def folder(syn: Synapse, project: Project, request: pytest.FixtureRequest) -> Folder:
    """Create a Folder for the test and tear it down on completion.

    The finalizer unbinds any JSON schema from the folder before deletion so
    that the module-scoped schema cleanup in patient_schema_uri can succeed
    (the server refuses to delete a schema that is still bound to an entity).
    """
    folder = Folder(name=_test_name(), parent_id=project.id).store(synapse_client=syn)

    def cleanup():
        folder.unbind_schema(synapse_client=syn)
        folder.delete(synapse_client=syn)

    request.addfinalizer(cleanup)

    return folder


class TestCreateRecordBasedMetadataTask:
    """Integration tests for create_record_based_metadata_task."""

    def test_creates_single_record_set_version(
        self,
        syn: Synapse,
        project: Project,
        request: pytest.FixtureRequest,
        patient_schema_uri: str,
        folder: Folder,
    ):
        """
        The Grid created during bootstrap is initialized from the RecordSet's
        CSV with no edits, so exporting that Grid back to the RecordSet (the
        reported bug) writes the same content as a duplicate v2.
        """
        test_name = _test_name()
        upsert_keys = ["PatientID"]
        instructions = "Curate per the schema."

        record_set, curation_task, grid = create_record_based_metadata_task(
            folder_id=folder.id,
            record_set_name=test_name,
            record_set_description=test_name,
            curation_task_name=test_name,
            upsert_keys=upsert_keys,
            instructions=instructions,
            schema_uri=patient_schema_uri,
            synapse_client=syn,
        )

        def cleanup():
            curation_task.delete(synapse_client=syn)
            grid.delete(synapse_client=syn)
            record_set.unbind_schema(synapse_client=syn)
            record_set.delete(synapse_client=syn)

        request.addfinalizer(cleanup)

        from synapseclient.models import RecordSet

        record_set = RecordSet(id=record_set.id).get(synapse_client=syn)

        assert grid.record_set_id == record_set.id
        assert grid.grid_json_schema_id == patient_schema_uri

        assert record_set.upsert_keys == upsert_keys
        assert record_set.version_number == 1
        assert record_set.parent_id == folder.id
        assert record_set.name == test_name
        assert record_set.description == test_name

        assert curation_task.data_type == test_name
        assert curation_task.project_id == project.id
        assert curation_task.instructions == instructions
        assert curation_task.task_properties.record_set_id == record_set.id
