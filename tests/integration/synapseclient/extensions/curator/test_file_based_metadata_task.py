"""Integration tests for create_file_based_metadata_task."""

import uuid
from typing import Any, Callable, Generator

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.extensions.curator.file_based_metadata_task import (
    create_file_based_metadata_task,
)
from synapseclient.models import (
    CurationTask,
    EntityView,
    Folder,
    JSONSchema,
    Project,
    SchemaOrganization,
)


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


class TestCreateFileBasedMetadataTask:
    """Integration tests for create_file_based_metadata_task."""

    def test_create_file_based_metadata_task(
        self,
        syn: Synapse,
        schedule_for_cleanup: Callable[[Any], None],
        request: pytest.FixtureRequest,
        patient_schema_uri: str,
        folder: Folder,
    ) -> None:
        """Test successful creation of CurationTask and EntityView"""
        # GIVEN a folder, a Patient JSON schema, and a unique task name
        test_name = _test_name()

        # WHEN a file-based metadata task is created for the folder
        entity_view_id, task_id = create_file_based_metadata_task(
            folder_id=folder.id,
            curation_task_name=test_name,
            instructions="Contribute Patient data.",
            entity_view_name=test_name,
            schema_uri=patient_schema_uri,
            synapse_client=syn,
        )
        schedule_for_cleanup(entity_view_id)

        # The CurationTask is not a deletable entity via syn.delete, so it keeps a
        # dedicated finalizer. The EntityView it points at is cleaned up via
        # schedule_for_cleanup above, so delete_source is not needed here.
        request.addfinalizer(
            lambda: CurationTask(task_id=task_id).delete(synapse_client=syn)
        )

        # THEN both the entity view and the curation task were created
        assert entity_view_id is not None
        assert task_id is not None

        # AND the entity view exists in Synapse with the expected name
        view = EntityView(id=entity_view_id).get(
            include_columns=True, synapse_client=syn
        )
        assert view.id == entity_view_id
        assert view.name == test_name

        # AND the curation task exists in Synapse pointing at that view
        task = CurationTask(task_id=task_id).get(synapse_client=syn)
        assert task.task_id == task_id
        assert task.data_type == test_name
        assert task.task_properties.file_view_id == entity_view_id

        # AND the leading columns are ordered with "name" first
        column_names = list(view.columns.keys())
        assert column_names[:3] == ["name", "id", "createdBy"]
