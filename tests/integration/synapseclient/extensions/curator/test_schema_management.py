"""Integration tests for schema management wrapper functions (register and bind)"""
import json
import os
import tempfile
import uuid

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator import bind_jsonschema, register_jsonschema
from synapseclient.extensions.curator.record_based_metadata_task import (
    project_id_from_entity_id,
)
from synapseclient.models import Folder, Project, SchemaOrganization


def create_test_name():
    """Creates a random string for naming test entities"""
    random_string = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    return f"SYNPY.TEST.{random_string}"


@pytest.fixture(name="test_organization", scope="module")
def fixture_test_organization(syn: Synapse, request) -> SchemaOrganization:
    """
    Returns a created organization for testing schema registration
    """
    org = SchemaOrganization(create_test_name())
    org.store(synapse_client=syn)

    def delete_org():
        # Delete all schemas in the organization
        for schema in org.get_json_schemas(synapse_client=syn):
            schema.delete(synapse_client=syn)
        org.delete(synapse_client=syn)

    request.addfinalizer(delete_org)
    return org


@pytest.fixture(name="test_project", scope="module")
def fixture_test_project(syn: Synapse, request) -> Project:
    """
    Returns a test project for binding schemas
    """
    project = Project(name=create_test_name())
    project.store(synapse_client=syn)

    def delete_project():
        project.delete(synapse_client=syn)

    request.addfinalizer(delete_project)
    return project


@pytest.fixture(name="test_schema_file", scope="function")
def fixture_test_schema_file(request):
    """
    Creates a temporary JSON schema file for testing
    """
    schema_definition = {
        "$id": "test.schema",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        "required": ["name"],
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(schema_definition, temp_file)
        temp_path = temp_file.name

    def cleanup():
        if os.path.exists(temp_path):
            os.remove(temp_path)

    request.addfinalizer(cleanup)
    return temp_path


class TestRegisterJsonSchema:
    """Integration tests for register_jsonschema wrapper function"""

    def test_register_jsonschema_with_version(
        self, syn: Synapse, test_organization: SchemaOrganization, test_schema_file: str
    ):
        """Test registering a JSON schema with a specific version"""
        schema_name = create_test_name()
        version = "1.0.0"

        # Register the schema
        json_schema = register_jsonschema(
            schema_path=test_schema_file,
            organization_name=test_organization.name,
            schema_name=schema_name,
            schema_version=version,
            synapse_client=syn,
        )

        # Verify the schema was registered
        assert json_schema is not None
        assert json_schema.uri is not None
        assert json_schema.name == schema_name
        assert test_organization.name in json_schema.uri

    def test_register_jsonschema_without_version(
        self, syn: Synapse, test_organization: SchemaOrganization, test_schema_file: str
    ):
        """Test registering a JSON schema without specifying a version"""
        schema_name = create_test_name()

        # Register the schema
        json_schema = register_jsonschema(
            schema_path=test_schema_file,
            organization_name=test_organization.name,
            schema_name=schema_name,
            synapse_client=syn,
        )

        # Verify the schema was registered
        assert json_schema is not None
        assert json_schema.uri is not None
        assert json_schema.name == schema_name


class TestBindJsonSchema:
    """Integration tests for bind_jsonschema wrapper function"""

    def test_bind_jsonschema_to_folder(
        self,
        syn: Synapse,
        test_organization: SchemaOrganization,
        test_project: Project,
        test_schema_file: str,
    ):
        """Test binding a JSON schema to a folder"""
        # First register a schema
        schema_name = create_test_name()
        json_schema = register_jsonschema(
            schema_path=test_schema_file,
            organization_name=test_organization.name,
            schema_name=schema_name,
            schema_version="1.0.0",
            synapse_client=syn,
        )

        # Create a test folder
        folder = Folder(name=create_test_name(), parent_id=test_project.id)
        folder.store(synapse_client=syn)

        try:
            # Bind the schema to the folder
            result = bind_jsonschema(
                entity_id=folder.id,
                json_schema_uri=json_schema.uri,
                enable_derived_annotations=False,
                synapse_client=syn,
            )

            # Verify the binding
            assert result is not None
        finally:
            # Cleanup: unbind schema before deleting folder
            folder.unbind_schema(synapse_client=syn)
            syn.delete(folder.id)

    def test_bind_jsonschema_with_derived_annotations(
        self,
        syn: Synapse,
        test_organization: SchemaOrganization,
        test_project: Project,
        test_schema_file: str,
    ):
        """Test binding a JSON schema with derived annotations enabled"""
        # Register a schema
        schema_name = create_test_name()
        json_schema = register_jsonschema(
            schema_path=test_schema_file,
            organization_name=test_organization.name,
            schema_name=schema_name,
            schema_version="1.0.0",
            synapse_client=syn,
        )

        # Create a test folder
        folder = Folder(name=create_test_name(), parent_id=test_project.id)
        folder.store(synapse_client=syn)

        try:
            # Bind the schema with derived annotations enabled
            result = bind_jsonschema(
                entity_id=folder.id,
                json_schema_uri=json_schema.uri,
                enable_derived_annotations=True,
                synapse_client=syn,
            )

            # Verify the binding
            assert result is not None
        finally:
            # Cleanup: unbind schema before deleting folder
            folder.unbind_schema(synapse_client=syn)
            syn.delete(folder.id)


class TestRegisterAndBindWorkflow:
    """Integration tests for the complete register + bind workflow"""

    def test_complete_workflow(
        self,
        syn: Synapse,
        test_organization: SchemaOrganization,
        test_project: Project,
        test_schema_file: str,
    ):
        """Test the complete workflow: register a schema and bind it to an entity"""
        schema_name = create_test_name()

        # Step 1: Register the schema
        json_schema = register_jsonschema(
            schema_path=test_schema_file,
            organization_name=test_organization.name,
            schema_name=schema_name,
            schema_version="1.0.0",
            synapse_client=syn,
        )

        assert json_schema is not None
        assert json_schema.uri is not None

        # Step 2: Create a folder
        folder = Folder(name=create_test_name(), parent_id=test_project.id)
        folder.store(synapse_client=syn)

        try:
            # Step 3: Bind the schema to the folder
            result = bind_jsonschema(
                entity_id=folder.id,
                json_schema_uri=json_schema.uri,
                enable_derived_annotations=True,
                synapse_client=syn,
            )

            # Verify the workflow completed successfully
            assert result is not None

            # Verify the schema is actually bound by retrieving it
            from synapseclient.operations import FileOptions, get

            retrieved_folder = get(
                file_options=FileOptions(download_file=False),
                synapse_id=folder.id,
                synapse_client=syn,
            )
            bound_schema = retrieved_folder.get_schema(synapse_client=syn)
            assert bound_schema is not None
        finally:
            # Cleanup: unbind schema before deleting folder
            folder.unbind_schema(synapse_client=syn)
            syn.delete(folder.id)


class TestProjectIDFromEntityID:
    @pytest.fixture(scope="module")
    def temp_hierarchy(self, syn: Synapse, request) -> tuple[str, str, str]:
        """Creates a Project -> Folder -> Folder hierarchy for testing."""
        project = Project(name=create_test_name()).store(synapse_client=syn)
        folder1 = Folder(name=create_test_name(), parent_id=project.id).store(
            synapse_client=syn
        )
        folder2 = Folder(name=create_test_name(), parent_id=folder1.id).store(
            synapse_client=syn
        )

        def delete_project():
            project.delete(synapse_client=syn)

        request.addfinalizer(delete_project)
        return project.id, folder1.id, folder2.id

    def test_project_id_from_folder(self, syn, temp_hierarchy):
        """Test finding project id when input id is from a nested folder."""
        folder_id = temp_hierarchy[2]
        expected_project_id = temp_hierarchy[0]

        result = project_id_from_entity_id(folder_id, syn)
        assert result == expected_project_id

    def test_project_id_from_project(self, syn, temp_hierarchy):
        """Test finding project id when input id is for a project"""
        project_id = temp_hierarchy[0]

        result = project_id_from_entity_id(project_id, syn)
        assert result == project_id
