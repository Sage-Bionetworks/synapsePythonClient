import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator.record_based_metadata_task import (
    project_id_from_entity_id,
)
from synapseclient.models import Folder, Project


class TestProjectIDFromEntityID:
    @pytest.fixture(scope="module")
    def temp_hierarchy(self, syn: Synapse, request) -> tuple[str, str, str]:
        """Creates a Project -> Folder -> Folder hierarchy for testing."""
        project = Project(name="IntegrationTest_Root_Project").store(synapse_client=syn)
        folder1 = Folder(name="TestFolder1", parent_id=project.id).store(
            synapse_client=syn
        )
        folder2 = Folder(name="TestFolder2", parent_id=folder1.id).store(
            synapse_client=syn
        )

        def delete_project():
            project.delete(synapse_client=syn)

        request.addfinalizer(delete_project)
        return project.id, folder1.id, folder2.id

    def test_project_id_from_folder(self, syn, temp_hierarchy):
        # Test finding project from a nested file
        folder_id = temp_hierarchy[2]
        expected_project_id = temp_hierarchy[0]

        result = project_id_from_entity_id(folder_id, syn)
        assert result == expected_project_id

    def test_project_id_from_project(self, syn, temp_hierarchy):
        # Test finding project from a nested file
        project_id = temp_hierarchy[0]

        result = project_id_from_entity_id(project_id, syn)
        assert result == project_id
