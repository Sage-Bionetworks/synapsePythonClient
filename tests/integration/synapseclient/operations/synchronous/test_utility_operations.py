"""Integration tests for utility operations synchronous."""
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import File, Folder, Project
from synapseclient.operations import find_entity_id, is_synapse_id, md5_query, onweb


class TestUtilityOperations:
    """Tests for the utility factory functions (synchronous)."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_find_entity_id_project_by_name(self, project_model: Project) -> None:
        """Test finding a project by name."""
        # GIVEN a project exists
        project_name = project_model.name

        # WHEN I search for the project by name
        found_id = find_entity_id(name=project_name, synapse_client=self.syn)

        # THEN I expect to find the correct project ID
        assert found_id is not None
        assert found_id == project_model.id

    def test_find_entity_id_project_by_name_not_found(self) -> None:
        """Test finding a project that doesn't exist."""
        # GIVEN a project name that doesn't exist
        fake_project_name = f"NonExistentProject_{uuid.uuid4()}"

        # WHEN I search for the project by name
        found_id = find_entity_id(name=fake_project_name, synapse_client=self.syn)

        # THEN I expect None to be returned
        assert found_id is None

    def test_find_entity_id_file_by_name_in_parent(
        self, project_model: Project
    ) -> None:
        """Test finding a file by name within a parent folder."""
        # GIVEN a file stored in a project
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file_name = f"test_file_{str(uuid.uuid4())[:8]}.txt"
        file = File(
            path=filename,
            parent_id=project_model.id,
            name=file_name,
            description="Test file for find_entity_id",
        )
        file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None

        # WHEN I search for the file by name and parent
        found_id = find_entity_id(
            name=file_name, parent=project_model.id, synapse_client=self.syn
        )

        # THEN I expect to find the correct file ID
        assert found_id is not None
        assert found_id == file.id

    def test_find_entity_id_file_not_found_in_parent(
        self, project_model: Project
    ) -> None:
        """Test finding a file that doesn't exist in parent."""
        # GIVEN a file name that doesn't exist
        fake_file_name = f"nonexistent_{uuid.uuid4()}.txt"

        # WHEN I search for the file by name and parent
        found_id = find_entity_id(
            name=fake_file_name, parent=project_model.id, synapse_client=self.syn
        )

        # THEN I expect None to be returned
        assert found_id is None

    def test_find_entity_id_with_parent_object(self, project_model: Project) -> None:
        """Test finding an entity using a parent object instead of ID."""
        # GIVEN a folder in a project
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
        )
        folder = folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # WHEN I search for the folder by name using parent object
        found_id = find_entity_id(
            name=folder.name, parent=project_model, synapse_client=self.syn
        )

        # THEN I expect to find the correct folder ID
        assert found_id is not None
        assert found_id == folder.id

    def test_is_synapse_id_valid(self, project_model: Project) -> None:
        """Test checking if a valid Synapse ID exists."""
        # GIVEN a valid project ID
        project_id = project_model.id

        # WHEN I check if the ID is valid
        is_valid = is_synapse_id(project_id, synapse_client=self.syn)

        # THEN I expect it to be valid
        assert is_valid is True

    def test_is_synapse_id_invalid(self) -> None:
        """Test checking an invalid Synapse ID."""
        # GIVEN an invalid Synapse ID
        invalid_id = "syn999999999999999"

        # WHEN I check if the ID is valid
        is_valid = is_synapse_id(invalid_id, synapse_client=self.syn)

        # THEN I expect it to be invalid
        assert is_valid is False

    def test_is_synapse_id_not_string(self) -> None:
        """Test checking a non-string value."""
        # GIVEN a non-string value
        not_string = 123456

        # WHEN I check if it's a valid Synapse ID
        is_valid = is_synapse_id(not_string, synapse_client=self.syn)

        # THEN I expect it to be invalid
        assert is_valid is False

    def test_md5_query_file(self, project_model: Project) -> None:
        """Test finding entities by MD5 hash."""
        # GIVEN a file stored in synapse
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            name=f"test_file_{str(uuid.uuid4())[:8]}.txt",
            description="Test file for MD5 query",
        )
        file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None
        assert file.file_handle is not None
        assert file.file_handle.content_md5 is not None

        # WHEN I query for entities with this MD5
        md5_hash = file.file_handle.content_md5
        results = md5_query(md5_hash, synapse_client=self.syn)

        # THEN I expect to find at least this file in the results
        assert isinstance(results, list)
        assert len(results) > 0
        found_ids = [result["id"] for result in results]
        assert file.id in found_ids

    def test_md5_query_nonexistent(self) -> None:
        """Test querying for a nonexistent MD5 hash."""
        # GIVEN a fake MD5 hash that doesn't exist
        fake_md5 = "00000000000000000000000000000000"

        # WHEN I query for entities with this MD5
        results = md5_query(fake_md5, synapse_client=self.syn)

        # THEN I expect an empty list
        assert isinstance(results, list)
        assert len(results) == 0

    def test_onweb_project_by_id(self, project_model: Project) -> None:
        """Test opening a project in web browser by ID."""
        # GIVEN a project exists
        project_id = project_model.id

        # WHEN I call onweb with the project ID
        url = onweb(project_id, synapse_client=self.syn)

        # THEN I expect a valid Synapse URL to be returned
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_id in url
        assert "Synapse:" in url

    def test_onweb_project_by_object(self, project_model: Project) -> None:
        """Test opening a project in web browser by object."""
        # GIVEN a project exists
        # WHEN I call onweb with the project object
        url = onweb(project_model, synapse_client=self.syn)

        # THEN I expect a valid Synapse URL to be returned
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_model.id in url
        assert "Synapse:" in url

    def test_onweb_with_subpage(self, project_model: Project) -> None:
        """Test opening a wiki subpage in web browser."""
        # GIVEN a project exists and a subpage ID
        project_id = project_model.id
        subpage_id = "12345"

        # WHEN I call onweb with subpage_id
        url = onweb(project_id, subpage_id=subpage_id, synapse_client=self.syn)

        # THEN I expect a valid Synapse URL with wiki reference
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_id in url
        assert subpage_id in url
        assert "Wiki:" in url
        assert "/ENTITY/" in url
