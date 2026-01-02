"""Integration tests for utility operations asynchronous."""
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import File, Folder, Project
from synapseclient.operations import (
    find_entity_id_async,
    is_synapse_id_async,
    md5_query_async,
    onweb_async,
)


class TestUtilityOperationsAsync:
    """Tests for the utility factory functions (asynchronous)."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_find_entity_id_async_project_by_name(
        self, project_model: Project
    ) -> None:
        """Test finding a project by name asynchronously."""
        # GIVEN a project exists
        project_name = project_model.name

        # WHEN I search for the project by name
        found_id = await find_entity_id_async(
            name=project_name, synapse_client=self.syn
        )

        # THEN I expect to find the correct project ID
        assert found_id is not None
        assert found_id == project_model.id

    async def test_find_entity_id_async_project_by_name_not_found(self) -> None:
        """Test finding a project that doesn't exist asynchronously."""
        # GIVEN a project name that doesn't exist
        fake_project_name = f"NonExistentProject_{uuid.uuid4()}"

        # WHEN I search for the project by name
        found_id = await find_entity_id_async(
            name=fake_project_name, synapse_client=self.syn
        )

        # THEN I expect None to be returned
        assert found_id is None

    async def test_find_entity_id_async_file_by_name_in_parent(
        self, project_model: Project
    ) -> None:
        """Test finding a file by name within a parent folder asynchronously."""
        # GIVEN a file stored in a project
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file_name = f"test_file_{str(uuid.uuid4())[:8]}.txt"
        file = File(
            path=filename,
            parent_id=project_model.id,
            name=file_name,
            description="Test file for find_entity_id_async",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None

        # WHEN I search for the file by name and parent
        found_id = await find_entity_id_async(
            name=file_name, parent=project_model.id, synapse_client=self.syn
        )

        # THEN I expect to find the correct file ID
        assert found_id is not None
        assert found_id == file.id

    async def test_find_entity_id_async_file_not_found_in_parent(
        self, project_model: Project
    ) -> None:
        """Test finding a file that doesn't exist in parent asynchronously."""
        # GIVEN a file name that doesn't exist
        fake_file_name = f"nonexistent_{uuid.uuid4()}.txt"

        # WHEN I search for the file by name and parent
        found_id = await find_entity_id_async(
            name=fake_file_name, parent=project_model.id, synapse_client=self.syn
        )

        # THEN I expect None to be returned
        assert found_id is None

    async def test_find_entity_id_async_with_parent_object(
        self, project_model: Project
    ) -> None:
        """Test finding an entity using a parent object instead of ID asynchronously."""
        # GIVEN a folder in a project
        folder = Folder(
            name=f"test_folder_{str(uuid.uuid4())[:8]}",
            parent_id=project_model.id,
        )
        folder = await folder.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # WHEN I search for the folder by name using parent object
        found_id = await find_entity_id_async(
            name=folder.name, parent=project_model, synapse_client=self.syn
        )

        # THEN I expect to find the correct folder ID
        assert found_id is not None
        assert found_id == folder.id

    async def test_is_synapse_id_async_valid(self, project_model: Project) -> None:
        """Test checking if a valid Synapse ID exists asynchronously."""
        # GIVEN a valid project ID
        project_id = project_model.id

        # WHEN I check if the ID is valid
        is_valid = await is_synapse_id_async(project_id, synapse_client=self.syn)

        # THEN I expect it to be valid
        assert is_valid is True

    async def test_is_synapse_id_async_invalid(self) -> None:
        """Test checking an invalid Synapse ID asynchronously."""
        # GIVEN an invalid Synapse ID
        invalid_id = "syn999999999999999"

        # WHEN I check if the ID is valid
        is_valid = await is_synapse_id_async(invalid_id, synapse_client=self.syn)

        # THEN I expect it to be invalid
        assert is_valid is False

    async def test_is_synapse_id_async_not_string(self) -> None:
        """Test checking a non-string value asynchronously."""
        # GIVEN a non-string value
        not_string = 123456

        # WHEN I check if it's a valid Synapse ID
        is_valid = await is_synapse_id_async(not_string, synapse_client=self.syn)

        # THEN I expect it to be invalid
        assert is_valid is False

    async def test_md5_query_async_file(self, project_model: Project) -> None:
        """Test finding entities by MD5 hash asynchronously."""
        # GIVEN a file stored in synapse
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            name=f"test_file_{str(uuid.uuid4())[:8]}.txt",
            description="Test file for MD5 query async",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None
        assert file.file_handle is not None
        assert file.file_handle.content_md5 is not None

        # WHEN I query for entities with this MD5
        md5_hash = file.file_handle.content_md5
        results = await md5_query_async(md5_hash, synapse_client=self.syn)

        # THEN I expect to find at least this file in the results
        assert isinstance(results, list)
        assert len(results) > 0
        found_ids = [result["id"] for result in results]
        assert file.id in found_ids

    async def test_md5_query_async_nonexistent(self) -> None:
        """Test querying for a nonexistent MD5 hash asynchronously."""
        # GIVEN a fake MD5 hash that doesn't exist
        fake_md5 = "00000000000000000000000000000000"

        # WHEN I query for entities with this MD5
        results = await md5_query_async(fake_md5, synapse_client=self.syn)

        # THEN I expect an empty list
        assert isinstance(results, list)
        assert len(results) == 0

    async def test_onweb_async_project_by_id(self, project_model: Project) -> None:
        """Test opening a project in web browser by ID asynchronously."""
        # GIVEN a project exists
        project_id = project_model.id

        # WHEN I call onweb_async with the project ID
        url = await onweb_async(project_id, synapse_client=self.syn)

        # THEN I expect a valid Synapse URL to be returned
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_id in url
        assert "#!Synapse:" in url

    async def test_onweb_async_project_by_object(self, project_model: Project) -> None:
        """Test opening a project in web browser by object asynchronously."""
        # GIVEN a project exists
        # WHEN I call onweb_async with the project object
        url = await onweb_async(project_model, synapse_client=self.syn)

        # THEN I expect a valid Synapse URL to be returned
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_model.id in url
        assert "#!Synapse:" in url

    async def test_onweb_async_with_subpage(self, project_model: Project) -> None:
        """Test opening a wiki subpage in web browser asynchronously."""
        # GIVEN a project exists and a subpage ID
        project_id = project_model.id
        subpage_id = "12345"

        # WHEN I call onweb_async with subpage_id
        url = await onweb_async(
            project_id, subpage_id=subpage_id, synapse_client=self.syn
        )

        # THEN I expect a valid Synapse URL with wiki reference
        assert url is not None
        assert isinstance(url, str)
        assert "synapse.org" in url.lower()
        assert project_id in url
        assert subpage_id in url
        assert "#!Wiki:" in url
        assert "/ENTITY/" in url
