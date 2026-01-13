"""Integration tests for delete operations async."""
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File, Project, RecordSet
from synapseclient.operations import delete_async


class TestDeleteOperationsAsync:
    """Tests for the delete_async factory function."""

    @pytest.fixture(autouse=True, scope="function")
    def init(
        self, syn_with_logger: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        self.syn = syn_with_logger
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_delete_file_by_id_string(self, project_model: Project) -> None:
        """Test deleting a file using a string ID."""
        # GIVEN a file stored in synapse
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file for deletion",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None

        # WHEN I delete the file using string ID
        await delete_async(file.id, synapse_client=self.syn)

        # THEN I expect the file to be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await File(id=file.id).get_async(synapse_client=self.syn)
        assert f"404 Client Error: Entity {file.id} is in trash can." in str(e.value)

    async def test_delete_file_by_object(self, project_model: Project) -> None:
        """Test deleting a file using a File object."""
        # GIVEN a file stored in synapse
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file for deletion",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.id is not None

        # WHEN I delete the file using File object
        await delete_async(file, synapse_client=self.syn)

        # THEN I expect the file to be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await File(id=file.id).get_async(synapse_client=self.syn)
        assert f"404 Client Error: Entity {file.id} is in trash can." in str(e.value)

    async def test_delete_file_specific_version_with_version_param(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test deleting a specific version using version parameter (highest priority)."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.version_number == 1

        # Create version 2
        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)
        assert file.version_number == 2

        # Create version 3
        file.description = "Test file version 3"
        file = await file.store_async(synapse_client=self.syn)
        assert file.version_number == 3

        # WHEN I delete version 2 using version parameter with version_only=True
        file_v2 = File(id=file.id, version_number=999)  # Set wrong version on entity

        # Capture logs
        caplog.clear()
        await delete_async(
            file_v2,
            version=2,  # This should take precedence over entity's version_number
            version_only=True,
            synapse_client=self.syn,
        )

        # Check that warning was logged
        assert any("Version conflict" in record.message for record in caplog.records)
        assert any(
            "version' parameter (2)" in record.message for record in caplog.records
        )

        # THEN version 2 should be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await File(id=file.id, version_number=2).get_async(synapse_client=self.syn)
        assert f"Cannot find a node with id {file.id} and version 2" in str(e.value)

        # AND version 1 and 3 should still exist
        file_v1 = await File(id=file.id, version_number=1).get_async(
            synapse_client=self.syn
        )
        assert file_v1.version_number == 1

        file_v3 = await File(id=file.id, version_number=3).get_async(
            synapse_client=self.syn
        )
        assert file_v3.version_number == 3

    async def test_delete_file_specific_version_with_entity_version_number(
        self, project_model: Project
    ) -> None:
        """Test deleting a specific version using entity's version_number attribute."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.version_number == 1

        # Create version 2
        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)
        assert file.version_number == 2

        # WHEN I delete version 1 using entity's version_number
        file_v1 = File(id=file.id, version_number=1)

        await delete_async(file_v1, version_only=True, synapse_client=self.syn)

        # THEN version 1 should be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await File(id=file.id, version_number=1).get_async(synapse_client=self.syn)
        assert f"Cannot find a node with id {file.id} and version 1" in str(e.value)

        # AND version 2 should still exist
        file_v2 = await File(id=file.id, version_number=2).get_async(
            synapse_client=self.syn
        )
        assert file_v2.version_number == 2

    async def test_delete_file_specific_version_with_id_string(
        self, project_model: Project
    ) -> None:
        """Test deleting a specific version using ID string with version."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        assert file.version_number == 1

        # Create version 2
        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)
        assert file.version_number == 2

        # WHEN I delete version 1 using string ID with version
        await delete_async(f"{file.id}.1", version_only=True, synapse_client=self.syn)

        # THEN version 1 should be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await File(id=file.id, version_number=1).get_async(synapse_client=self.syn)
        assert f"Cannot find a node with id {file.id} and version 1" in str(e.value)

        # AND version 2 should still exist
        file_v2 = await File(id=file.id, version_number=2).get_async(
            synapse_client=self.syn
        )
        assert file_v2.version_number == 2

    async def test_delete_recordset_specific_version(
        self, project_model: Project
    ) -> None:
        """Test deleting a specific version of a RecordSet."""
        # GIVEN a RecordSet with multiple versions
        filename1 = utils.make_bogus_uuid_file()
        filename2 = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename1)
        self.schedule_for_cleanup(filename2)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename1,
            description="RecordSet version 1",
            parent_id=project_model.id,
            upsert_keys=["id"],
        )
        record_set = await record_set.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)
        assert record_set.version_number == 1

        # Create version 2
        record_set.path = filename2
        record_set.description = "RecordSet version 2"
        record_set = await record_set.store_async(synapse_client=self.syn)
        assert record_set.version_number == 2

        # WHEN I delete version 2 using version parameter
        await delete_async(
            RecordSet(id=record_set.id, version_number=2),
            version_only=True,
            synapse_client=self.syn,
        )

        # THEN version 2 should be gone and version 1 should be current
        current_record_set = await RecordSet(id=record_set.id).get_async(
            synapse_client=self.syn
        )
        assert current_record_set.version_number == 1
        assert current_record_set.description == "RecordSet version 1"

    async def test_delete_version_only_without_version_raises_error(
        self, project_model: Project
    ) -> None:
        """Test that version_only=True without a version number raises an error."""
        # GIVEN a file without version_number set
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # WHEN I try to delete with version_only=True but no version
        file_no_version = File(id=file.id)

        # THEN it should raise ValueError
        with pytest.raises(ValueError) as e:
            await delete_async(
                file_no_version, version_only=True, synapse_client=self.syn
            )
        assert "version_only=True requires a version number" in str(e.value)

    async def test_delete_project_ignores_version_parameters(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that deleting a Project ignores version parameters with warning."""
        # GIVEN a project
        project = Project(
            name=str(uuid.uuid4()),
            description="Test project for version parameter",
        )
        project = await project.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # WHEN I try to delete with version_only=True
        caplog.clear()
        await delete_async(project, version_only=True, synapse_client=self.syn)

        # THEN warnings should be logged
        assert any(
            "does not support version-specific deletion" in record.message
            for record in caplog.records
        )

        # AND the entire project should be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await Project(id=project.id).get_async(synapse_client=self.syn)
        assert f"404 Client Error: Entity {project.id} is in trash can." in str(e.value)

    async def test_delete_invalid_synapse_id_raises_error(self) -> None:
        """Test that an invalid Synapse ID raises an error."""
        # WHEN I try to delete with an invalid ID
        # THEN it should raise ValueError
        with pytest.raises(ValueError) as e:
            await delete_async("invalid_id", synapse_client=self.syn)
        assert "Invalid Synapse ID: invalid_id" in str(e.value)

    async def test_delete_with_dot_notation_without_version_only_raises_error(
        self, project_model: Project
    ) -> None:
        """Test that using dot notation without version_only=True raises an error."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # WHEN I try to delete with dot notation but without version_only=True
        # THEN it should raise ValueError
        with pytest.raises(ValueError) as e:
            await delete_async(f"{file.id}.1", synapse_client=self.syn)
        assert "Deleting a specific version requires version_only=True" in str(e.value)
        assert f"delete('{file.id}.1', version_only=True)" in str(e.value)

    async def test_version_precedence_version_param_over_entity_attribute(
        self, project_model: Project
    ) -> None:
        """Test that version parameter takes precedence over entity's version_number."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)

        file.description = "Test file version 3"
        file = await file.store_async(synapse_client=self.syn)

        # WHEN I have entity with version_number=1 but pass version=2
        file_entity = File(id=file.id, version_number=1)

        # version=2 should take precedence
        await delete_async(
            file_entity, version=2, version_only=True, synapse_client=self.syn
        )

        # THEN version 2 should be deleted (not version 1)
        with pytest.raises(SynapseHTTPError):
            await File(id=file.id, version_number=2).get_async(synapse_client=self.syn)

        # AND version 1 should still exist
        file_v1 = await File(id=file.id, version_number=1).get_async(
            synapse_client=self.syn
        )
        assert file_v1.version_number == 1

    async def test_delete_version_param_without_conflict_no_warning(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that no warning is logged when version parameter is used without conflict."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)

        # WHEN I delete with version parameter but entity has no version_number set
        file_no_version = File(id=file.id)  # No version_number attribute set

        caplog.clear()
        await delete_async(
            file_no_version, version=1, version_only=True, synapse_client=self.syn
        )

        # THEN no version conflict warning should be logged
        assert not any(
            "Version conflict" in record.message for record in caplog.records
        )

        # AND version 1 should be deleted
        with pytest.raises(SynapseHTTPError):
            await File(id=file.id, version_number=1).get_async(synapse_client=self.syn)

    async def test_delete_folder_with_version_only_logs_warning(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that deleting a Folder with version_only=True logs a warning."""
        from synapseclient.models import Folder

        # GIVEN a folder
        folder = Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        )
        folder = await folder.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # WHEN I try to delete with version_only=True
        caplog.clear()
        await delete_async(folder, version_only=True, synapse_client=self.syn)

        # THEN warning should be logged
        assert any(
            "does not support version-specific deletion" in record.message
            for record in caplog.records
        )
        assert any("Folder" in record.message for record in caplog.records)

    async def test_no_warning_when_version_only_false_despite_conflict(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that no warning is logged when version_only=False even with version conflict."""
        # GIVEN a file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # WHEN I delete with version parameter but version_only=False
        file_entity = File(id=file.id, version_number=999)

        caplog.clear()
        await delete_async(
            file_entity,
            version=1,
            version_only=False,  # Not deleting specific version
            synapse_client=self.syn,
        )

        # THEN no version conflict warning should be logged (since version_only=False)
        assert not any(
            "Version conflict" in record.message for record in caplog.records
        )

    async def test_delete_file_with_version_number_none_no_warning(
        self, project_model: Project, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that no warning when entity.version_number is explicitly None."""
        # GIVEN a file with multiple versions
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        file = File(
            path=filename,
            parent_id=project_model.id,
            description="Test file version 1",
        )
        file = await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        file.description = "Test file version 2"
        file = await file.store_async(synapse_client=self.syn)

        # WHEN I have entity with version_number=None but pass version parameter
        file_entity = File(id=file.id, version_number=None)

        caplog.clear()
        await delete_async(
            file_entity, version=1, version_only=True, synapse_client=self.syn
        )

        # THEN no conflict warning should be logged (None is not a conflict)
        assert not any(
            "Version conflict" in record.message for record in caplog.records
        )
