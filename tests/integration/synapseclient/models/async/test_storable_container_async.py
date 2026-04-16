"""Integration tests for StorableContainer"""

import csv
import uuid
from pathlib import Path
from typing import Callable

import pytest

import synapseclient.core.utils as utils
from synapseclient import Synapse
from synapseclient.models import File, Folder, Project
from synapseclient.models.activity import UsedURL

BOGUS_URL = "https://www.synapse.org/"


def _write_manifest(rows: list[dict], tmp_path: Path) -> Path:
    """Write a minimal CSV manifest to a unique path under *tmp_path*.

    Returns:
        Path to the written manifest file.
    """
    path = tmp_path / f"{uuid.uuid4()}_manifest.csv"
    if not rows:
        return path
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, restval="", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


def _create_local_test_file(content: str, tmp_path: Path) -> Path:
    """Write content to a unique file under *tmp_path*.

    Returns:
        Path to the written file.
    """
    path = tmp_path / f"{uuid.uuid4()}_local_test_file.txt"
    path.write_text(content, encoding="utf-8")
    return path


class TestSyncToSynapse:
    """Integration tests for Project.sync_to_synapse / Folder.sync_to_synapse.

    Tests:
        - Upload new files from a CSV manifest
        - Annotation columns in the manifest are stored as file annotations
        - Updating an existing file by ID creates a new version
        - Provenance (used/executed) columns are recorded as activity
        - dry_run=True validates without uploading
        - Files can target a subfolder as parentId
        - A non-container parentId (e.g. a File) raises ValueError
        - Rows with a non-empty error column are skipped
    """

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def _create_test_file(self, project: Project, **kwargs) -> File:
        """Upload a small test file to Synapse and return the File model."""
        path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(path)
        file = File(
            parent_id=project.id,
            path=path,
            name=f"test_file_{uuid.uuid4()}",
            **kwargs,
        )
        await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        return file

    async def test_upload_new_files_from_manifest(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """Files listed in the manifest that don't yet exist in Synapse are created."""
        # GIVEN two local files and a manifest that points them at the project
        file_a = _create_local_test_file("content of file A", tmp_path)
        name_a = file_a.name
        file_b = _create_local_test_file("content of file B", tmp_path)
        name_b = file_b.name

        manifest_path = _write_manifest(
            [
                {
                    "path": str(file_a),
                    "parentId": project_model.id,
                    "name": name_a,
                },
                {
                    "path": str(file_b),
                    "parentId": project_model.id,
                    "name": name_b,
                },
            ],
            tmp_path,
        )

        # WHEN I sync to Synapse
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        for f in uploaded_files:
            self.schedule_for_cleanup(f.id)

        # THEN both files are returned and exist in Synapse
        assert len(uploaded_files) == 2
        uploaded_names = {f.name for f in uploaded_files}
        assert name_a in uploaded_names
        assert name_b in uploaded_names

        for f in uploaded_files:
            file_entity = await File(id=f.id).get_async(synapse_client=self.syn)
            assert file_entity.id is not None
            assert file_entity.parent_id == project_model.id

    async def test_annotations_written_to_synapse(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """Annotation columns in the manifest are stored as file annotations."""
        # GIVEN a local file with annotation columns in the manifest
        local_file = _create_local_test_file("annotated content", tmp_path)

        unique_name = f"annotated_{uuid.uuid4()}.txt"
        manifest_path = _write_manifest(
            [
                {
                    "path": str(local_file),
                    "parentId": project_model.id,
                    "name": unique_name,
                    "my_string": "hello",
                    "my_number": "42",
                },
            ],
            tmp_path,
        )

        # WHEN I sync to Synapse
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        for f in uploaded_files:
            self.schedule_for_cleanup(f.id)

        # THEN the file is returned and exists in Synapse
        # AND the file exists with the correct annotations
        assert len(uploaded_files) == 1
        uploaded_names = {f.name for f in uploaded_files}
        assert unique_name in uploaded_names

        for f in uploaded_files:
            file_entity = await File(id=f.id).get_async(synapse_client=self.syn)
            assert file_entity.id is not None
            assert file_entity.parent_id == project_model.id
            assert file_entity.annotations.get("my_string") == ["hello"]
            # "42" in the CSV is parsed by ast.literal_eval into the integer 42
            assert file_entity.annotations.get("my_number") == [42]

    async def test_update_existing_file_creates_new_version(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """Referencing an existing file by ID in the manifest creates a new version."""
        # GIVEN a file already in Synapse
        stored_file = await self._create_test_file(
            project_model, annotations={"status": "original"}
        )
        assert stored_file.version_number == 1

        # WHEN I update the local file content and change the annotation in the manifest
        updated_path = _create_local_test_file("updated content — v2", tmp_path)

        manifest_path = _write_manifest(
            [
                {
                    "path": str(updated_path),
                    "parentId": project_model.id,
                    "ID": stored_file.id,
                    "name": stored_file.name,
                    "status": "updated",
                },
            ],
            tmp_path,
        )

        await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN a new version exists with the updated annotation
        refreshed = await File(id=stored_file.id).get_async(synapse_client=self.syn)
        assert refreshed.version_number == 2
        assert refreshed.annotations.get("status") == ["updated"]

    async def test_provenance_recorded_from_manifest(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """used and executed columns in the manifest are stored as provenance."""
        # GIVEN a local file with provenance entries in the manifest
        local_file = _create_local_test_file("data", tmp_path)

        unique_name = f"prov_{uuid.uuid4()}.txt"
        manifest_path = _write_manifest(
            [
                {
                    "path": str(local_file),
                    "parentId": project_model.id,
                    "name": unique_name,
                    "used": BOGUS_URL,
                    "executed": BOGUS_URL,
                    "activityName": "my_activity",
                    "activityDescription": "test provenance",
                },
            ],
            tmp_path,
        )

        # WHEN I sync to Synapse
        await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN the file has activity with the expected provenance
        synced = await Project(id=project_model.id).sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )
        uploaded = next((f for f in synced.files if f.name == unique_name), None)
        assert uploaded is not None
        self.schedule_for_cleanup(uploaded.id)

        refreshed = await File(id=uploaded.id).get_async(
            include_activity=True, synapse_client=self.syn
        )
        assert refreshed.activity is not None
        assert refreshed.activity.name == "my_activity"
        assert refreshed.activity.description == "test provenance"
        used_urls = [u.url for u in refreshed.activity.used if isinstance(u, UsedURL)]
        assert BOGUS_URL in used_urls

    async def test_dry_run_does_not_upload(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """dry_run=True validates the manifest but does not create any entities."""
        # GIVEN a unique file name we can look for after the dry run
        local_file = _create_local_test_file("should not appear in Synapse", tmp_path)
        unique_name = f"dry_run_{uuid.uuid4()}.txt"

        manifest_path = _write_manifest(
            [
                {
                    "path": str(local_file),
                    "parentId": project_model.id,
                    "name": unique_name,
                },
            ],
            tmp_path,
        )

        # WHEN I sync with dry_run=True
        await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            dry_run=True,
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN no file with that name should exist in the project
        synced = await Project(id=project_model.id).sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )
        names = {f.name for f in synced.files}
        assert unique_name not in names

    async def test_upload_into_subfolder(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """Files can be targeted at a Folder (not just the Project root)."""
        # GIVEN a folder in the project
        folder = await Folder(
            name=f"sub_{uuid.uuid4()}", parent_id=project_model.id
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        local_file = _create_local_test_file("goes into the folder", tmp_path)

        manifest_path = _write_manifest(
            [
                {
                    "path": str(local_file),
                    "parentId": folder.id,
                    "name": "in_folder.txt",
                },
            ],
            tmp_path,
        )

        # WHEN I sync to Synapse
        await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN the file exists inside the folder, not at the project root
        synced_folder = await Folder(id=folder.id).sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )
        names = {f.name for f in synced_folder.files}
        assert "in_folder.txt" in names
        for f in synced_folder.files:
            self.schedule_for_cleanup(f.id)

    async def test_non_container_parent_id_raises(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """A parentId pointing at a File (not a container) raises during validation."""
        # GIVEN a File entity stored in Synapse (not a container)
        file_entity = await self._create_test_file(project_model)

        # AND a manifest that points another file at that File entity as parent
        upload_file = _create_local_test_file("upload", tmp_path)

        manifest_path = _write_manifest(
            [
                {
                    "path": str(upload_file),
                    "parentId": file_entity.id,
                    "name": "upload.txt",
                }
            ],
            tmp_path,
        )

        # THEN sync_to_synapse should raise because the parent is not a container
        with pytest.raises(ValueError, match="not a Folder or Project"):
            await project_model.sync_to_synapse_async(
                manifest_path=str(manifest_path),
                send_messages=False,
                synapse_client=self.syn,
            )

    async def test_error_column_rows_skipped(
        self, project_model: Project, tmp_path: Path
    ) -> None:
        """Rows with a non-empty 'error' column (from get-download-list) are ignored."""
        # GIVEN a manifest where one row has an error and one is valid
        good_file = _create_local_test_file("good content", tmp_path)
        unique_name = f"error_skip_{uuid.uuid4()}.txt"

        manifest_path = _write_manifest(
            [
                # Row with an error — should be skipped
                {
                    "path": "/nonexistent/file.txt",
                    "parentId": project_model.id,
                    "name": "should_not_appear.txt",
                    "error": "Download failed",
                },
                # Valid row — should be uploaded
                {
                    "path": str(good_file),
                    "parentId": project_model.id,
                    "name": unique_name,
                    "error": "",
                },
            ],
            tmp_path,
        )

        # WHEN I sync to Synapse
        await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN only the valid row was uploaded
        synced = await Project(id=project_model.id).sync_from_synapse_async(
            recursive=False, download_file=False, synapse_client=self.syn
        )
        names = {f.name for f in synced.files}
        assert unique_name in names
        assert "should_not_appear.txt" not in names

        for f in synced.files:
            if f.name == unique_name:
                self.schedule_for_cleanup(f.id)
