"""Integration tests for StorableContainer"""

import csv
import os
import platform
import uuid
from pathlib import Path
from typing import Callable

import pandas as pd
import pytest
import pytest_asyncio

import synapseclient.core.utils as utils
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
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
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        for f in uploaded_files:
            self.schedule_for_cleanup(f.id)

        # THEN the file is returned with the expected provenance
        assert len(uploaded_files) == 1
        uploaded = uploaded_files[0]
        assert uploaded.name == unique_name

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
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            dry_run=True,
            send_messages=False,
            synapse_client=self.syn,
        )

        # THEN no files were uploaded
        assert uploaded_files == []

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
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        for f in uploaded_files:
            self.schedule_for_cleanup(f.id)

        # THEN the file exists inside the folder, not at the project root
        assert len(uploaded_files) == 1
        assert uploaded_files[0].name == "in_folder.txt"

        file_entity = await File(id=uploaded_files[0].id).get_async(
            synapse_client=self.syn
        )
        assert file_entity.parent_id == folder.id

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
        uploaded_files = await project_model.sync_to_synapse_async(
            manifest_path=str(manifest_path),
            send_messages=False,
            synapse_client=self.syn,
        )

        for f in uploaded_files:
            self.schedule_for_cleanup(f.id)

        # THEN only the valid row was uploaded
        assert len(uploaded_files) == 1
        assert uploaded_files[0].name == unique_name


class TestGenerateSyncManifest:
    """Integration tests for StorableContainer.generate_sync_manifest_async
    against the live Synapse API.

    These tests walk a local temporary directory and verify that the method
    creates matching Synapse folders under a real container and writes a
    manifest CSV that references the newly-created folder IDs.
    """

    @pytest_asyncio.fixture(loop_scope="session", scope="function")
    async def scope_folder(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
        request: pytest.FixtureRequest,
    ) -> Folder:
        """A fresh Folder under the worker-scoped project per test, so
        assertions can reference the folder's full child state without
        interference from sibling tests. A Folder is cheaper to create than
        a Project while providing equivalent isolation for these tests.
        """
        folder = await Folder(
            name=f"{request.node.name}_{uuid.uuid4()}",
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder)
        return folder

    async def test_flat_directory_uses_parent_id(
        self,
        syn: Synapse,
        scope_folder: Folder,
        tmp_path: Path,
    ) -> None:
        """Flat directories should produce a manifest where every file points
        directly at the container's id, and empty files should be skipped.
        No Synapse folders should be created when there are no subdirectories
        to mirror.
        """
        # GIVEN a flat directory of non-empty files plus one empty file
        src = tmp_path / "flat"
        src.mkdir()
        (src / "a.txt").write_text("alpha")
        (src / "b.txt").write_text("bravo")
        (src / "empty.txt").write_text("")
        manifest = tmp_path / "manifest.csv"

        # WHEN I generate a sync manifest on the scope folder
        await scope_folder.generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        # THEN the manifest only contains the non-empty files, all pointing
        # at the scope folder as their parent, and paths are absolute
        df = pd.read_csv(manifest)
        assert list(df.columns) == ["path", "parentId"]
        assert sorted(os.path.basename(p) for p in df["path"]) == ["a.txt", "b.txt"]
        assert (df["parentId"] == scope_folder.id).all()
        for path in df["path"]:
            assert os.path.isabs(path)

        # AND no folders were created under the scope folder
        await scope_folder.sync_from_synapse_async(
            download_file=False, recursive=False, synapse_client=syn
        )
        assert scope_folder.folders == []

    async def test_nested_directory_creates_folders(
        self,
        syn: Synapse,
        scope_folder: Folder,
        tmp_path: Path,
    ) -> None:
        """Nested directory trees should create matching Synapse folders at
        each level, and the manifest parentId for each file should be the ID
        of the Synapse folder corresponding to the file's on-disk directory.
        """
        # GIVEN a nested directory tree with sibling folders at the root and a
        # deeper leaf folder
        src = tmp_path / "root"
        sibling_a = src / "sibling_a"
        sibling_b = src / "sibling_b"
        deep = sibling_a / "deep"
        deep.mkdir(parents=True)
        sibling_b.mkdir()

        (src / "root.txt").write_text("at root")
        (sibling_a / "a.txt").write_text("sibling a")
        (sibling_b / "b.txt").write_text("sibling b")
        (deep / "deep.txt").write_text("deep file")
        manifest = tmp_path / "manifest.csv"

        # WHEN I generate a sync manifest on the scope folder
        await scope_folder.generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        # THEN each manifest row's parentId identifies the Synapse folder that
        # contains the file on disk
        df = pd.read_csv(manifest)
        by_basename = {
            os.path.basename(p): pid for p, pid in zip(df["path"], df["parentId"])
        }
        assert by_basename["root.txt"] == scope_folder.id
        sibling_a_id = by_basename["a.txt"]
        sibling_b_id = by_basename["b.txt"]
        deep_id = by_basename["deep.txt"]

        for path in df["path"]:
            assert os.path.isabs(path)

        # AND the Synapse tree matches the local layout
        await scope_folder.sync_from_synapse_async(
            download_file=False, recursive=True, synapse_client=syn
        )
        top_level = {f.name: f for f in scope_folder.folders}
        assert sorted(top_level) == ["sibling_a", "sibling_b"]
        assert top_level["sibling_a"].id == sibling_a_id
        assert top_level["sibling_b"].id == sibling_b_id
        assert [(f.name, f.id) for f in top_level["sibling_a"].folders] == [
            ("deep", deep_id)
        ]
        assert top_level["sibling_b"].folders == []

    async def test_existing_folders_are_reused(
        self,
        syn: Synapse,
        scope_folder: Folder,
        tmp_path: Path,
    ) -> None:
        """When a Synapse folder with a matching name already exists under
        the container, the method should reuse its ID instead of creating a
        new folder or raising a conflict.
        """
        # GIVEN a folder that already exists in Synapse under the scope folder
        folder_name = "preexisting"
        existing = await Folder(
            name=folder_name, parent_id=scope_folder.id
        ).store_async(synapse_client=syn)

        # AND a local directory that mirrors that folder's name with a file inside
        src = tmp_path / "root"
        child = src / folder_name
        child.mkdir(parents=True)
        (child / "payload.txt").write_text("payload")
        manifest = tmp_path / "manifest.csv"

        # WHEN I generate a sync manifest
        await scope_folder.generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        # THEN the manifest reuses the existing folder's Synapse ID
        df = pd.read_csv(manifest)
        assert len(df) == 1
        assert df["parentId"].iloc[0] == existing.id

        # AND the scope folder has exactly the one pre-existing child folder
        await scope_folder.sync_from_synapse_async(
            download_file=False, recursive=False, synapse_client=syn
        )
        assert len(scope_folder.folders) == 1
        assert scope_folder.folders[0].id == existing.id

    async def test_empty_directory_writes_header_only(
        self, syn: Synapse, scope_folder: Folder, tmp_path: Path
    ) -> None:
        """An empty source directory should produce a manifest containing
        only the CSV header row."""
        empty = tmp_path / "empty"
        empty.mkdir()
        manifest = tmp_path / "manifest.csv"

        await scope_folder.generate_sync_manifest_async(
            directory_path=str(empty),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        assert manifest.read_text().strip().splitlines() == ["path,parentId"]

    async def test_missing_directory_raises(
        self, syn: Synapse, project_model: Project, tmp_path: Path
    ) -> None:
        """A directory_path that does not exist on disk should raise
        ValueError before any manifest file is written."""
        missing = tmp_path / "does_not_exist"
        manifest = tmp_path / "manifest.csv"

        with pytest.raises(ValueError, match="is not a directory or does not exist"):
            await project_model.generate_sync_manifest_async(
                directory_path=str(missing),
                manifest_path=str(manifest),
                synapse_client=syn,
            )
        assert not manifest.exists()

    async def test_invalid_parent_id_raises_http_error(
        self, syn: Synapse, tmp_path: Path
    ) -> None:
        """A container whose id does not resolve to any Synapse entity should
        surface as a SynapseHTTPError from the server, and no manifest file
        should be written.
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        with pytest.raises(SynapseHTTPError):
            await Folder(id="syn00000000").generate_sync_manifest_async(
                directory_path=str(src),
                manifest_path=str(manifest),
                synapse_client=syn,
            )
        assert not manifest.exists()

    async def test_output_is_sorted_deterministically(
        self,
        syn: Synapse,
        scope_folder: Folder,
        tmp_path: Path,
    ) -> None:
        """Directories and files should be traversed in sorted order so the
        generated manifest is deterministic regardless of filesystem
        iteration order.
        """
        # GIVEN subdirs and files created in non-alphabetical order
        src = tmp_path / "root"
        for dirname in ["charlie", "alpha", "bravo"]:
            subdir = src / dirname
            subdir.mkdir(parents=True)
            for filename in ["z.txt", "a.txt", "m.txt"]:
                (subdir / filename).write_text("payload")
        manifest = tmp_path / "manifest.csv"

        await scope_folder.generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        # THEN manifest rows are sorted first by directory name, then by file
        # name within each directory
        df = pd.read_csv(manifest)
        ordered = [
            (os.path.basename(os.path.dirname(p)), os.path.basename(p))
            for p in df["path"]
        ]
        assert ordered == [
            ("alpha", "a.txt"),
            ("alpha", "m.txt"),
            ("alpha", "z.txt"),
            ("bravo", "a.txt"),
            ("bravo", "m.txt"),
            ("bravo", "z.txt"),
            ("charlie", "a.txt"),
            ("charlie", "m.txt"),
            ("charlie", "z.txt"),
        ]

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Creating symlinks on Windows requires elevated privileges.",
    )
    async def test_directory_symlinks_are_not_followed(
        self,
        syn: Synapse,
        scope_folder: Folder,
        tmp_path: Path,
    ) -> None:
        """Symlinks to directories should not be recursed into — files under
        a symlinked directory must not appear in the generated manifest, and
        no Synapse folder should be created for the symlinked directory.
        """
        # GIVEN a tree where a sibling directory is a symlink to a real
        # directory that contains a file
        src = tmp_path / "root"
        real = src / "real"
        real.mkdir(parents=True)
        (real / "real_file.txt").write_text("real")
        os.symlink(real, src / "link", target_is_directory=True)
        manifest = tmp_path / "manifest.csv"

        await scope_folder.generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=syn,
        )

        # THEN only the file in the real directory appears in the manifest
        df = pd.read_csv(manifest)
        parents_and_names = [
            (os.path.basename(os.path.dirname(p)), os.path.basename(p))
            for p in df["path"]
        ]
        assert parents_and_names == [("real", "real_file.txt")]

        # AND no Synapse folder is created for the symlinked directory:
        # only the on-disk "real" sibling is mirrored; the "link" symlink
        # was pruned during the walk and produced no Synapse folder.
        await scope_folder.sync_from_synapse_async(
            download_file=False, recursive=False, synapse_client=syn
        )
        assert sorted(f.name for f in scope_folder.folders) == ["real"]
