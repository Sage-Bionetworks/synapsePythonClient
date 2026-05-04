"""Unit tests for StorableContainer"""

import csv
import os
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.models import Folder, Project


def _write_manifest(rows: list[dict], tmp_path: Path) -> Path:
    """Write a minimal CSV manifest to a unique path under *tmp_path*."""
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


class TestSyncToSynapse:
    """Tests for StorableContainer.sync_to_synapse_async that do not require a Synapse connection."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_missing_path_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'path' column raises ValueError immediately."""
        manifest_path = _write_manifest([{"parentId": "syn123", "name": "x"}], tmp_path)
        project = Project(id="test", name="test")

        with pytest.raises(ValueError, match="'path'"):
            await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )

    async def test_missing_parent_id_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'parentId' column raises ValueError immediately."""
        local_file = tmp_path / "f.txt"
        local_file.write_text("x")

        manifest_path = _write_manifest(
            [{"path": str(local_file), "name": "f.txt"}], tmp_path
        )
        project = Project(id="test", name="test")

        with pytest.raises(ValueError, match="'parentId'"):
            await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )

    async def test_all_rows_have_errors_no_crash(self, tmp_path: Path) -> None:
        """When every row has an error the call completes without uploading anything."""
        manifest_path = _write_manifest(
            [
                {
                    "path": "/nonexistent/file.txt",
                    "parentId": "syn123",
                    "name": "ignored.txt",
                    "error": "Some failure",
                }
            ],
            tmp_path,
        )
        project = Project(id="test", name="test")

        with patch(
            "synapseclient.models.mixins.storable_container.upload_sync_files"
        ) as mock_upload:
            result = await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )
            mock_upload.assert_not_called()
            assert result == []


class TestGenerateSyncManifest:
    """Tests for StorableContainer.generate_sync_manifest_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.fixture(autouse=True)
    def stub_parent_check(self) -> Any:
        """Bypass the upfront parent_id validation so tests don't hit the network.
        Tests that exercise the validation itself patch this explicitly."""
        with patch(
            "synapseclient.models.services.manifest._validate_target_container_async",
            new=AsyncMock(return_value=None),
        ) as mock:
            yield mock

    async def test_unstored_container_raises(self, tmp_path: Path) -> None:
        """Calling the method on a container whose id is None is a caller bug
        and must surface as a ValueError rather than a confusing downstream
        error."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        with pytest.raises(ValueError, match="has not been stored in Synapse"):
            await Project(name="unstored").generate_sync_manifest_async(
                directory_path=str(src),
                manifest_path=str(manifest),
                synapse_client=self.syn,
            )

    async def test_empty_directory_writes_header_only(self, tmp_path: Path) -> None:
        """An empty directory produces a manifest with only the header row and
        logs a warning so the caller is not left wondering why no files uploaded."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        manifest = tmp_path / "manifest.csv"

        with patch.object(self.syn.logger, "warning") as mock_warning:
            await Folder(id="syn1").generate_sync_manifest_async(
                directory_path=str(empty_dir),
                manifest_path=str(manifest),
                synapse_client=self.syn,
            )

        assert manifest.read_text().strip().splitlines() == ["path,parentId"]
        assert any(
            "No uploadable files found" in call.args[0]
            for call in mock_warning.call_args_list
        )

    async def test_flat_directory_lists_files_skips_empty(self, tmp_path: Path) -> None:
        """Non-empty files each produce a manifest row sharing the container's
        id as parentId; zero-byte files are skipped."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        (src / "b.txt").write_text("world")
        (src / "empty.txt").write_text("")
        manifest = tmp_path / "manifest.csv"

        await Folder(id="syn42").generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=self.syn,
        )

        df = pd.read_csv(manifest)
        assert list(df.columns) == ["path", "parentId"]
        assert sorted(os.path.basename(p) for p in df["path"]) == ["a.txt", "b.txt"]
        assert (df["parentId"] == "syn42").all()

    async def test_nested_directories_thread_parent_ids(self, tmp_path: Path) -> None:
        """Files in nested directories get the Synapse ID of their innermost
        Synapse folder as parentId, and Folder.store_async is called once per
        subdirectory with the correct name/parent_id."""
        src = tmp_path / "src"
        (src / "level1" / "level2").mkdir(parents=True)
        (src / "level1" / "level2" / "deep.txt").write_text("content")
        manifest = tmp_path / "manifest.csv"

        folder_id_iter = iter(["syn100", "syn200"])
        observed: list[tuple[str, str]] = []

        async def fake_store_async(self: Any, *args: Any, **kwargs: Any) -> Any:
            observed.append((self.name, self.parent_id))
            self.id = next(folder_id_iter)
            return self

        with patch(
            "synapseclient.models.Folder.store_async",
            new=fake_store_async,
        ):
            await Folder(id="synROOT").generate_sync_manifest_async(
                directory_path=str(src),
                manifest_path=str(manifest),
                synapse_client=self.syn,
            )

        assert observed == [("level1", "synROOT"), ("level2", "syn100")]
        df = pd.read_csv(manifest)
        assert df["path"].tolist() == [
            os.path.join(os.path.realpath(str(src)), "level1", "level2", "deep.txt")
        ]
        assert df["parentId"].tolist() == ["syn200"]

    async def test_output_is_csv_not_tsv(self, tmp_path: Path) -> None:
        """Output uses comma delimiter (CSV), matching the new manifest reader.

        The legacy synapseutils.generate_sync_manifest wrote TSV with a
        'parent' column; the OOP sync_to_synapse expects CSV with 'parentId'.
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "file.txt").write_text("data")
        manifest = tmp_path / "manifest.csv"

        await Folder(id="syn1").generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=self.syn,
        )

        text = manifest.read_text()
        assert "\t" not in text
        assert text.startswith("path,parentId")

    async def test_missing_directory_raises(self, tmp_path: Path) -> None:
        """A directory_path that does not exist raises ValueError instead of
        silently writing an empty manifest."""
        missing = tmp_path / "does_not_exist"
        manifest = tmp_path / "manifest.csv"

        with pytest.raises(ValueError, match="is not a directory or does not exist"):
            await Folder(id="syn1").generate_sync_manifest_async(
                directory_path=str(missing),
                manifest_path=str(manifest),
                synapse_client=self.syn,
            )

        assert not manifest.exists()

    async def test_file_path_raises(self, tmp_path: Path) -> None:
        """A directory_path pointing at a file (not a directory) raises ValueError."""
        file_path = tmp_path / "a.txt"
        file_path.write_text("hello")
        manifest = tmp_path / "manifest.csv"

        with pytest.raises(ValueError, match="is not a directory or does not exist"):
            await Folder(id="syn1").generate_sync_manifest_async(
                directory_path=str(file_path),
                manifest_path=str(manifest),
                synapse_client=self.syn,
            )

    async def test_parent_id_validated_upfront(
        self, tmp_path: Path, stub_parent_check: AsyncMock
    ) -> None:
        """The container's id is validated via _validate_target_container_async
        before the directory is walked."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        await Folder(id="synROOT").generate_sync_manifest_async(
            directory_path=str(src),
            manifest_path=str(manifest),
            synapse_client=self.syn,
        )

        stub_parent_check.assert_awaited_once_with("synROOT", client=self.syn)

    async def test_invalid_parent_id_propagates_http_error(
        self, tmp_path: Path
    ) -> None:
        """If the upfront parent_id check raises SynapseHTTPError, no manifest
        is written and the error propagates to the caller."""
        from synapseclient.core.exceptions import SynapseHTTPError

        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        with patch(
            "synapseclient.models.services.manifest._validate_target_container_async",
            new=AsyncMock(side_effect=SynapseHTTPError("Not found")),
        ):
            with pytest.raises(SynapseHTTPError):
                await Folder(id="synBOGUS").generate_sync_manifest_async(
                    directory_path=str(src),
                    manifest_path=str(manifest),
                    synapse_client=self.syn,
                )

        assert not manifest.exists()
