"""Unit tests for StorableContainer"""

import csv
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.models import Project


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
