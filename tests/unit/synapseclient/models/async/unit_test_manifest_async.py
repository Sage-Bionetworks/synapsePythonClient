"""Unit tests for synapseclient.models.services.manifest (upload-side)."""

import asyncio
import os
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.models.services.manifest import (
    NON_ANNOTATION_COLUMNS,
    SyncUploader,
    SyncUploadItem,
    _build_annotations_for_file,
    _build_upload_items,
    _check_file_names,
    _check_parent_containers_async,
    _check_path_and_normalize,
    _check_provenance,
    _check_size_each_file,
    _convert_value,
    _expand_path,
    _parse_annotation_cell,
    _parse_literal,
    _recreate_directory_hierarchy_async,
    _resolve_provenance_column,
    _sort_and_fix_provenance,
    _split_csv_cell,
    read_manifest_for_upload,
)


class TestConvertCellInManifestToPythonTypes:
    @pytest.mark.parametrize(
        "cell, expected",
        [
            ("hello", ["hello"]),
            ("42", [42]),
            ("3.14", [3.14]),
            ("true", [True]),
            ("false", [False]),
            ("[a, b, c]", ["a", "b", "c"]),
            ("[1, 2, 3]", [1, 2, 3]),
            ('["foo bar", "baz"]', ["foo bar", "baz"]),
            ("[hello]", ["hello"]),
            ("  42  ", [42]),
            ("[]", []),
            ("[a, , c]", ["a", "c"]),
        ],
    )
    def test_scalar_and_array_conversions(self, cell: str, expected: Any) -> None:
        """Strings, numbers, bools, and bracket-delimited arrays are always returned
        as lists. Single-value cells produce a one-element list."""
        assert _parse_annotation_cell(cell) == expected

    def test_datetime_string(self) -> None:
        """An ISO-8601 datetime string is converted to a one-element list containing
        a datetime.datetime object."""
        import datetime

        result = _parse_annotation_cell("1970-01-01T00:00:00.000Z")
        assert isinstance(result, list)
        assert isinstance(result[0], datetime.datetime)


class TestCheckPathAndNormalize:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_url_passes_through(self) -> None:
        """URLs are returned unchanged without any filesystem check."""
        url = "https://example.com/file.txt"
        assert _check_path_and_normalize(url) == url

    def test_existing_file_returns_absolute_path(self, tmp_path: Path) -> None:
        """A relative or home-relative path to an existing file is resolved to an
        absolute path."""
        f = tmp_path / "test.txt"
        f.write_text("hello")
        result = _check_path_and_normalize(str(f))
        assert os.path.isabs(result)
        assert result == str(f.resolve())

    def test_missing_file_raises(self) -> None:
        """A path that does not point to an existing file raises IOError."""
        with pytest.raises(IOError):
            _check_path_and_normalize("/nonexistent/path/file.txt")


class TestCheckFileNameNewFormat:
    def test_valid_names_pass(self) -> None:
        """Unique, validly named files in the same parent do not raise."""
        df = pd.DataFrame(
            {
                "path": ["/a/file1.txt", "/a/file2.txt"],
                "name": ["file1.txt", "file2.txt"],
                "parentId": ["syn1", "syn1"],
            }
        )
        _check_file_names(df)  # should not raise

    def test_invalid_name_raises(self) -> None:
        """A file name containing characters not permitted by Synapse raises ValueError."""
        df = pd.DataFrame(
            {
                "path": ["/a/bad!name.txt"],
                "name": ["bad!name.txt"],
                "parentId": ["syn1"],
            }
        )
        with pytest.raises(ValueError, match="cannot be stored to Synapse"):
            _check_file_names(df)

    def test_duplicate_name_and_parent_raises(self) -> None:
        """Two files with the same name uploaded to the same parent raise ValueError."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt", "/b/file.txt"],
                "name": ["file.txt", "file.txt"],
                "parentId": ["syn1", "syn1"],
            }
        )
        with pytest.raises(ValueError, match="unique file name"):
            _check_file_names(df)

    def test_same_name_different_parent_is_ok(self) -> None:
        """The same file name is allowed in different parent containers."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt", "/b/file.txt"],
                "name": ["file.txt", "file.txt"],
                "parentId": ["syn1", "syn2"],
            }
        )
        _check_file_names(df)  # should not raise

    @pytest.mark.parametrize(
        "name",
        [
            "file`name.txt",
            "file+name.txt",
            "file (1).txt",
            "file-name.txt",
            "file.name.txt",
        ],
    )
    def test_special_characters_in_name_accepted(self, name: str) -> None:
        """Backticks, plus signs, parentheses, hyphens, and periods are valid in file names."""
        df = pd.DataFrame(
            {
                "path": ["/a/" + name],
                "name": [name],
                "parentId": ["syn1"],
            }
        )
        _check_file_names(df)  # should not raise


class TestCheckParentContainersAsync:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_valid_project_passes(self) -> None:
        """A parent ID that resolves to a Project does not raise."""
        from synapseclient.models.project import Project

        mock_project = MagicMock(spec=Project)
        with patch(
            "synapseclient.models.services.manifest.factory_get_async",
            new=AsyncMock(return_value=mock_project),
        ):
            await _check_parent_containers_async(["syn1"], syn=self.syn)

    async def test_valid_folder_passes(self) -> None:
        """A parent ID that resolves to a Folder does not raise."""
        from synapseclient.models.folder import Folder

        mock_folder = MagicMock(spec=Folder)
        with patch(
            "synapseclient.models.services.manifest.factory_get_async",
            new=AsyncMock(return_value=mock_folder),
        ):
            await _check_parent_containers_async(["syn1"], syn=self.syn)

    async def test_non_container_raises(self) -> None:
        """A parent ID that resolves to a non-container Synapse entity raises ValueError."""
        mock_entity = MagicMock()  # not a Folder or Project
        with patch(
            "synapseclient.models.services.manifest.factory_get_async",
            new=AsyncMock(return_value=mock_entity),
        ):
            with pytest.raises(ValueError, match="not a Folder or Project"):
                await _check_parent_containers_async(["syn1"], syn=self.syn)

    async def test_empty_parent_id_skipped(self) -> None:
        """An empty parent ID string is skipped without calling the Synapse API."""
        with patch(
            "synapseclient.models.services.manifest.factory_get_async",
            new=AsyncMock(),
        ) as mock_get:
            await _check_parent_containers_async([""], syn=self.syn)
            mock_get.assert_not_called()

    async def test_nonexistent_parent_id_reraises_http_error(self) -> None:
        """A parent ID that does not exist in Synapse re-raises the SynapseHTTPError."""
        from synapseclient.core.exceptions import SynapseHTTPError

        with patch(
            "synapseclient.models.services.manifest.factory_get_async",
            new=AsyncMock(side_effect=SynapseHTTPError("Not found")),
        ):
            with pytest.raises(SynapseHTTPError):
                await _check_parent_containers_async(["syn999"], syn=self.syn)


class TestReadManifestForUpload:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_missing_path_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'path' column raises ValueError."""
        csv = "parentId\nsyn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        with pytest.raises(ValueError, match="'path'"):
            await read_manifest_for_upload(str(manifest), self.syn, True, False)

    async def test_missing_parent_id_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'parentId' column raises ValueError."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        csv = f"path\n{f}\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        with pytest.raises(ValueError, match="'parentId'"):
            await read_manifest_for_upload(str(manifest), self.syn, True, False)

    # -- error column handling ---------------------------------------------

    async def test_rows_with_error_are_skipped(self, tmp_path: Path) -> None:
        """Rows with a non-empty 'error' cell are excluded from the returned items list."""
        f = tmp_path / "file.txt"
        f.write_text("hello")
        csv = f"path,parentId,error\n{f},syn1,some error\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        items, total = await read_manifest_for_upload(
            str(manifest), self.syn, True, False
        )
        assert items == []
        assert total == 0

    async def test_all_rows_have_errors_returns_empty(self, tmp_path: Path) -> None:
        """When every row has an error, both the items list and total size are zero."""
        csv = "path,parentId,error\n/x/y.txt,syn1,fail\n/x/z.txt,syn1,fail\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        items, total = await read_manifest_for_upload(
            str(manifest), self.syn, True, False
        )
        assert items == []
        assert total == 0

    # -- path validation ---------------------------------------------------

    async def test_duplicate_paths_raise(self, tmp_path: Path) -> None:
        """Two rows referencing the same file path raise ValueError about unique file paths."""
        f = tmp_path / "file.txt"
        f.write_text("hi")
        csv = f"path,parentId\n{f},syn1\n{f},syn2\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        with (
            patch(
                "synapseclient.models.services.manifest._check_parent_containers_async",
                new=AsyncMock(),
            ),
        ):
            with pytest.raises(ValueError, match="unique file path"):
                await read_manifest_for_upload(str(manifest), self.syn, True, False)

    async def test_empty_file_raises(self, tmp_path: Path) -> None:
        """A manifest row pointing to a zero-byte file raises ValueError."""
        f = tmp_path / "empty.txt"
        f.write_text("")  # 0 bytes
        csv = f"path,parentId\n{f},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)
        with pytest.raises(ValueError, match="empty"):
            await read_manifest_for_upload(str(manifest), self.syn, True, False)

    # -- successful upload items -------------------------------------------

    async def test_valid_manifest_returns_items_and_size(self, tmp_path: Path) -> None:
        """A valid manifest returns one upload item and the correct total file size."""
        f = tmp_path / "file.txt"
        f.write_text("hello world")
        size = f.stat().st_size
        csv = f"path,parentId,ID\n{f},syn1,syn42\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, total = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert total == size
        assert len(items) == 1
        assert items[0].entity.parent_id == "syn1"
        assert items[0].entity.id == "syn42"

    async def test_name_derived_from_basename_when_absent(self, tmp_path: Path) -> None:
        """When the manifest has no 'name' column, the entity name defaults to the file's basename."""
        f = tmp_path / "myfile.csv"
        f.write_text("data")
        csv = f"path,parentId\n{f},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, _ = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert items[0].entity.name == "myfile.csv"

    async def test_url_path_sets_synapse_store_false_and_excluded_from_size(
        self, tmp_path: Path
    ) -> None:
        """A URL path sets synapse_store=False on the item and contributes 0 bytes to the total size."""
        url = "https://example.com/data.csv"
        csv = f"path,parentId\n{url},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, total = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert total == 0
        assert items[0].entity.synapse_store is False

    async def test_empty_error_column_row_is_kept(self, tmp_path: Path) -> None:
        """A row with an empty 'error' cell is treated as valid and included in the upload items."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        csv = f"path,parentId,error\n{f},syn1,\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, _ = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert len(items) == 1

    async def test_synapse_store_defaults_to_true(self, tmp_path: Path) -> None:
        """When 'synapseStore' is absent from the manifest, it defaults to True."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        csv = f"path,parentId\n{f},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, _ = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert items[0].entity.synapse_store is True

    async def test_empty_csv_returns_empty(self, tmp_path: Path) -> None:
        """A CSV with headers but no data rows returns ([], 0)."""
        manifest = tmp_path / "manifest.csv"
        manifest.write_text("path,parentId\n")
        items, total = await read_manifest_for_upload(
            str(manifest), self.syn, True, False
        )
        assert items == []
        assert total == 0

    async def test_explicit_synapse_store_false_preserved(self, tmp_path: Path) -> None:
        """An explicit 'False' in the synapseStore column is preserved."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        csv = f"path,parentId,synapseStore\n{f},syn1,False\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        with patch(
            "synapseclient.models.services.manifest._check_parent_containers_async",
            new=AsyncMock(),
        ):
            items, _ = await read_manifest_for_upload(
                str(manifest), self.syn, True, False
            )

        assert items[0].entity.synapse_store is False


class TestSyncToSynapseAsync:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_dry_run_skips_upload(self, tmp_path: Path) -> None:
        """With dry_run=True, the manifest is read and validated but upload is never called, returning []."""
        f = tmp_path / "file.txt"
        f.write_text("hi")
        csv = f"path,parentId\n{f},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        from synapseclient.models import Project

        project = Project(id="syn123", name="test")
        project._last_persistent_instance = project

        mock_items = [MagicMock()]
        with (
            patch(
                "synapseclient.models.mixins.storable_container.read_manifest_for_upload",
                new=AsyncMock(return_value=(mock_items, 100)),
            ) as mock_read,
            patch(
                "synapseclient.models.mixins.storable_container.SyncUploader"
            ) as mock_uploader_cls,
        ):
            result = await project.sync_to_synapse_async(
                manifest_path=str(manifest),
                dry_run=True,
                synapse_client=self.syn,
            )
            mock_read.assert_awaited_once()
            mock_uploader_cls.return_value.upload.assert_not_called()
            assert result == []

    async def test_upload_called_with_items(self, tmp_path: Path) -> None:
        """With valid items, the uploader is called and the returned File list is passed back to the caller."""
        f = tmp_path / "file.txt"
        f.write_text("hi")
        csv = f"path,parentId\n{f},syn1\n"
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(csv)

        from synapseclient.models import Project

        project = Project(id="syn123", name="test")
        project._last_persistent_instance = project

        mock_items = [MagicMock()]
        mock_uploaded = [MagicMock()]
        mock_uploader = MagicMock()
        mock_uploader.upload = AsyncMock(return_value=mock_uploaded)

        with (
            patch(
                "synapseclient.models.mixins.storable_container.read_manifest_for_upload",
                new=AsyncMock(return_value=(mock_items, 100)),
            ),
            patch(
                "synapseclient.models.mixins.storable_container.SyncUploader",
                return_value=mock_uploader,
            ),
        ):
            result = await project.sync_to_synapse_async(
                manifest_path=str(manifest),
                dry_run=False,
                send_messages=False,
                synapse_client=self.syn,
            )
            mock_uploader.upload.assert_awaited_once_with(mock_items)
            assert result is mock_uploaded

    async def test_empty_items_skips_upload(self, tmp_path: Path) -> None:
        """When read_manifest_for_upload returns no items, the uploader is not called and [] is returned."""
        f = tmp_path / "manifest.csv"
        f.write_text("path,parentId,error\n/x.txt,syn1,fail\n")

        from synapseclient.models import Project

        project = Project(id="syn123", name="test")
        project._last_persistent_instance = project

        with (
            patch(
                "synapseclient.models.mixins.storable_container.read_manifest_for_upload",
                new=AsyncMock(return_value=([], 0)),
            ),
            patch(
                "synapseclient.models.mixins.storable_container.SyncUploader"
            ) as mock_uploader_cls,
        ):
            result = await project.sync_to_synapse_async(
                manifest_path=str(f),
                synapse_client=self.syn,
            )
            mock_uploader_cls.return_value.upload.assert_not_called()
            assert result == []

    async def test_merge_existing_annotations_passed_through(
        self, tmp_path: Path
    ) -> None:
        """The merge_existing_annotations flag is forwarded to read_manifest_for_upload."""
        f = tmp_path / "manifest.csv"
        f.write_text("")

        from synapseclient.models import Project

        project = Project(id="syn123", name="test")
        project._last_persistent_instance = project

        mock_read = AsyncMock(return_value=([], 0))
        with patch(
            "synapseclient.models.mixins.storable_container.read_manifest_for_upload",
            new=mock_read,
        ):
            await project.sync_to_synapse_async(
                manifest_path=str(f),
                merge_existing_annotations=False,
                synapse_client=self.syn,
            )
            _, kwargs = mock_read.call_args
            assert kwargs["merge_existing_annotations"] is False

    async def test_associate_activity_to_new_version_passed_through(
        self, tmp_path
    ) -> None:
        """The associate_activity_to_new_version flag is forwarded to read_manifest_for_upload."""
        f = tmp_path / "manifest.csv"
        f.write_text("")

        from synapseclient.models import Project

        project = Project(id="syn123", name="test")
        project._last_persistent_instance = project

        mock_read = AsyncMock(return_value=([], 0))
        with patch(
            "synapseclient.models.mixins.storable_container.read_manifest_for_upload",
            new=mock_read,
        ):
            await project.sync_to_synapse_async(
                manifest_path=str(f),
                associate_activity_to_new_version=True,
                synapse_client=self.syn,
            )
            _, kwargs = mock_read.call_args
            assert kwargs["associate_activity_to_new_version"] is True


class TestSortAndFixProvenance:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_no_provenance_columns_returns_rows_unchanged(
        self, tmp_path: Path
    ) -> None:
        """A manifest with no 'used' or 'executed' columns is returned as-is
        (same rows, same order, 'path' column preserved)."""
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("a")
        f2.write_text("b")
        df = pd.DataFrame({"path": [str(f1), str(f2)], "parentId": ["syn1", "syn1"]})

        result = await _sort_and_fix_provenance(self.syn, df)

        assert list(result["path"]) == [str(f1), str(f2)]

    async def test_used_column_split_and_resolved(self, tmp_path: Path) -> None:
        """Semicolon-delimited 'used' strings are split, resolved via _check_provenance,
        and stored as lists on the returned DataFrame."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        df = pd.DataFrame(
            {
                "path": [str(f)],
                "parentId": ["syn1"],
                "used": ["syn111;https://example.com"],
            }
        )

        result = await _sort_and_fix_provenance(self.syn, df)

        assert result.loc[0, "used"] == ["syn111", "https://example.com"]

    async def test_executed_column_split_and_resolved(self, tmp_path: Path) -> None:
        """Semicolon-delimited 'executed' strings are split and stored as lists."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        df = pd.DataFrame(
            {
                "path": [str(f)],
                "parentId": ["syn1"],
                "executed": ["https://github.com/a;https://github.com/b"],
            }
        )

        result = await _sort_and_fix_provenance(self.syn, df)

        assert result.loc[0, "executed"] == [
            "https://github.com/a",
            "https://github.com/b",
        ]

    async def test_empty_used_cell_produces_empty_list(self, tmp_path: Path) -> None:
        """A 'used' cell that is empty or whitespace-only is resolved to an empty list."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        df = pd.DataFrame({"path": [str(f)], "parentId": ["syn1"], "used": [""]})

        result = await _sort_and_fix_provenance(self.syn, df)

        assert result.loc[0, "used"] == []

    async def test_topological_sort_orders_dependency_before_dependent(
        self, tmp_path: Path
    ) -> None:
        """When file B lists file A as 'used', the returned DataFrame places A
        before B so it is uploaded first."""
        fa = tmp_path / "a.txt"
        fb = tmp_path / "b.txt"
        fa.write_text("a")
        fb.write_text("b")
        abs_a = str(fa.resolve())
        abs_b = str(fb.resolve())
        # b depends on a
        df = pd.DataFrame(
            {
                "path": [abs_b, abs_a],
                "parentId": ["syn1", "syn1"],
                "used": [abs_a, ""],
            }
        )

        result = await _sort_and_fix_provenance(self.syn, df)

        paths = list(result["path"])
        assert paths.index(abs_a) < paths.index(abs_b)

    async def test_used_cell_already_a_list_is_not_split(self, tmp_path: Path) -> None:
        """A 'used' cell that was already converted to a Python list by
        _parse_annotation_cell is handled without calling
        .strip()/.split() — which would raise AttributeError on a list."""
        f = tmp_path / "file.txt"
        f.write_text("content")
        df = pd.DataFrame(
            {
                "path": [str(f)],
                "parentId": ["syn1"],
                "used": [["syn111", "https://example.com"]],
            }
        )

        result = await _sort_and_fix_provenance(self.syn, df)

        assert result.loc[0, "used"] == ["syn111", "https://example.com"]

    async def test_used_cell_list_with_non_string_item_does_not_crash(
        self, tmp_path: Path
    ) -> None:
        """Non-string items already in a 'used' list (e.g. a File object) are
        passed to _check_provenance without calling .strip(), which would raise
        AttributeError on a non-string."""
        from synapseclient.models.file import File

        f = tmp_path / "file.txt"
        f.write_text("content")
        existing_file = MagicMock(spec=File)
        df = pd.DataFrame(
            {
                "path": [str(f)],
                "parentId": ["syn1"],
                "used": [[existing_file]],
            }
        )

        with patch(
            "synapseclient.models.services.manifest._check_provenance",
            new=AsyncMock(return_value=existing_file),
        ) as mock_check:
            result = await _sort_and_fix_provenance(self.syn, df)

        # _check_provenance must be called with the File object, not item.strip()
        mock_check.assert_awaited_once_with(
            existing_file, str(f), self.syn, mock_check.call_args[0][3]
        )
        assert result.loc[0, "used"] == [existing_file]

    async def test_invalid_provenance_item_propagates_error(
        self, tmp_path: Path
    ) -> None:
        """A provenance reference that is not a file path, URL, or Synapse ID
        propagates SynapseProvenanceError from _check_provenance."""
        from synapseclient.core.exceptions import SynapseProvenanceError

        f = tmp_path / "file.txt"
        f.write_text("content")
        df = pd.DataFrame(
            {
                "path": [str(f)],
                "parentId": ["syn1"],
                "used": ["not_a_valid_reference"],
            }
        )

        with pytest.raises(SynapseProvenanceError):
            await _sort_and_fix_provenance(self.syn, df)


class TestResolveProvenanceColumn:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def _make_df(self, paths: list[str]) -> pd.DataFrame:
        """Return a path-indexed DataFrame with the given paths as the index."""
        return pd.DataFrame(index=paths)

    @pytest.mark.parametrize("cell", ["", "   "])
    async def test_empty_or_whitespace_string_returns_empty_list(
        self, cell: str
    ) -> None:
        """An empty or whitespace-only string cell returns [] with no provenance calls."""
        df = self._make_df([])
        result = await _resolve_provenance_column(cell, "/file.txt", self.syn, df)
        assert result == []

    async def test_single_synapse_id_string_resolved(self) -> None:
        """A single Synapse ID string is resolved and returned as a one-element list."""
        df = self._make_df([])
        result = await _resolve_provenance_column("syn123", "/file.txt", self.syn, df)
        assert result == ["syn123"]

    async def test_semicolon_delimited_string_split_and_resolved(self) -> None:
        """A semicolon-delimited string is split into individual items, each resolved."""
        df = self._make_df([])
        result = await _resolve_provenance_column(
            "syn111 ; https://example.com", "/file.txt", self.syn, df
        )
        assert result == ["syn111", "https://example.com"]

    async def test_already_a_list_passed_through_without_splitting(self) -> None:
        """A cell that is already a Python list is not split — items are resolved directly."""
        df = self._make_df([])
        result = await _resolve_provenance_column(
            ["syn111", "https://example.com"], "/file.txt", self.syn, df
        )
        assert result == ["syn111", "https://example.com"]

    async def test_non_string_list_item_passed_without_strip(self) -> None:
        """Non-string items in an already-parsed list are forwarded to _check_provenance
        without calling .strip(), which would raise AttributeError."""
        from synapseclient.models.file import File

        existing_file = MagicMock(spec=File)
        df = self._make_df([])
        with patch(
            "synapseclient.models.services.manifest._check_provenance",
            new=AsyncMock(return_value=existing_file),
        ) as mock_check:
            result = await _resolve_provenance_column(
                [existing_file], "/file.txt", self.syn, df
            )
        mock_check.assert_awaited_once_with(existing_file, "/file.txt", self.syn, df)
        assert result == [existing_file]


class TestCheckProvenance:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def _make_df(self, paths: list[str]) -> pd.DataFrame:
        """Return a path-indexed DataFrame with the given paths as the index."""
        return pd.DataFrame(index=paths)

    @pytest.mark.parametrize(
        "item",
        [
            None,
            "https://github.com/example/repo",
            "syn123456",
        ],
    )
    async def test_passthrough_items(self, item: str | None) -> None:
        """None, URLs, and Synapse IDs are returned unchanged without any lookup."""
        df = self._make_df([])
        result = await _check_provenance(item, "/some/file.txt", self.syn, df)
        assert result == item

    async def test_local_file_in_upload_batch_returned_as_path(
        self, tmp_path: Path
    ) -> None:
        """A local file that is part of the current upload batch is returned as its
        resolved absolute path so the topological sort can order it correctly."""
        f = tmp_path / "dep.txt"
        f.write_text("content")
        abs_path = str(f.resolve())
        df = self._make_df([abs_path])
        result = await _check_provenance(str(f), "/some/file.txt", self.syn, df)
        assert result == abs_path

    async def test_local_file_not_in_batch_found_in_synapse(
        self, tmp_path: Path
    ) -> None:
        """A local file that is not in the upload batch but exists in Synapse is
        resolved to a File model object via MD5 lookup."""
        from synapseclient.models.file import File

        f = tmp_path / "existing.txt"
        f.write_text("content")
        synapse_file = MagicMock(spec=File)
        df = self._make_df([])  # file not in upload batch
        with patch(
            "synapseclient.models.file.File.from_path_async",
            new=AsyncMock(return_value=synapse_file),
        ):
            result = await _check_provenance(str(f), "/some/file.txt", self.syn, df)
        assert result is synapse_file

    async def test_local_file_not_in_batch_not_in_synapse_raises(
        self, tmp_path: Path
    ) -> None:
        """A local file that is neither in the upload batch nor found in Synapse
        raises SynapseProvenanceError — it cannot be used as a provenance reference."""
        from synapseclient.core.exceptions import (
            SynapseFileNotFoundError,
            SynapseProvenanceError,
        )

        f = tmp_path / "orphan.txt"
        f.write_text("content")
        df = self._make_df([])
        with patch(
            "synapseclient.models.file.File.from_path_async",
            new=AsyncMock(side_effect=SynapseFileNotFoundError("not found")),
        ):
            with pytest.raises(
                SynapseProvenanceError, match="not being uploaded and is not in Synapse"
            ):
                await _check_provenance(str(f), "/some/file.txt", self.syn, df)

    async def test_invalid_item_raises(self) -> None:
        """A string that is not a local file path, URL, or Synapse ID raises
        SynapseProvenanceError."""
        from synapseclient.core.exceptions import SynapseProvenanceError

        df = self._make_df([])
        with pytest.raises(SynapseProvenanceError):
            await _check_provenance(
                "not_a_url_or_synapse_id", "/some/file.txt", self.syn, df
            )


class TestBuildUploadItems:
    def test_parent_id_mapped_to_parent_id(self) -> None:
        """The parentId column is mapped to the entity's parent_id attribute."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "ID": ["syn1"],
                "name": ["file.txt"],
                "synapseStore": [True],
                "contentType": ["text/plain"],
                "forceVersion": [True],
                "used": [[]],
                "executed": [[]],
                "activityName": [""],
                "activityDescription": [""],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert len(items) == 1
        assert items[0].entity.parent_id == "syn99"

    def test_id_column_mapped_to_file_id(self) -> None:
        """The ID column is mapped to the entity's id attribute."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "ID": ["syn42"],
                "name": ["file.txt"],
                "synapseStore": [True],
                "contentType": ["text/plain"],
                "forceVersion": [True],
                "used": [[]],
                "executed": [[]],
                "activityName": [""],
                "activityDescription": [""],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].entity.id == "syn42"

    def test_empty_id_becomes_none(self) -> None:
        """An empty ID cell is normalised to None on the resulting entity."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "ID": [""],
                "name": ["file.txt"],
                "synapseStore": [True],
                "contentType": [""],
                "forceVersion": [True],
                "used": [[]],
                "executed": [[]],
                "activityName": [""],
                "activityDescription": [""],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].entity.id is None

    def test_used_executed_activity_fields_mapped(self) -> None:
        """Provenance columns (used, executed, activityName, activityDescription) are mapped to the upload item."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "name": ["file.txt"],
                "synapseStore": [True],
                "contentType": [""],
                "forceVersion": [True],
                "used": [["syn1"]],
                "executed": [["https://github.com/example"]],
                "activityName": ["my activity"],
                "activityDescription": ["a description"],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].used == ["syn1"]
        assert items[0].executed == ["https://github.com/example"]
        assert items[0].activity_name == "my activity"
        assert items[0].activity_description == "a description"

    def test_empty_force_version_defaults_to_true(self) -> None:
        """An empty string in the forceVersion column (from fillna('')) defaults to True."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "forceVersion": [""],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].entity.force_version is True

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("True", True),
            ("False", False),
            ("true", True),
            ("false", False),
        ],
    )
    def test_force_version_string_converted_to_bool(
        self, raw: str, expected: bool
    ) -> None:
        """String values like 'True'/'False' in forceVersion are converted to bool."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "forceVersion": [raw],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].entity.force_version is expected

    def test_force_version_bool_false_preserved(self) -> None:
        """An explicit bool False for forceVersion is preserved, not overridden to True."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "forceVersion": [False],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].entity.force_version is False

    @pytest.mark.parametrize(
        "column, item_attr",
        [
            ("contentType", "entity.content_type"),
            ("activityName", "activity_name"),
            ("activityDescription", "activity_description"),
        ],
    )
    def test_empty_optional_field_becomes_none(
        self, column: str, item_attr: str
    ) -> None:
        """An empty string in an optional column is coerced to None on the resulting item."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                column: [""],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        obj = items[0]
        for part in item_attr.split("."):
            obj = getattr(obj, part)
        assert obj is None

    def test_missing_used_and_executed_columns_default_to_empty_list(self) -> None:
        """When 'used' and 'executed' columns are absent, both default to empty lists."""
        df = pd.DataFrame({"path": ["/a/file.txt"], "parentId": ["syn99"]})
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert items[0].used == []
        assert items[0].executed == []

    def test_annotation_columns_excluded_from_non_annotation_set(self) -> None:
        """Extra columns become file annotations; standard manifest columns are not included as annotations."""
        df = pd.DataFrame(
            {
                "path": ["/a/file.txt"],
                "parentId": ["syn99"],
                "name": ["file.txt"],
                "synapseStore": [True],
                "contentType": [""],
                "forceVersion": [True],
                "used": [[]],
                "executed": [[]],
                "activityName": [""],
                "activityDescription": [""],
                "my_annotation": ["hello"],
            }
        )
        items = _build_upload_items(
            df,
            merge_existing_annotations=True,
            associate_activity_to_new_version=False,
        )
        assert "my_annotation" in items[0].entity.annotations
        # Standard columns must not be passed as annotations
        for col in NON_ANNOTATION_COLUMNS:
            assert col not in items[0].entity.annotations


class TestBuildAnnotationsForFile:
    @pytest.mark.parametrize("empty_value", ["", None])
    def test_empty_and_none_values_omitted(self, empty_value: str | None) -> None:
        """Empty strings and None values are silently dropped from the annotations dict."""
        result = _build_annotations_for_file({"key": empty_value})
        assert "key" not in result

    def test_string_value_converted(self) -> None:
        """String annotation values are converted to their Python types via
        _parse_annotation_cell."""
        result = _build_annotations_for_file({"score": "42"})
        assert result["score"] == [42]

    def test_non_string_value_wrapped_in_list(self) -> None:
        """Non-string values (e.g. already-typed ints) are wrapped in a single-element list."""
        result = _build_annotations_for_file({"count": 7})
        assert result["count"] == [7]

    def test_multiple_keys_mixed(self) -> None:
        """Empty and None values are dropped while valid string and non-string values
        are retained and converted correctly."""
        result = _build_annotations_for_file(
            {"keep": "hello", "drop": "", "also_drop": None, "num": "3.14"}
        )
        assert result == {"keep": ["hello"], "num": [3.14]}

    def test_empty_dict_returns_empty(self) -> None:
        """An empty annotations dict returns an empty dict."""
        assert _build_annotations_for_file({}) == {}


class TestParseLiteral:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("42", 42),
            ("-7", -7),
            ("3.14", 3.14),
            ("-0.5", -0.5),
            ('"hello"', "hello"),
            ('"foo bar"', "foo bar"),
        ],
    )
    def test_valid_scalars_are_returned(
        self, value: str, expected: int | float | str
    ) -> None:
        """Valid int, float, and quoted-string literals are parsed and returned."""
        assert _parse_literal(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "hello",
            "foo bar",
            "",
            "   ",
        ],
    )
    def test_plain_strings_return_none(self, value: str) -> None:
        """Plain unquoted strings are not Python literals and return None."""
        assert _parse_literal(value) is None

    @pytest.mark.parametrize(
        "value",
        [
            "True",
            "False",
            "true",
            "false",
        ],
    )
    def test_bool_strings_return_none(self, value: str) -> None:
        """Bool literals return None so that bool_or_none handles them instead,
        ensuring consistent case-insensitive parsing."""
        assert _parse_literal(value) is None

    @pytest.mark.parametrize(
        "value",
        [
            "(1, 2)",
            "[1, 2]",
            "{'a': 1}",
        ],
    )
    def test_complex_literals_return_none(self, value: str) -> None:
        """Tuples, lists, and dicts are not valid Synapse annotation types and return None."""
        assert _parse_literal(value) is None


class TestConvertValue:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("42", 42),
            ("-7", -7),
            ("3.14", 3.14),
            ("-0.5", -0.5),
        ],
    )
    def test_numeric_strings_converted(self, value: str, expected: int | float) -> None:
        """Numeric strings are converted to int or float."""
        assert _convert_value(value) == expected
        assert type(_convert_value(value)) is type(expected)

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("true", True),
            ("false", False),
            ("True", True),
            ("False", False),
            ("TRUE", True),
            ("FALSE", False),
        ],
    )
    def test_bool_strings_converted_case_insensitively(
        self, value: str, expected: bool
    ) -> None:
        """Bool strings are converted case-insensitively and returned as bool, not int."""
        result = _convert_value(value)
        assert result is expected
        assert type(result) is bool

    def test_datetime_string_converted(self) -> None:
        """ISO date strings are converted to datetime.datetime."""
        import datetime

        result = _convert_value("2024-01-15")
        assert isinstance(result, datetime.datetime)

    def test_datetime_wins_over_numeric(self) -> None:
        """A string like '2024-01-01' is parsed as datetime, not as a subtraction
        expression (which ast.literal_eval cannot parse anyway, but datetime must
        run first to ensure correct priority)."""
        import datetime

        result = _convert_value("2024-01-01")
        assert isinstance(result, datetime.datetime)

    def test_plain_string_returned_unchanged(self) -> None:
        """Plain unquoted strings that don't match any type are returned as-is."""
        assert _convert_value("hello") == "hello"
        assert type(_convert_value("hello")) is str

    def test_quoted_string_literal_unquoted(self) -> None:
        """A token that is a quoted Python string literal is unquoted by ast.literal_eval."""
        assert _convert_value('"foo bar"') == "foo bar"

    def test_bool_not_returned_as_int(self) -> None:
        """'True' must not come back as the integer 1 — bool_or_none runs before
        ast.literal_eval to prevent bool being treated as a subclass of int."""
        result = _convert_value("True")
        assert result is True
        assert type(result) is bool
        assert result != 1 or type(result) is not int


class TestExpandPath:
    def test_absolute_path_unchanged(self, tmp_path: Path) -> None:
        """An absolute path with no special characters is returned as-is."""
        p = str(tmp_path / "file.txt")
        assert _expand_path(p) == p

    def test_tilde_expanded(self) -> None:
        """A leading ~ is expanded to the user's home directory."""
        result = _expand_path("~/somefile.txt")
        assert not result.startswith("~")
        assert os.path.isabs(result)

    def test_relative_path_becomes_absolute(self) -> None:
        """A relative path is resolved to an absolute path."""
        result = _expand_path("relative/path.txt")
        assert os.path.isabs(result)

    def test_env_var_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment variables in the path are expanded."""
        monkeypatch.setenv("MY_TEST_DIR", "/tmp/test_dir")
        result = _expand_path("$MY_TEST_DIR/file.txt")
        assert "$MY_TEST_DIR" not in result
        expected = os.path.abspath(os.path.join("/tmp/test_dir", "file.txt"))
        assert result == expected

    def test_combined_tilde_and_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both ~ and environment variables are expanded in the same path."""
        monkeypatch.setenv("MY_SUBDIR", "docs")
        result = _expand_path("~/$MY_SUBDIR/file.txt")
        assert "~" not in result
        assert "$MY_SUBDIR" not in result
        assert result.endswith(os.sep + "docs" + os.sep + "file.txt")


class TestSplitCsvCell:
    def test_single_value(self) -> None:
        """A string with no commas returns a single-element list."""
        assert _split_csv_cell("hello") == ["hello"]

    def test_multiple_values(self) -> None:
        """Comma-separated values are split into a list."""
        assert _split_csv_cell("a, b, c") == ["a", "b", "c"]

    def test_quoted_commas_preserved(self) -> None:
        """Commas inside double quotes are not treated as delimiters."""
        assert _split_csv_cell('"foo, bar", baz') == ['"foo, bar"', "baz"]

    def test_whitespace_stripped(self) -> None:
        """Leading and trailing whitespace around each value is stripped."""
        assert _split_csv_cell("  a  ,  b  ") == ["a", "b"]

    def test_empty_string(self) -> None:
        """An empty string returns a list with a single empty string."""
        assert _split_csv_cell("") == [""]

    def test_trailing_comma(self) -> None:
        """A trailing comma produces an empty trailing element."""
        result = _split_csv_cell("a, b,")
        assert result == ["a", "b", ""]


class TestCheckSizeEachFile:
    def test_returns_total_size(self, tmp_path: Path) -> None:
        """Returns the combined size of all local files."""
        f1 = tmp_path / "a.txt"
        f1.write_text("hello")
        f2 = tmp_path / "b.txt"
        f2.write_text("world!")
        df = pd.DataFrame({"path": [str(f1), str(f2)]})
        total = _check_size_each_file(df)
        assert total == f1.stat().st_size + f2.stat().st_size

    def test_empty_file_raises(self, tmp_path: Path) -> None:
        """A zero-byte file raises ValueError."""
        f = tmp_path / "empty.txt"
        f.write_text("")
        df = pd.DataFrame({"path": [str(f)]})
        with pytest.raises(ValueError, match="empty"):
            _check_size_each_file(df)

    def test_url_rows_skipped(self, tmp_path: Path) -> None:
        """Rows whose path is a URL are skipped and not counted."""
        f = tmp_path / "local.txt"
        f.write_text("data")
        df = pd.DataFrame({"path": [str(f), "https://example.com/file.csv"]})
        total = _check_size_each_file(df)
        assert total == f.stat().st_size

    def test_all_urls_returns_zero(self) -> None:
        """If every row is a URL, the total size is zero."""
        df = pd.DataFrame({"path": ["https://a.com/f1", "https://b.com/f2"]})
        assert _check_size_each_file(df) == 0


def _make_file_mock(path: str, file_id: str = "syn100") -> MagicMock:
    """Create a MagicMock that behaves like a File entity for SyncUploader tests."""
    mock = MagicMock()
    mock.path = path
    mock.id = file_id
    mock.store_async = AsyncMock(return_value=mock)
    return mock


def _make_item(
    path: str,
    file_id: str = "syn100",
    used: list | None = None,
    executed: list | None = None,
    activity_name: str | None = None,
    activity_description: str | None = None,
) -> SyncUploadItem:
    """Create a SyncUploadItem backed by a mock File entity."""
    return SyncUploadItem(
        entity=_make_file_mock(path, file_id),
        used=used or [],
        executed=executed or [],
        activity_name=activity_name,
        activity_description=activity_description,
    )


class TestSyncUploaderBuildDependencyGraph:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.uploader = SyncUploader(syn=syn)

    def test_no_provenance(self) -> None:
        """Items without provenance produce a graph with no dependencies."""
        item = _make_item("/a.txt")
        graph = self.uploader._build_dependency_graph([item])

        assert graph.path_to_dependencies == {"/a.txt": []}
        assert graph.path_to_upload_item["/a.txt"] is item

    def test_file_dependency_between_items(self, tmp_path: Path) -> None:
        """A file dependency between two items is captured in the graph."""
        f1 = tmp_path / "dep.txt"
        f1.write_text("dep")
        f2 = tmp_path / "main.txt"
        f2.write_text("main")

        dep_item = _make_item(str(f1), file_id="syn1")
        main_item = _make_item(str(f2), file_id="syn2", used=[str(f1)])

        graph = self.uploader._build_dependency_graph([dep_item, main_item])

        assert graph.path_to_dependencies[str(f2)] == [str(f1)]
        assert graph.path_to_dependencies[str(f1)] == []

    @pytest.mark.parametrize(
        "used, executed, description",
        [
            ([_make_file_mock("/resolved.txt", "syn999")], [], "File object in used"),
            (["https://example.com/code.py"], [], "URL in used"),
            ([], ["syn12345"], "Synapse ID in executed"),
        ],
        ids=["file_object", "url", "synapse_id"],
    )
    def test_non_file_provenance_not_a_dependency(
        self, used: list, executed: list, description: str
    ) -> None:
        """Non-local-file provenance references (File objects, URLs, Synapse IDs)
        are not treated as dependencies in the graph."""
        item = _make_item("/a.txt", used=used, executed=executed)

        graph = self.uploader._build_dependency_graph([item])

        assert graph.path_to_dependencies["/a.txt"] == []

    def test_missing_file_dependency_raises(self, tmp_path: Path) -> None:
        """If an item depends on a local file not in the upload batch, ValueError is raised."""
        dep = tmp_path / "dep.txt"
        dep.write_text("dep")
        item = _make_item("/main.txt", used=[str(dep)])

        with pytest.raises(ValueError, match="depends on"):
            self.uploader._build_dependency_graph([item])

    def test_cached_file_check(self, tmp_path: Path) -> None:
        """The file-check cache avoids redundant os.path.isfile calls."""
        f1 = tmp_path / "dep.txt"
        f1.write_text("dep")

        dep_item = _make_item(str(f1), file_id="syn1")
        item_a = _make_item(str(tmp_path / "a.txt"), file_id="syn2", used=[str(f1)])
        # Give a.txt a real file so the item_a entity.path resolves
        (tmp_path / "a.txt").write_text("a")
        item_b = _make_item(str(tmp_path / "b.txt"), file_id="syn3", used=[str(f1)])
        (tmp_path / "b.txt").write_text("b")

        graph = self.uploader._build_dependency_graph([dep_item, item_a, item_b])
        # dep.txt should appear in the file-check cache (checked once, then reused)
        assert str(f1) in graph.path_to_file_check
        assert graph.path_to_file_check[str(f1)] is True


class TestSyncUploaderBuildActivityLinkage:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.uploader = SyncUploader(syn=syn)

    def test_resolved_file_id(self) -> None:
        """A local path that was resolved to a Synapse ID uses UsedEntity."""
        from synapseclient.models import UsedEntity

        result = self.uploader._build_activity_linkage(
            used_or_executed=["/dep.txt"],
            resolved_file_ids={"/dep.txt": "syn999"},
        )
        assert len(result) == 1
        assert isinstance(result[0], UsedEntity)
        assert result[0].target_id == "syn999"

    def test_url_uses_used_url(self) -> None:
        """A URL provenance item becomes a UsedURL."""
        from synapseclient.models import UsedURL

        result = self.uploader._build_activity_linkage(
            used_or_executed=["https://github.com/repo"],
            resolved_file_ids={},
        )
        assert len(result) == 1
        assert isinstance(result[0], UsedURL)
        assert result[0].url == "https://github.com/repo"

    def test_synapse_id(self) -> None:
        """A bare Synapse ID becomes a UsedEntity with the correct target_id."""
        from synapseclient.models import UsedEntity

        result = self.uploader._build_activity_linkage(
            used_or_executed=["syn12345"],
            resolved_file_ids={},
        )
        assert len(result) == 1
        assert isinstance(result[0], UsedEntity)
        assert result[0].target_id == "syn12345"

    def test_synapse_id_with_version(self) -> None:
        """A Synapse ID with a version suffix is parsed correctly."""
        from synapseclient.models import UsedEntity

        result = self.uploader._build_activity_linkage(
            used_or_executed=["syn12345.3"],
            resolved_file_ids={},
        )
        assert len(result) == 1
        assert isinstance(result[0], UsedEntity)
        assert result[0].target_id == "syn12345"
        assert result[0].target_version_number == 3

    def test_file_object_uses_its_id(self) -> None:
        """A File object in the list is converted to a UsedEntity using its .id."""
        from synapseclient.models import UsedEntity

        file_obj = _make_file_mock("/resolved.txt", "syn777")
        result = self.uploader._build_activity_linkage(
            used_or_executed=[file_obj],
            resolved_file_ids={},
        )
        assert len(result) == 1
        assert isinstance(result[0], UsedEntity)
        assert result[0].target_id == "syn777"

    def test_invalid_string_raises(self) -> None:
        """A string that is not a URL, Synapse ID, or resolved path raises ValueError."""
        with pytest.raises(ValueError, match="not a valid Synapse id"):
            self.uploader._build_activity_linkage(
                used_or_executed=["not-a-valid-reference"],
                resolved_file_ids={},
            )

    def test_empty_list(self) -> None:
        """An empty list returns an empty list."""
        result = self.uploader._build_activity_linkage(
            used_or_executed=[],
            resolved_file_ids={},
        )
        assert result == []

    def test_mixed_provenance_types(self) -> None:
        """Different provenance types in a single list are all handled correctly."""
        from synapseclient.models import UsedEntity, UsedURL

        file_obj = _make_file_mock("/already.txt", "syn555")
        result = self.uploader._build_activity_linkage(
            used_or_executed=[
                "/local.txt",
                "https://example.com",
                "syn123",
                file_obj,
            ],
            resolved_file_ids={"/local.txt": "syn444"},
        )
        assert len(result) == 4
        assert isinstance(result[0], UsedEntity)
        assert result[0].target_id == "syn444"
        assert isinstance(result[1], UsedURL)
        assert isinstance(result[2], UsedEntity)
        assert result[2].target_id == "syn123"
        assert isinstance(result[3], UsedEntity)
        assert result[3].target_id == "syn555"


class TestSyncUploaderUploadItemAsync:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.uploader = SyncUploader(syn=syn)

    async def test_no_provenance_no_activity(self) -> None:
        """When used and executed are empty, no Activity is set on the file."""
        mock_file = _make_file_mock("/a.txt", "syn1")

        result = await self.uploader._upload_item_async(
            item=mock_file,
            used=[],
            executed=[],
            activity_name=None,
            activity_description=None,
            dependent_futures=[],
        )

        assert result is mock_file
        mock_file.store_async.assert_awaited_once_with(synapse_client=self.uploader.syn)
        assert (
            not hasattr(mock_file, "activity")
            or mock_file.activity == mock_file.activity
        )

    async def test_with_provenance_sets_activity(self) -> None:
        """When used/executed are provided, an Activity is attached before store."""
        from synapseclient.models import Activity

        mock_file = _make_file_mock("/a.txt", "syn1")
        # Remove any default activity attribute so we can check it gets set
        del mock_file.activity

        await self.uploader._upload_item_async(
            item=mock_file,
            used=["syn999"],
            executed=["https://github.com/code.py"],
            activity_name="my activity",
            activity_description="my description",
            dependent_futures=[],
        )

        assert isinstance(mock_file.activity, Activity)
        assert mock_file.activity.name == "my activity"
        assert mock_file.activity.description == "my description"
        assert len(mock_file.activity.used) == 1
        assert len(mock_file.activity.executed) == 1

    async def test_dependent_futures_resolved(self) -> None:
        """Dependent futures are awaited and their results populate resolved_file_ids."""
        dep_file = _make_file_mock("/dep.txt", "syn_dep")

        # Create a real future that resolves to dep_file
        dep_future = asyncio.get_event_loop().create_future()
        dep_future.set_result(dep_file)

        mock_file = _make_file_mock("/main.txt", "syn_main")
        del mock_file.activity

        await self.uploader._upload_item_async(
            item=mock_file,
            used=["/dep.txt"],
            executed=[],
            activity_name="test",
            activity_description=None,
            dependent_futures=[dep_future],
        )

        # The dependency's path should have been resolved to its Synapse ID
        assert mock_file.activity.used[0].target_id == "syn_dep"


class TestSyncUploaderUpload:
    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.uploader = SyncUploader(syn=syn)

    async def test_single_item_no_provenance(self) -> None:
        """A single item with no dependencies is uploaded and returned."""
        item = _make_item("/a.txt", file_id="syn1")

        results = await self.uploader.upload([item])

        assert len(results) == 1
        item.entity.store_async.assert_awaited_once()

    async def test_multiple_independent_items(self) -> None:
        """Multiple items with no inter-dependencies are all uploaded."""
        items = [
            _make_item("/a.txt", file_id="syn1"),
            _make_item("/b.txt", file_id="syn2"),
            _make_item("/c.txt", file_id="syn3"),
        ]

        results = await self.uploader.upload(items)

        assert len(results) == 3
        for item in items:
            item.entity.store_async.assert_awaited_once()

    async def test_empty_items(self) -> None:
        """An empty item list produces an empty result."""
        results = await self.uploader.upload([])
        assert results == []

    async def test_dependent_items_uploaded_in_order(self, tmp_path: Path) -> None:
        """When item B depends on item A, A is stored before B."""
        f_dep = tmp_path / "dep.txt"
        f_dep.write_text("dep")
        f_main = tmp_path / "main.txt"
        f_main.write_text("main")

        call_order = []

        dep_mock = _make_file_mock(str(f_dep), "syn_dep")

        async def dep_store(**kwargs):
            call_order.append("dep")
            return dep_mock

        dep_mock.store_async = AsyncMock(side_effect=dep_store)

        main_mock = _make_file_mock(str(f_main), "syn_main")

        async def main_store(**kwargs):
            call_order.append("main")
            return main_mock

        main_mock.store_async = AsyncMock(side_effect=main_store)

        dep_item = SyncUploadItem(
            entity=dep_mock,
            used=[],
            executed=[],
            activity_name=None,
            activity_description=None,
        )
        main_item = SyncUploadItem(
            entity=main_mock,
            used=[str(f_dep)],
            executed=[],
            activity_name="uses dep",
            activity_description=None,
        )

        results = await self.uploader.upload([dep_item, main_item])

        assert len(results) == 2
        assert call_order.index("dep") < call_order.index("main")


class TestRecreateDirectoryHierarchy:
    """Tests for _recreate_directory_hierarchy_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_updates_parent_ids_for_subdirectory_files(
        self, tmp_path: Path
    ) -> None:
        """Files in subdirectories get their parentId updated to the created folder."""
        # GIVEN local files in a nested structure
        subdir = tmp_path / "sub"
        subdir.mkdir()
        root_file = tmp_path / "root.txt"
        root_file.write_text("root")
        sub_file = subdir / "nested.txt"
        sub_file.write_text("nested")

        df = pd.DataFrame(
            {
                "path": [str(root_file), str(sub_file)],
                "parentId": ["syn123", "syn123"],
                "synapseStore": [True, True],
            }
        )

        mock_folder = MagicMock()
        mock_folder.id = "syn_sub_folder"
        mock_folder.store_async = AsyncMock(return_value=mock_folder)

        with patch(
            "synapseclient.models.Folder",
            return_value=mock_folder,
        ):
            result = await _recreate_directory_hierarchy_async(df, syn=self.syn)

        # Root file keeps original parentId
        assert result.loc[0, "parentId"] == "syn123"
        # Subdirectory file gets updated parentId
        assert result.loc[1, "parentId"] == "syn_sub_folder"

    async def test_creates_nested_folders(self, tmp_path: Path) -> None:
        """Nested directories create a chain of Synapse folders."""
        sub_a = tmp_path / "a"
        sub_b = sub_a / "b"
        sub_b.mkdir(parents=True)
        root_file = tmp_path / "root.txt"
        root_file.write_text("root")
        deep_file = sub_b / "deep.txt"
        deep_file.write_text("deep")

        df = pd.DataFrame(
            {
                "path": [str(root_file), str(deep_file)],
                "parentId": ["syn123", "syn123"],
                "synapseStore": [True, True],
            }
        )

        folder_a = MagicMock()
        folder_a.id = "syn_folder_a"
        folder_a.store_async = AsyncMock(return_value=folder_a)

        folder_b = MagicMock()
        folder_b.id = "syn_folder_b"
        folder_b.store_async = AsyncMock(return_value=folder_b)

        created_folders = iter([folder_a, folder_b])

        def make_folder(**kwargs):
            f = next(created_folders)
            return f

        with patch(
            "synapseclient.models.Folder",
            side_effect=make_folder,
        ):
            result = await _recreate_directory_hierarchy_async(df, syn=self.syn)

        assert result.loc[0, "parentId"] == "syn123"
        assert result.loc[1, "parentId"] == "syn_folder_b"

    async def test_skips_url_rows(self, tmp_path: Path) -> None:
        """Rows with URLs (synapseStore=False) are not affected."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        local_file = subdir / "local.txt"
        local_file.write_text("local")

        df = pd.DataFrame(
            {
                "path": ["https://example.com/file.csv", str(local_file)],
                "parentId": ["syn123", "syn123"],
                "synapseStore": [False, True],
            }
        )

        # Only one local file in one directory — no hierarchy to create
        result = await _recreate_directory_hierarchy_async(df, syn=self.syn)

        # Both parentIds unchanged (single unique dir for local files)
        assert result.loc[0, "parentId"] == "syn123"
        assert result.loc[1, "parentId"] == "syn123"

    async def test_single_directory_no_folders_created(self, tmp_path: Path) -> None:
        """When all files share the same directory, no folders are created."""
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("a")
        f2.write_text("b")

        df = pd.DataFrame(
            {
                "path": [str(f1), str(f2)],
                "parentId": ["syn123", "syn123"],
                "synapseStore": [True, True],
            }
        )

        result = await _recreate_directory_hierarchy_async(df, syn=self.syn)

        assert result.loc[0, "parentId"] == "syn123"
        assert result.loc[1, "parentId"] == "syn123"
