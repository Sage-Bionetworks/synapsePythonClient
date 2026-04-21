"""Unit tests for DownloadList helper methods."""

import csv
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError, SynapseHTTPError
from synapseclient.models.download_list import DownloadList, DownloadListItem
from synapseclient.models.table_components import CsvTableDescriptor


class TestReadManifestRows:
    """Tests for DownloadList._read_manifest_rows."""

    def _write_csv(self, tmp_path: Path, header: list[str], rows: list[dict]) -> str:
        path = str(tmp_path / "manifest.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        return path

    @pytest.mark.parametrize(
        "csv_content, expected_columns, expected_row_count, row_checks",
        [
            pytest.param(
                "ID,versionNumber,name\nsyn111,1,file_a.txt\nsyn222,3,file_b.txt\n",
                ["ID", "versionNumber", "name"],
                2,
                {0: {"ID": "syn111", "versionNumber": "1"}, 1: {"ID": "syn222"}},
                id="standard_manifest",
            ),
            pytest.param(
                "ID,versionNumber\n",
                ["ID", "versionNumber"],
                0,
                {},
                id="headers_only_no_rows",
            ),
            pytest.param(
                "",
                None,
                0,
                {},
                id="empty_file",
            ),
            pytest.param(
                "ID\nsyn999\n",
                ["ID"],
                1,
                {0: {"ID": "syn999"}},
                id="single_column",
            ),
            pytest.param(
                "ID,versionNumber\nsyn123,\n",
                ["ID", "versionNumber"],
                1,
                {0: {"ID": "syn123", "versionNumber": ""}},
                id="empty_string_values_preserved",
            ),
            pytest.param(
                'ID,name\nsyn123,"file, with comma.txt"\n',
                ["ID", "name"],
                1,
                {0: {"name": "file, with comma.txt"}},
                id="quoted_field_with_comma",
            ),
        ],
    )
    def test_read_manifest_rows(
        self,
        tmp_path: Path,
        csv_content: str,
        expected_columns: list[str],
        expected_row_count: int,
        row_checks: dict[int, dict[str, str]],
    ) -> None:
        """_read_manifest_rows returns correct columns and rows for various CSV shapes."""
        # GIVEN a CSV file with the specified content
        path = str(tmp_path / "manifest.csv")
        with open(path, "w", newline="") as f:
            f.write(csv_content)

        # WHEN I read the manifest
        columns, rows = DownloadList._read_manifest_rows(path)

        # THEN columns and row count match expectations
        assert columns == expected_columns
        assert len(rows) == expected_row_count

        # AND specific cell values match
        for row_idx, expected_values in row_checks.items():
            for key, value in expected_values.items():
                assert rows[row_idx][key] == value

    def test_many_rows(self, tmp_path: Path) -> None:
        """Reading a manifest with many rows returns all of them."""
        # GIVEN a CSV with 500 rows
        header = ["ID", "versionNumber"]
        data = [{"ID": f"syn{i}", "versionNumber": str(i)} for i in range(500)]
        path = self._write_csv(tmp_path, header, data)

        # WHEN I read the manifest
        columns, rows = DownloadList._read_manifest_rows(path)

        # THEN all 500 rows are returned
        assert columns == header
        assert len(rows) == 500
        assert rows[0]["ID"] == "syn0"
        assert rows[499]["ID"] == "syn499"


class TestValidateAndExtendColumns:
    """Tests for DownloadList._validate_and_extend_columns."""

    @pytest.mark.parametrize(
        "columns, expected",
        [
            pytest.param(
                ["ID", "versionNumber"],
                ["ID", "versionNumber", "path", "error"],
                id="standard_columns",
            ),
            pytest.param(
                ["ID"],
                ["ID", "path", "error"],
                id="single_column",
            ),
            pytest.param(
                ["ID", "versionNumber", "name", "createdBy"],
                ["ID", "versionNumber", "name", "createdBy", "path", "error"],
                id="many_columns",
            ),
        ],
    )
    def test_appends_path_and_error(
        self, columns: list[str], expected: list[str]
    ) -> None:
        """Valid columns are returned with path and error appended."""
        assert DownloadList._validate_and_extend_columns(columns) == expected

    def test_none_columns_raises(self) -> None:
        """None columns (empty manifest) raises SynapseError."""
        with pytest.raises(SynapseError, match="no headers"):
            DownloadList._validate_and_extend_columns(None)

    @pytest.mark.parametrize(
        "columns",
        [
            pytest.param(["ID", "path"], id="contains_path"),
            pytest.param(["ID", "error"], id="contains_error"),
            pytest.param(["path", "error"], id="contains_both"),
        ],
    )
    def test_reserved_column_names_raise(self, columns: list[str]) -> None:
        """Columns containing reserved names 'path' or 'error' raise SynapseError."""
        with pytest.raises(SynapseError, match="reserved column names"):
            DownloadList._validate_and_extend_columns(columns)


class TestClearAsync:
    """Tests for DownloadList.clear_async."""

    async def test_clear_async(self, syn: Synapse) -> None:
        """clear_async issues a DELETE to /download/list via the client."""
        # GIVEN a mocked rest_delete_async on the client
        with patch.object(
            syn,
            "rest_delete_async",
            new_callable=AsyncMock,
            return_value=None,
        ) as mocked_delete:
            # WHEN I call clear_async with an explicit client
            result = await DownloadList.clear_async(synapse_client=syn)

            # THEN the client issues a DELETE to /download/list
            mocked_delete.assert_awaited_once_with("/download/list")
            # AND the method returns None
            assert result is None


class TestAddFilesAsync:
    """Tests for DownloadList.add_files_async."""

    async def test_add_files_async(self, syn: Synapse) -> None:
        """add_files_async POSTs the batch to /download/list/add and returns the count."""
        # GIVEN a list of files to add and a mocked rest_post_async on the client
        files = [
            DownloadListItem(file_entity_id="syn111", version_number=1),
            DownloadListItem(file_entity_id="syn222", version_number=None),
        ]
        with patch.object(
            syn,
            "rest_post_async",
            new_callable=AsyncMock,
            return_value={"numberOfFilesAdded": 2},
        ) as mocked_post:
            # WHEN I call add_files_async with an explicit client
            result = await DownloadList.add_files_async(
                files=files, synapse_client=syn
            )

            # THEN the client POSTs the batch to /download/list/add
            mocked_post.assert_awaited_once()
            call = mocked_post.await_args
            assert call.args == ("/download/list/add",)
            assert json.loads(call.kwargs["body"]) == {
                "batchToAdd": [
                    {"fileEntityId": "syn111", "versionNumber": 1},
                    {"fileEntityId": "syn222", "versionNumber": None},
                ]
            }
            # AND the method returns the number of files added
            assert result == 2


class TestRemoveFilesAsync:
    """Tests for DownloadList.remove_files_async."""

    async def test_remove_files_async(self, syn: Synapse) -> None:
        """remove_files_async POSTs the batch to /download/list/remove and returns the count."""
        # GIVEN a list of files to remove and a mocked rest_post_async on the client
        files = [
            DownloadListItem(file_entity_id="syn111", version_number=1),
            DownloadListItem(file_entity_id="syn222", version_number=None),
        ]
        with patch.object(
            syn,
            "rest_post_async",
            new_callable=AsyncMock,
            return_value={"numberOfFilesRemoved": 2},
        ) as mocked_post:
            # WHEN I call remove_files_async with an explicit client
            result = await DownloadList.remove_files_async(
                files=files, synapse_client=syn
            )

            # THEN the client POSTs the batch to /download/list/remove
            mocked_post.assert_awaited_once()
            call = mocked_post.await_args
            assert call.args == ("/download/list/remove",)
            assert json.loads(call.kwargs["body"]) == {
                "batchToRemove": [
                    {"fileEntityId": "syn111", "versionNumber": 1},
                    {"fileEntityId": "syn222", "versionNumber": None},
                ]
            }
            # AND the method returns the number of files removed
            assert result == 2


class TestGetManifestAsync:
    """Tests for DownloadList.get_manifest_async."""

    async def test_get_manifest_async(self, syn: Synapse) -> None:
        """get_manifest_async submits the request and returns the downloaded manifest path."""
        # GIVEN a mocked DownloadListManifestRequest whose job populates manifest_path
        manifest_path = "/tmp/manifest.csv"
        mock_instance = MagicMock()
        mock_instance.send_job_and_wait_async = AsyncMock(return_value=None)
        mock_instance.manifest_path = manifest_path
        descriptor = CsvTableDescriptor()
        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_instance,
        ) as mocked_request_cls:
            # WHEN I call get_manifest_async with an explicit descriptor and destination
            result = await DownloadList.get_manifest_async(
                csv_table_descriptor=descriptor,
                destination="/tmp/out",
                synapse_client=syn,
            )

            # THEN the request is built with the provided descriptor
            mocked_request_cls.assert_called_once_with(csv_table_descriptor=descriptor)
            # AND the job is awaited once with the destination and client
            mock_instance.send_job_and_wait_async.assert_awaited_once_with(
                post_exchange_args={"destination": "/tmp/out"},
                synapse_client=syn,
            )
            # AND the method returns the manifest path set by the job
            assert result == manifest_path

    async def test_get_manifest_async_no_file_produced(self, syn: Synapse) -> None:
        """get_manifest_async raises SynapseError when the job finishes without a file."""
        # GIVEN a mocked DownloadListManifestRequest whose job leaves manifest_path None
        mock_instance = MagicMock()
        mock_instance.send_job_and_wait_async = AsyncMock(return_value=None)
        mock_instance.manifest_path = None
        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_instance,
        ):
            # WHEN I call get_manifest_async
            # THEN a SynapseError is raised
            with pytest.raises(SynapseError, match="no local file was produced"):
                await DownloadList.get_manifest_async(synapse_client=syn)


class TestDownloadFilesAsync:
    """Tests for DownloadList.download_files_async."""

    async def test_empty_cart_propagates_synapse_http_error(
        self, syn: Synapse
    ) -> None:
        """download_files_async propagates the server's 'No files available for
        download' error when the cart is empty.

        Synapse returns this error from the manifest async job rather than
        returning an empty manifest, and the method must not swallow it.
        """
        # GIVEN get_manifest_async raises SynapseHTTPError (simulating an empty cart)
        with patch.object(
            DownloadList,
            "get_manifest_async",
            new_callable=AsyncMock,
            side_effect=SynapseHTTPError("No files available for download"),
        ):
            # WHEN I call download_files_async
            # THEN the error propagates to the caller unchanged
            with pytest.raises(
                SynapseHTTPError, match="No files available for download"
            ):
                await DownloadList.download_files_async(synapse_client=syn)


class TestDownloadManifestFile:
    """Tests for DownloadList._download_manifest_file."""

    async def test_success_annotates_row_and_returns_item(self, syn: Synapse) -> None:
        """On success, the row is annotated with path/error and a DownloadListItem
        is returned with the resolved entity id and version."""
        # GIVEN a manifest row with a version and a mocked File whose
        # get_async returns a file with a local path
        row = {"ID": "syn111", "versionNumber": "2"}
        mock_file = MagicMock()
        mock_file.path = "/tmp/downloads/file_a.txt"
        mock_file_cls = MagicMock(
            return_value=MagicMock(get_async=AsyncMock(return_value=mock_file))
        )
        with patch(
            "synapseclient.models.file.File",
            mock_file_cls,
        ):
            # WHEN I call _download_manifest_file
            result = await DownloadList._download_manifest_file(
                row,
                download_location="/tmp/downloads",
                synapse_client=syn,
            )

        # THEN the File is constructed with the coerced int version and
        # download_location as path
        mock_file_cls.assert_called_once_with(
            id="syn111",
            version_number=2,
            path="/tmp/downloads",
        )
        # AND the row is annotated with the local path and empty error
        assert row["path"] == "/tmp/downloads/file_a.txt"
        assert row["error"] == ""
        # AND the returned DownloadListItem carries the entity id and version
        assert result == DownloadListItem(file_entity_id="syn111", version_number=2)

    @pytest.mark.parametrize(
        "row",
        [
            pytest.param({"ID": "syn111"}, id="no_version_key"),
            pytest.param({"ID": "syn111", "versionNumber": ""}, id="blank_version"),
            pytest.param({"ID": "syn111", "versionNumber": None}, id="none_version"),
        ],
    )
    async def test_missing_version_fetches_latest(
        self, syn: Synapse, row: dict
    ) -> None:
        """A missing or blank versionNumber is passed through as None so
        File.get_async fetches the latest version."""
        # GIVEN a manifest row without a usable version and a mocked File
        mock_file = MagicMock()
        mock_file.path = "/tmp/downloads/latest.txt"
        mock_file_cls = MagicMock(
            return_value=MagicMock(get_async=AsyncMock(return_value=mock_file))
        )
        with patch(
            "synapseclient.models.file.File",
            mock_file_cls,
        ):
            # WHEN I call _download_manifest_file
            result = await DownloadList._download_manifest_file(
                row,
                download_location="/tmp/downloads",
                synapse_client=syn,
            )

        # THEN File is constructed with version_number=None (meaning latest)
        mock_file_cls.assert_called_once_with(
            id="syn111",
            version_number=None,
            path="/tmp/downloads",
        )
        # AND the row is annotated for success
        assert row["path"] == "/tmp/downloads/latest.txt"
        assert row["error"] == ""
        # AND the returned DownloadListItem also carries version_number=None
        assert result == DownloadListItem(
            file_entity_id="syn111", version_number=None
        )

    async def test_get_async_failure_annotates_row_and_returns_none(
        self, syn: Synapse
    ) -> None:
        """When File.get_async raises, the exception is swallowed, the row is
        annotated with the error message, and None is returned so the batch
        continues."""
        # GIVEN a manifest row and a File whose get_async raises
        row = {"ID": "syn999", "versionNumber": "1"}
        error_message = "boom"
        mock_file_cls = MagicMock(
            return_value=MagicMock(
                get_async=AsyncMock(side_effect=RuntimeError(error_message))
            )
        )
        with patch(
            "synapseclient.models.file.File",
            mock_file_cls,
        ):
            # WHEN I call _download_manifest_file
            result = await DownloadList._download_manifest_file(
                row, synapse_client=syn
            )

        # THEN the row is annotated with the error message and empty path
        assert row["path"] == ""
        assert row["error"] == error_message
        # AND None is returned (so the caller skips this row)
        assert result is None

    async def test_file_with_no_path_sets_row_path_empty(self, syn: Synapse) -> None:
        """If get_async returns a file whose path is None, the row's path is
        normalized to an empty string rather than the literal None."""
        # GIVEN a mocked File whose returned instance has path=None
        row = {"ID": "syn111", "versionNumber": "1"}
        mock_file = MagicMock()
        mock_file.path = None
        mock_file_cls = MagicMock(
            return_value=MagicMock(get_async=AsyncMock(return_value=mock_file))
        )
        with patch(
            "synapseclient.models.file.File",
            mock_file_cls,
        ):
            # WHEN I call _download_manifest_file
            result = await DownloadList._download_manifest_file(
                row, synapse_client=syn
            )

        # THEN the row's path is an empty string (not None)
        assert row["path"] == ""
        assert row["error"] == ""
        # AND a DownloadListItem is still returned for the successful call
        assert result == DownloadListItem(file_entity_id="syn111", version_number=1)
