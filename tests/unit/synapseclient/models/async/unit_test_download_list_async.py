"""Unit tests for asynchronous methods in DownloadList, DownloadListManifestRequest."""

import csv
import os
import shutil
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.api.download_list_services import (
    add_to_download_list_async,
    remove_from_download_list_async,
)
from synapseclient.core.constants.concrete_types import DOWNLOAD_LIST_MANIFEST_REQUEST
from synapseclient.core.exceptions import SynapseError
from synapseclient.models.download_list import (
    DownloadList,
    DownloadListItem,
    DownloadListManifestRequest,
)
from synapseclient.models.table_components import CsvTableDescriptor


class TestDownloadListServices:
    """Unit tests for download_list_services API functions."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_add_to_download_list_async_returns_count(self):
        """add_to_download_list_async extracts numberOfFilesAdded from the response."""
        files = [DownloadListItem(file_entity_id="syn123", version_number=1)]
        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "rest_post_async",
                new_callable=AsyncMock,
                return_value={"numberOfFilesAdded": 1},
            ),
        ):
            result = await add_to_download_list_async(
                files=files, synapse_client=self.syn
            )
        assert result == 1

    async def test_add_to_download_list_async_raises_on_missing_key(self):
        """add_to_download_list_async raises KeyError when numberOfFilesAdded is missing."""
        files = [DownloadListItem(file_entity_id="syn123", version_number=1)]
        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "rest_post_async",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            with pytest.raises(KeyError, match="numberOfFilesAdded"):
                await add_to_download_list_async(files=files, synapse_client=self.syn)

    async def test_remove_from_download_list_async_returns_count(self):
        """remove_from_download_list_async extracts numberOfFilesRemoved from the response."""
        files = [DownloadListItem(file_entity_id="syn123", version_number=1)]
        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "rest_post_async",
                new_callable=AsyncMock,
                return_value={"numberOfFilesRemoved": 1},
            ),
        ):
            result = await remove_from_download_list_async(
                files=files, synapse_client=self.syn
            )
        assert result == 1

    async def test_remove_from_download_list_async_raises_on_missing_key(self):
        """remove_from_download_list_async raises KeyError when numberOfFilesRemoved is missing."""
        files = [DownloadListItem(file_entity_id="syn123", version_number=1)]
        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "rest_post_async",
                new_callable=AsyncMock,
                return_value={},
            ),
        ):
            with pytest.raises(KeyError, match="numberOfFilesRemoved"):
                await remove_from_download_list_async(
                    files=files, synapse_client=self.syn
                )


class TestDownloadListManifestRequest:
    """Unit tests for DownloadListManifestRequest."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_to_synapse_request(self):
        # GIVEN a DownloadListManifestRequest
        request = DownloadListManifestRequest()
        # WHEN I call to_synapse_request
        result = request.to_synapse_request()
        # THEN the result has the correct structure
        assert result["concreteType"] == DOWNLOAD_LIST_MANIFEST_REQUEST
        assert "csvTableDescriptor" in result
        csv_desc = result["csvTableDescriptor"]
        assert csv_desc["separator"] == ","
        assert csv_desc["quoteCharacter"] == '"'
        assert csv_desc["isFirstLineHeader"] is True

    @pytest.mark.parametrize(
        "response,expected",
        [
            ({"resultFileHandleId": "fh123"}, "fh123"),
            ({}, None),
        ],
    )
    def test_fill_from_dict(self, response, expected):
        # GIVEN a DownloadListManifestRequest
        request = DownloadListManifestRequest()
        # WHEN I call fill_from_dict with the response
        result = request.fill_from_dict(response)
        # THEN result_file_handle_id matches expected and the same object is returned
        assert result.result_file_handle_id == expected
        assert result is request

    async def test_post_exchange_async(self):
        # GIVEN a DownloadListManifestRequest with a result_file_handle_id
        request = DownloadListManifestRequest()
        request.result_file_handle_id = "fh456"

        fake_file_handle = {"id": "fh456", "contentMd5": "abc123"}
        fake_presigned_url = "https://example.com/manifest.csv"
        with (
            patch(
                "synapseclient.api.file_services.get_file_handle",
                new_callable=AsyncMock,
                return_value=fake_file_handle,
            ) as mock_get_fh,
            patch(
                "synapseclient.api.file_services.get_file_handle_presigned_url",
                new_callable=AsyncMock,
                return_value=fake_presigned_url,
            ) as mock_get_url,
            patch(
                "synapseclient.core.download.download_functions.download_from_url",
                return_value="/tmp/manifest.csv",
            ) as mock_download,
            patch(
                "asyncio.to_thread",
                new_callable=AsyncMock,
                return_value="/tmp/manifest.csv",
            ) as mock_to_thread,
        ):
            # WHEN I call _post_exchange_async
            await request._post_exchange_async(
                synapse_client=self.syn, destination="/tmp"
            )
            # THEN get_file_handle is called with the file handle ID
            mock_get_fh.assert_called_once_with(
                file_handle_id="fh456",
                synapse_client=self.syn,
            )
            # AND get_file_handle_presigned_url is called with the file handle ID
            mock_get_url.assert_called_once_with(
                file_handle_id="fh456",
                synapse_client=self.syn,
            )
            # AND the manifest path is set
            assert request.manifest_path == "/tmp/manifest.csv"
            # AND asyncio.to_thread was called with download_from_url
            mock_to_thread.assert_called_once()
            first_arg = mock_to_thread.call_args.args[0]
            assert first_arg is mock_download


class TestDownloadListClear:
    """Unit tests for DownloadList.clear_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_clear_async(self):
        # GIVEN a mocked clear_download_list_async
        with patch(
            "synapseclient.models.download_list.clear_download_list_async",
            new_callable=AsyncMock,
        ) as mock_clear:
            # WHEN I call clear_async
            await DownloadList.clear_async(synapse_client=self.syn)
            # THEN the API function is called
            mock_clear.assert_called_once_with(synapse_client=self.syn)

    def test_clear_sync_wrapper_exists(self):
        # THEN the sync wrapper is present on the class
        assert hasattr(DownloadList, "clear")
        assert callable(DownloadList.clear)


class TestDownloadListAddFiles:
    """Unit tests for DownloadList.add_files_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_add_files_async(self):
        # GIVEN a list of DownloadListItems
        files = [
            DownloadListItem(file_entity_id="syn123", version_number=1),
            DownloadListItem(file_entity_id="syn456", version_number=3),
        ]
        with patch(
            "synapseclient.models.download_list.add_to_download_list_async",
            new_callable=AsyncMock,
            return_value=2,
        ) as mock_add:
            # WHEN I call add_files_async
            result = await DownloadList.add_files_async(
                files=files, synapse_client=self.syn
            )
            # THEN the API function is called with the correct arguments
            mock_add.assert_called_once_with(files=files, synapse_client=self.syn)
            # AND the count is returned
            assert result == 2

    def test_add_files_sync_wrapper_exists(self):
        assert hasattr(DownloadList, "add_files")
        assert callable(DownloadList.add_files)


class TestDownloadListRemoveFiles:
    """Unit tests for DownloadList.remove_files_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_remove_files_async(self):
        # GIVEN a list of DownloadListItems
        files = [
            DownloadListItem(file_entity_id="syn123", version_number=1),
            DownloadListItem(file_entity_id="syn456", version_number=3),
        ]
        with patch(
            "synapseclient.models.download_list.remove_from_download_list_async",
            new_callable=AsyncMock,
            return_value=2,
        ) as mock_remove:
            # WHEN I call remove_files_async
            result = await DownloadList.remove_files_async(
                files=files, synapse_client=self.syn
            )
            # THEN the API function is called with the correct arguments
            mock_remove.assert_called_once_with(files=files, synapse_client=self.syn)
            # AND the count is returned
            assert result == 2

    async def test_remove_files_async_allows_none_version(self):
        # GIVEN a list containing an item with no version number
        files = [DownloadListItem(file_entity_id="syn123", version_number=None)]
        with patch(
            "synapseclient.models.download_list.remove_from_download_list_async",
            new_callable=AsyncMock,
            return_value=1,
        ) as mock_remove:
            # WHEN I call remove_files_async
            result = await DownloadList.remove_files_async(
                files=files, synapse_client=self.syn
            )
            # THEN the API function is called with the item as-is (version_number=None)
            mock_remove.assert_called_once_with(files=files, synapse_client=self.syn)
            assert result == 1

    def test_remove_files_sync_wrapper_exists(self):
        assert hasattr(DownloadList, "remove_files")
        assert callable(DownloadList.remove_files)


class TestDownloadListGetManifest:
    """Unit tests for DownloadList.get_manifest_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_get_manifest_async(self):
        # GIVEN a mocked DownloadListManifestRequest that returns a manifest path
        mock_request = MagicMock()
        mock_request.manifest_path = "/tmp/manifest.csv"
        mock_request.send_job_and_wait_async = AsyncMock(return_value=mock_request)

        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_request,
        ):
            # WHEN I call get_manifest_async
            result = await DownloadList.get_manifest_async(synapse_client=self.syn)
            # THEN send_job_and_wait_async is called and the path is returned
            mock_request.send_job_and_wait_async.assert_called_once_with(
                post_exchange_args={"destination": "."},
                synapse_client=self.syn,
            )
            assert result == "/tmp/manifest.csv"

    async def test_get_manifest_async_forwards_csv_table_descriptor(self):
        """get_manifest_async passes a custom CsvTableDescriptor to the request."""
        descriptor = CsvTableDescriptor(separator="\t", is_first_line_header=False)
        mock_request = MagicMock()
        mock_request.manifest_path = "/tmp/manifest.csv"
        mock_request.send_job_and_wait_async = AsyncMock(return_value=mock_request)

        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_request,
        ) as mock_cls:
            result = await DownloadList.get_manifest_async(
                csv_table_descriptor=descriptor, synapse_client=self.syn
            )
            mock_cls.assert_called_once_with(csv_table_descriptor=descriptor)
            assert result == "/tmp/manifest.csv"

    async def test_get_manifest_async_forwards_destination(self):
        """get_manifest_async passes destination through post_exchange_args."""
        mock_request = MagicMock()
        mock_request.manifest_path = "/custom/dir/manifest.csv"
        mock_request.send_job_and_wait_async = AsyncMock(return_value=mock_request)

        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_request,
        ):
            result = await DownloadList.get_manifest_async(
                destination="/custom/dir", synapse_client=self.syn
            )
            mock_request.send_job_and_wait_async.assert_called_once_with(
                post_exchange_args={"destination": "/custom/dir"},
                synapse_client=self.syn,
            )
            assert result == "/custom/dir/manifest.csv"

    async def test_get_manifest_async_raises_when_no_path(self):
        """get_manifest_async raises SynapseError when the job produces no local file."""
        # GIVEN a manifest request whose job completes but sets no manifest_path
        mock_request = MagicMock()
        mock_request.manifest_path = None
        mock_request.send_job_and_wait_async = AsyncMock()

        with patch(
            "synapseclient.models.download_list.DownloadListManifestRequest",
            return_value=mock_request,
        ):
            # WHEN / THEN a SynapseError is raised
            with pytest.raises(SynapseError, match="no local file was produced"):
                await DownloadList.get_manifest_async(synapse_client=self.syn)

    def test_get_manifest_sync_wrapper_exists(self):
        assert hasattr(DownloadList, "get_manifest")
        assert callable(DownloadList.get_manifest)


class TestDownloadListDownloadFiles:
    """Unit tests for DownloadList.download_files_async."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def _write_sample_manifest(self, path: str, rows: list) -> None:
        """Helper to write a sample manifest CSV."""
        fieldnames = ["ID", "versionNumber", "name"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    async def test_download_files_async_success(self):
        """Successful downloads are removed from the cart and written to the manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [{"ID": "syn123", "versionNumber": "1", "name": "file1.txt"}],
            )

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    new_callable=AsyncMock,
                    return_value=DownloadListItem(
                        file_entity_id="syn123", version_number=1
                    ),
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ) as mock_remove,
            ):
                result_path = await DownloadList.download_files_async(
                    download_location=tmpdir,
                    synapse_client=self.syn,
                )

            # THEN remove_from_download_list_async is called with the successful file
            mock_remove.assert_called_once()
            called_files = mock_remove.call_args.kwargs["files"]
            assert len(called_files) == 1
            assert called_files[0].file_entity_id == "syn123"
            # AND a new manifest CSV is created
            assert os.path.exists(result_path)
            os.remove(result_path)

    async def test_download_files_async_failure_leaves_in_cart(self):
        """Files that fail to download are NOT removed from the cart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [{"ID": "syn789", "versionNumber": "2", "name": "inaccessible.txt"}],
            )

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    new_callable=AsyncMock,
                    return_value=None,  # failure
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ) as mock_remove,
            ):
                result_path = await DownloadList.download_files_async(
                    synapse_client=self.syn,
                )

            # THEN remove is NOT called
            mock_remove.assert_not_called()
            assert os.path.exists(result_path)
            os.remove(result_path)

    async def test_download_row_success(self):
        """_download_row sets path on the row and returns a DownloadListItem."""
        fake_entity = MagicMock()
        fake_entity.path = "/tmp/file.txt"

        row = {"ID": "syn111", "versionNumber": "5", "name": "file.txt"}

        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "get_async",
                new_callable=AsyncMock,
                return_value=fake_entity,
            ),
        ):
            item = await DownloadList._download_row(
                row, download_location="/tmp", synapse_client=self.syn
            )

        assert item is not None
        assert item.file_entity_id == "syn111"
        assert item.version_number == 5
        assert row["path"] == "/tmp/file.txt"
        assert row["error"] == ""

    async def test_download_row_failure(self):
        """_download_row sets error on the row and returns None on failure."""
        row = {"ID": "syn222", "versionNumber": "1", "name": "secret.txt"}

        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "get_async",
                new_callable=AsyncMock,
                side_effect=SynapseError("Forbidden"),
            ),
        ):
            item = await DownloadList._download_row(row, synapse_client=self.syn)

        assert item is None
        assert row["path"] == ""
        assert "Forbidden" in row["error"]

    async def test_download_row_failure_unexpected_exception(self):
        """_download_row catches non-SynapseError exceptions so one failure does not abort the run."""
        row = {"ID": "syn222", "versionNumber": "1", "name": "secret.txt"}

        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "get_async",
                new_callable=AsyncMock,
                side_effect=AttributeError("path"),
            ),
        ):
            item = await DownloadList._download_row(row, synapse_client=self.syn)

        assert item is None
        assert row["path"] == ""
        assert "path" in row["error"]

    async def test_download_row_missing_version_number(self):
        """_download_row returns None and sets error when versionNumber is absent."""
        row = {"ID": "syn333", "versionNumber": "", "name": "file.txt"}

        with patch(
            "synapseclient.Synapse.get_client",
            return_value=self.syn,
        ):
            item = await DownloadList._download_row(row, synapse_client=self.syn)

        assert item is None
        assert row["path"] == ""
        assert "versionNumber" in row["error"]

    async def test_download_row_no_download_location(self):
        """_download_row does not pass downloadLocation when it is None."""
        fake_entity = MagicMock()
        fake_entity.path = "/tmp/file.txt"

        row = {"ID": "syn444", "versionNumber": "2", "name": "file.txt"}

        with (
            patch(
                "synapseclient.Synapse.get_client",
                return_value=self.syn,
            ),
            patch.object(
                self.syn,
                "get_async",
                new_callable=AsyncMock,
                return_value=fake_entity,
            ) as mock_get,
        ):
            item = await DownloadList._download_row(
                row, download_location=None, synapse_client=self.syn
            )

        assert item is not None
        # THEN get_async was called with only the entity ID and version — no downloadLocation
        mock_get.assert_called_once_with("syn444", version=2)

    def test_read_manifest_rows_headers_no_rows(self):
        """_read_manifest_rows returns (columns, []) for a CSV with headers but no data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            path = tmp.name

        try:
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["ID", "versionNumber", "name"])
                writer.writeheader()
                # no rows written

            columns, rows = DownloadList._read_manifest_rows(path)

            assert columns == ["ID", "versionNumber", "name"]
            assert rows == []
        finally:
            os.remove(path)

    def test_write_result_manifest(self):
        """_write_result_manifest writes all columns including path and error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            path = tmp.name

        try:
            columns = ["ID", "versionNumber", "name", "path", "error"]
            rows = [
                {
                    "ID": "syn1",
                    "versionNumber": "1",
                    "name": "a.txt",
                    "path": "/tmp/a.txt",
                    "error": "",
                }
            ]
            DownloadList._write_result_manifest(path=path, columns=columns, rows=rows)

            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                result_rows = list(reader)

            assert len(result_rows) == 1
            assert result_rows[0]["path"] == "/tmp/a.txt"
            assert result_rows[0]["error"] == ""
        finally:
            os.remove(path)

    async def test_download_files_async_empty_cart_logs_warning(self):
        """When no files are downloaded, a warning is logged and remove is not called."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            # Headers present but no data rows — simulates an empty manifest
            with open(manifest_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["ID", "versionNumber", "name"])
                writer.writeheader()

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ) as mock_remove,
                patch.object(self.syn.logger, "warning") as mock_warning,
            ):
                await DownloadList.download_files_async(
                    download_location=tmpdir,
                    synapse_client=self.syn,
                )

            # THEN remove is not called
            mock_remove.assert_not_called()
            # AND a warning was logged
            mock_warning.assert_called_once()
            assert "no files were downloaded" in mock_warning.call_args.args[0].lower()

    async def test_download_files_async_expands_tilde_in_download_location(self):
        """download_location with a leading ~ is expanded to the user's home directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [{"ID": "syn123", "versionNumber": "1", "name": "file1.txt"}],
            )

            captured_location = {}

            async def capture_download_row(
                row, download_location=None, *, synapse_client=None
            ):
                captured_location["value"] = download_location
                return DownloadListItem(file_entity_id=row["ID"], version_number=1)

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    side_effect=capture_download_row,
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ),
            ):
                result_path = await DownloadList.download_files_async(
                    download_location="~/my-download-dir",
                    synapse_client=self.syn,
                )

            # THEN ~ was expanded before being passed downstream
            expected = os.path.expanduser("~/my-download-dir")
            assert captured_location["value"] == expected
            # AND the manifest path does not contain a literal ~
            assert "~" not in result_path
            if os.path.exists(expected):
                shutil.rmtree(expected)

    async def test_save_result_manifest_path_in_download_location(self):
        """_save_result_manifest writes the manifest inside download_location."""
        with tempfile.TemporaryDirectory() as tmpdir:
            columns = ["ID", "versionNumber", "path", "error"]
            rows = [{"ID": "syn1", "versionNumber": "1", "path": "/tmp/f", "error": ""}]

            result_path = await DownloadList._save_result_manifest(
                rows=rows,
                columns=columns,
                download_location=tmpdir,
            )

            # THEN the manifest is inside tmpdir, not CWD
            assert os.path.dirname(os.path.abspath(result_path)) == os.path.abspath(
                tmpdir
            )
            assert os.path.exists(result_path)
            os.remove(result_path)

    async def test_download_files_async_parallel(self):
        """parallel=True uses asyncio.gather instead of a sequential loop."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [
                    {"ID": "syn123", "versionNumber": "1", "name": "file1.txt"},
                    {"ID": "syn456", "versionNumber": "2", "name": "file2.txt"},
                ],
            )

            returned_items = [
                DownloadListItem(file_entity_id="syn123", version_number=1),
                DownloadListItem(file_entity_id="syn456", version_number=2),
            ]

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    new_callable=AsyncMock,
                    side_effect=returned_items,
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ) as mock_remove,
            ):
                result_path = await DownloadList.download_files_async(
                    download_location=tmpdir,
                    parallel=True,
                    synapse_client=self.syn,
                )

            # THEN both files were removed from the cart
            mock_remove.assert_called_once()
            called_files = mock_remove.call_args.kwargs["files"]
            assert len(called_files) == 2
            assert os.path.exists(result_path)
            os.remove(result_path)

    async def test_download_files_async_parallel_partial_failure(self):
        """parallel=True: only successfully downloaded files are removed from the cart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [
                    {"ID": "syn123", "versionNumber": "1", "name": "file1.txt"},
                    {"ID": "syn456", "versionNumber": "2", "name": "inaccessible.txt"},
                ],
            )

            # syn123 succeeds, syn456 fails
            side_effects = [
                DownloadListItem(file_entity_id="syn123", version_number=1),
                None,
            ]

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    new_callable=AsyncMock,
                    side_effect=side_effects,
                ),
                patch(
                    "synapseclient.models.download_list.remove_from_download_list_async",
                    new_callable=AsyncMock,
                ) as mock_remove,
            ):
                result_path = await DownloadList.download_files_async(
                    download_location=tmpdir,
                    parallel=True,
                    synapse_client=self.syn,
                )

            # THEN only the successful file is removed from the cart
            mock_remove.assert_called_once()
            called_files = mock_remove.call_args.kwargs["files"]
            assert len(called_files) == 1
            assert called_files[0].file_entity_id == "syn123"
            assert os.path.exists(result_path)
            os.remove(result_path)

    @pytest.mark.parametrize(
        "fieldnames,error_match",
        [
            ([], "no headers"),
            (["ID", "versionNumber", "path"], "reserved column"),
        ],
    )
    async def test_download_files_async_manifest_cleaned_up_on_error(
        self, fieldnames, error_match
    ):
        """manifest_path is deleted even when reading the CSV raises a SynapseError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            with open(manifest_path, "w", newline="") as f:
                if fieldnames:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

            with patch.object(
                DownloadList,
                "get_manifest_async",
                new_callable=AsyncMock,
                return_value=manifest_path,
            ):
                with pytest.raises(SynapseError, match=error_match):
                    await DownloadList.download_files_async(synapse_client=self.syn)

            # THEN the temp manifest is cleaned up despite the error
            assert not os.path.exists(manifest_path)

    async def test_download_files_async_manifest_cleaned_up_on_write_failure(self):
        """manifest_path is deleted even when _write_result_manifest raises."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "original_manifest.csv")
            self._write_sample_manifest(
                manifest_path,
                [{"ID": "syn123", "versionNumber": "1", "name": "file1.txt"}],
            )

            with (
                patch.object(
                    DownloadList,
                    "get_manifest_async",
                    new_callable=AsyncMock,
                    return_value=manifest_path,
                ),
                patch.object(
                    DownloadList,
                    "_download_row",
                    new_callable=AsyncMock,
                    return_value=DownloadListItem(
                        file_entity_id="syn123", version_number=1
                    ),
                ),
                patch.object(
                    DownloadList,
                    "_save_result_manifest",
                    new_callable=AsyncMock,
                    side_effect=OSError("disk full"),
                ),
            ):
                with pytest.raises(OSError, match="disk full"):
                    await DownloadList.download_files_async(synapse_client=self.syn)

            # THEN the temp manifest is cleaned up despite the write failure
            assert not os.path.exists(manifest_path)

    async def test_download_files_async_max_concurrent_limits_parallelism(self):
        """max_concurrent caps the number of simultaneous downloads in parallel mode."""
        import asyncio

        concurrency_log: list[int] = []
        active = 0

        async def fake_download_row(
            row, download_location=None, *, synapse_client=None
        ):
            nonlocal active
            active += 1
            concurrency_log.append(active)
            await asyncio.sleep(0)  # yield so other coroutines can start
            active -= 1
            return DownloadListItem(file_entity_id=row["ID"], version_number=1)

        rows = [{"ID": f"syn{i}", "versionNumber": "1"} for i in range(20)]

        with patch.object(DownloadList, "_download_row", side_effect=fake_download_row):
            await DownloadList._download_all_rows(
                rows=rows,
                download_location=None,
                parallel=True,
                max_concurrent=5,
                synapse_client=self.syn,
            )

        assert (
            max(concurrency_log) <= 5
        ), f"Expected at most 5 concurrent downloads, got {max(concurrency_log)}"

    async def test_download_all_rows_max_concurrent_below_1_raises(self):
        """max_concurrent < 1 raises ValueError."""
        rows = [{"ID": "syn1", "versionNumber": "1"}]
        with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
            await DownloadList._download_all_rows(
                rows=rows,
                download_location=None,
                parallel=True,
                max_concurrent=0,
            )

    def test_download_files_sync_wrapper_exists(self):
        assert hasattr(DownloadList, "download_files")
        assert callable(DownloadList.download_files)
