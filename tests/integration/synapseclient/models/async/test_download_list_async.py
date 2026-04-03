"""Integration tests for DownloadList OOP model."""

import csv
import os
import tempfile
import uuid
from typing import Callable

import pytest

import synapseclient.core.utils as utils
from synapseclient import Project as Synapse_Project
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import DownloadList, DownloadListItem, File


class TestDownloadList:
    """Integration tests for the DownloadList model."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def _create_test_file(self, project: Synapse_Project) -> File:
        """Upload a small test file to Synapse and return the File model."""
        path = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(path)
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"download_list_test_{uuid.uuid4()}",
        )
        await file.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)
        return file

    async def _add_to_cart(self, file: File) -> None:
        """Add a single file to the Synapse download list cart."""
        await DownloadList.add_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file.id,
                    version_number=file.version_number,
                )
            ],
            synapse_client=self.syn,
        )

    async def test_add_files_adds_to_cart(self, project: Synapse_Project) -> None:
        """add_files_async() adds the specified file versions to the cart."""
        await DownloadList.clear_async(synapse_client=self.syn)

        file = await self._create_test_file(project)

        count = await DownloadList.add_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file.id,
                    version_number=file.version_number,
                )
            ],
            synapse_client=self.syn,
        )
        assert count == 1, f"Expected 1 file added, got {count}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=self.syn)
        self.schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            ids_in_cart = {row["ID"] for row in reader}

        await DownloadList.clear_async(synapse_client=self.syn)

        assert file.id in ids_in_cart, f"Expected {file.id} to be in the cart"

    async def test_download_files_downloads_and_removes_from_cart(
        self, project: Synapse_Project
    ) -> None:
        """Files downloaded successfully are present in the manifest and removed from cart."""
        await DownloadList.clear_async(synapse_client=self.syn)

        file = await self._create_test_file(project)
        await self._add_to_cart(file)

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await DownloadList.download_files_async(
                download_location=tmpdir,
                synapse_client=self.syn,
            )
            self.schedule_for_cleanup(manifest_path)

            assert os.path.exists(manifest_path)
            with open(manifest_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert any(
                row["ID"] == file.id for row in rows
            ), f"Expected {file.id} in manifest rows"
            file_row = next(r for r in rows if r["ID"] == file.id)
            assert file_row["path"] != ""
            assert file_row["error"] == ""
            assert os.path.exists(file_row["path"])

        # THEN the cart is empty after successful downloads
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=self.syn)

    async def test_clear_empties_cart(self, project: Synapse_Project) -> None:
        """clear() removes all files from the download list.

        Synapse returns 400 "No files available for download" when the cart is
        empty and a manifest is requested — that response IS confirmation the
        cart is empty.
        """
        await DownloadList.clear_async(synapse_client=self.syn)

        file = await self._create_test_file(project)
        await self._add_to_cart(file)

        await DownloadList.clear_async(synapse_client=self.syn)

        # Synapse returns 400 "No files available for download" when a manifest
        # is requested on an empty cart — that IS confirmation the cart is empty.
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=self.syn)

    async def test_remove_files_removes_only_specified_files(
        self, project: Synapse_Project
    ) -> None:
        """remove_files() removes only the specified file versions, not others."""
        await DownloadList.clear_async(synapse_client=self.syn)

        file_a = await self._create_test_file(project)
        file_b = await self._create_test_file(project)

        await self._add_to_cart(file_a)
        await self._add_to_cart(file_b)

        await DownloadList.remove_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file_a.id,
                    version_number=file_a.version_number,
                )
            ],
            synapse_client=self.syn,
        )

        manifest_path = await DownloadList.get_manifest_async(synapse_client=self.syn)
        self.schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            ids_in_cart = {row["ID"] for row in reader}

        await DownloadList.clear_async(synapse_client=self.syn)

        assert file_a.id not in ids_in_cart, "file_a should have been removed"
        assert file_b.id in ids_in_cart, "file_b should still be in the cart"

    async def test_remove_files_wrong_version_leaves_file_in_cart(
        self, project: Synapse_Project
    ) -> None:
        """remove_files() with the wrong version is a no-op — the file stays in the cart."""
        await DownloadList.clear_async(synapse_client=self.syn)

        file = await self._create_test_file(project)
        await self._add_to_cart(file)

        wrong_version = (file.version_number or 1) + 99

        removed = await DownloadList.remove_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file.id,
                    version_number=wrong_version,
                )
            ],
            synapse_client=self.syn,
        )

        assert removed == 0, f"Expected 0 files removed, got {removed}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=self.syn)
        self.schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            ids_in_cart = {row["ID"] for row in reader}

        await DownloadList.clear_async(synapse_client=self.syn)

        assert (
            file.id in ids_in_cart
        ), f"Expected {file.id} to remain in the cart after removing the wrong version"

    async def test_download_files_parallel_downloads_and_removes_from_cart(
        self, project: Synapse_Project
    ) -> None:
        """parallel=True downloads all files and removes them from the cart."""
        await DownloadList.clear_async(synapse_client=self.syn)

        file_a = await self._create_test_file(project)
        file_b = await self._create_test_file(project)
        await self._add_to_cart(file_a)
        await self._add_to_cart(file_b)

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await DownloadList.download_files_async(
                download_location=tmpdir,
                parallel=True,
                synapse_client=self.syn,
            )
            self.schedule_for_cleanup(manifest_path)

            assert os.path.exists(manifest_path)
            with open(manifest_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            ids_in_manifest = {row["ID"] for row in rows}
            assert file_a.id in ids_in_manifest
            assert file_b.id in ids_in_manifest

            for row in rows:
                assert (
                    row["error"] == ""
                ), f"Unexpected error for {row['ID']}: {row['error']}"
                assert os.path.exists(
                    row["path"]
                ), f"File not downloaded: {row['path']}"

        # THEN the cart is empty after successful downloads
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=self.syn)
