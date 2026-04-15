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
from synapseclient.models.table_components import CsvTableDescriptor


class TestDownloadList:
    """Integration tests for the DownloadList model.

    add_files_async:
        - add_files_multiple_files_and_versions: multiple files and versions added in one call
        - add_files_with_no_version_number: version_number=None adds latest version

    remove_files_async:
        - remove_files_removes_only_specified_files: selective version removal
        - remove_files_wrong_version_leaves_file_in_cart: wrong version is a no-op
        - remove_files_no_version_leaves_file_in_cart: omitted version does not match explicit version
        - remove_files_no_version_matches_no_version_entry: omitted version removes no-version entry

    clear_async:
        - clear_empties_cart: empties the cart

    download_files_async:
        - download_files_downloads_and_removes_from_cart: sequential and parallel download
        - download_files_multiple_versions_of_same_file: two versions both download
        - download_files_empty_cart_raises: raises SynapseHTTPError on empty cart
        - download_files_default_location: omitting download_location writes to CWD

    get_manifest_async:
        - get_manifest_with_custom_csv_descriptor: tab-separated descriptor produces TSV
    """

    async def _create_test_file(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Upload a small test file to Synapse and return the File model."""
        path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(path)
        file = File(
            parent_id=project["id"],
            path=path,
            name=f"download_list_test_{uuid.uuid4()}",
        )
        await file.store_async(synapse_client=syn)
        schedule_for_cleanup(file.id)
        return file

    async def _add_to_cart(self, file: File, syn: Synapse) -> None:
        """Add a single file to the Synapse download list cart."""
        await DownloadList.add_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file.id,
                    version_number=file.version_number,
                )
            ],
            synapse_client=syn,
        )

    async def test_add_files_multiple_files_and_versions(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """add_files_async() adds multiple files with multiple versions in a single call."""
        # GIVEN an empty cart and two files, each with two versions
        await DownloadList.clear_async(synapse_client=syn)

        file_a = await self._create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_a.path = new_path
        await file_a.store_async(synapse_client=syn)
        file_a_v2 = file_a.version_number

        file_b = await self._create_test_file(project, syn, schedule_for_cleanup)
        file_b_v1 = file_b.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_b.path = new_path
        await file_b.store_async(synapse_client=syn)
        file_b_v2 = file_b.version_number

        # WHEN I add one version of each file in one call
        count = await DownloadList.add_files_async(
            files=[
                DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
                DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
            ],
            synapse_client=syn,
        )

        # THEN the returned count is 2
        assert count == 2, f"Expected 2 files added, got {count}"

        # AND only the added versions are in the manifest
        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)
        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            cart_entries = {(row["ID"], int(row["versionNumber"])) for row in reader}
        await DownloadList.clear_async(synapse_client=syn)

        assert (
            file_a.id,
            file_a_v1,
        ) in cart_entries, f"Expected {file_a.id} v{file_a_v1} in the cart"
        assert (
            file_a.id,
            file_a_v2,
        ) not in cart_entries, f"Did not expect {file_a.id} v{file_a_v2} in the cart"
        assert (
            file_b.id,
            file_b_v1,
        ) not in cart_entries, f"Did not expect {file_b.id} v{file_b_v1} in the cart"
        assert (
            file_b.id,
            file_b_v2,
        ) in cart_entries, f"Expected {file_b.id} v{file_b_v2} in the cart"

    async def test_add_files_with_no_version_number(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """add_files_async() with version_number=None adds the latest version."""
        # GIVEN an empty cart and a file with two versions
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        v1 = file.version_number

        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file.path = new_path
        await file.store_async(synapse_client=syn)
        v2 = file.version_number
        assert v2 != v1, "Expected a new version number"

        # WHEN I add the file without specifying a version number
        count = await DownloadList.add_files_async(
            files=[
                DownloadListItem(file_entity_id=file.id),
            ],
            synapse_client=syn,
        )

        # THEN the file is added to the cart with the latest version
        assert count == 1, f"Expected 1 file added, got {count}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)
        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        await DownloadList.clear_async(synapse_client=syn)

        file_row = next(r for r in rows if r["ID"] == file.id)
        assert (
            int(file_row["versionNumber"]) == v2
        ), f"Expected latest version {v2}, got {file_row['versionNumber']}"

    async def test_remove_files_removes_only_specified_files(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """remove_files() removes only the specified file versions, not others."""
        # GIVEN two files, each with two versions
        await DownloadList.clear_async(synapse_client=syn)

        file_a = await self._create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_a.path = new_path
        await file_a.store_async(synapse_client=syn)
        file_a_v2 = file_a.version_number

        file_b = await self._create_test_file(project, syn, schedule_for_cleanup)
        file_b_v1 = file_b.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_b.path = new_path
        await file_b.store_async(synapse_client=syn)
        file_b_v2 = file_b.version_number

        # AND all four versions are in the cart
        await DownloadList.add_files_async(
            files=[
                DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
                DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v2),
                DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v1),
                DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
            ],
            synapse_client=syn,
        )

        # WHEN I remove file_a v1 and file_b v2
        removed = await DownloadList.remove_files_async(
            files=[
                DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
                DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
            ],
            synapse_client=syn,
        )

        # THEN exactly 2 items were removed
        assert removed == 2, f"Expected 2 files removed, got {removed}"

        # AND the manifest contains only file_a v2 and file_b v1
        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            cart_entries = {(row["ID"], int(row["versionNumber"])) for row in reader}

        await DownloadList.clear_async(synapse_client=syn)

        assert (file_a.id, file_a_v1) not in cart_entries, "file_a v1 should be removed"
        assert (file_a.id, file_a_v2) in cart_entries, "file_a v2 should remain"
        assert (file_b.id, file_b_v1) in cart_entries, "file_b v1 should remain"
        assert (file_b.id, file_b_v2) not in cart_entries, "file_b v2 should be removed"

    async def test_remove_files_wrong_version_leaves_file_in_cart(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """remove_files() with a wrong version is a no-op -- the file stays in the cart."""
        # GIVEN a cart with one file (added with an explicit version)
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file, syn)

        # WHEN I try to remove the file with a wrong version number
        removed = await DownloadList.remove_files_async(
            files=[
                DownloadListItem(
                    file_entity_id=file.id,
                    version_number=(file.version_number or 1) + 99,
                )
            ],
            synapse_client=syn,
        )

        # THEN no files are removed and the file remains in the cart
        assert removed == 0, f"Expected 0 files removed, got {removed}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            ids_in_cart = {row["ID"] for row in reader}

        await DownloadList.clear_async(synapse_client=syn)

        assert file.id in ids_in_cart, f"Expected {file.id} to remain in the cart"

    async def test_remove_files_no_version_leaves_file_in_cart(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """remove_files() with no version does not match a cart entry that was
        added with an explicit version -- the API requires an exact
        (fileEntityId, versionNumber) pair."""
        # GIVEN a cart with one file (added with an explicit version)
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file, syn)

        # WHEN I try to remove the file without specifying a version
        removed = await DownloadList.remove_files_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN no files are removed and the file remains in the cart
        assert removed == 0, f"Expected 0 files removed, got {removed}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            ids_in_cart = {row["ID"] for row in reader}

        await DownloadList.clear_async(synapse_client=syn)

        assert file.id in ids_in_cart, f"Expected {file.id} to remain in the cart"

    async def test_remove_files_no_version_matches_no_version_entry(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """remove_files() with no version removes a cart entry that was also
        added without a version."""
        # GIVEN a cart with one file added without a version number
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await DownloadList.add_files_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # WHEN I remove the file without specifying a version
        removed = await DownloadList.remove_files_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN the file is removed from the cart
        assert removed == 1, f"Expected 1 file removed, got {removed}"

        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=syn)

    async def test_clear_empties_cart(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """clear() removes all files from the download list.

        Synapse returns 400 "No files available for download" when the cart is
        empty and a manifest is requested — that response IS confirmation the
        cart is empty.
        """
        # GIVEN a cart with one file
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file, syn)

        # WHEN I clear the cart
        await DownloadList.clear_async(synapse_client=syn)

        # THEN requesting a manifest raises an exception because the cart is empty
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=syn)

    @pytest.mark.parametrize("parallel", [False, True])
    async def test_download_files_downloads_and_removes_from_cart(
        self,
        parallel: bool,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Downloaded files are present in the manifest and removed from cart."""
        # GIVEN an empty cart with two files added
        await DownloadList.clear_async(synapse_client=syn)
        file_a = await self._create_test_file(project, syn, schedule_for_cleanup)
        file_b = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file_a, syn)
        await self._add_to_cart(file_b, syn)

        # WHEN I download the files
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await DownloadList.download_files_async(
                download_location=tmpdir,
                parallel=parallel,
                synapse_client=syn,
            )
            schedule_for_cleanup(manifest_path)

            # THEN the manifest contains both files with valid paths and no errors
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
            await DownloadList.get_manifest_async(synapse_client=syn)

    async def test_download_files_multiple_versions_of_same_file(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Cart can hold two versions of the same file and both are downloaded."""
        # GIVEN a file with two versions, both added to an empty cart
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        v1_id = file.id
        v1_version = file.version_number

        # Upload a new version of the same file
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file.path = new_path
        await file.store_async(synapse_client=syn)
        v2_version = file.version_number
        assert v2_version != v1_version, "Expected a new version number"

        await DownloadList.add_files_async(
            files=[
                DownloadListItem(file_entity_id=v1_id, version_number=v1_version),
                DownloadListItem(file_entity_id=v1_id, version_number=v2_version),
            ],
            synapse_client=syn,
        )

        # WHEN I download the cart
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await DownloadList.download_files_async(
                download_location=tmpdir,
                synapse_client=syn,
            )
            schedule_for_cleanup(manifest_path)

            # THEN the manifest contains two rows for the same entity ID
            with open(manifest_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = [r for r in reader if r["ID"] == v1_id]

            assert len(rows) == 2, f"Expected 2 rows for {v1_id}, got {len(rows)}"
            versions_in_manifest = {int(r["versionNumber"]) for r in rows}
            assert versions_in_manifest == {
                v1_version,
                v2_version,
            }, f"Expected versions {v1_version} and {v2_version}, got {versions_in_manifest}"
            for row in rows:
                assert (
                    row["path"] != ""
                ), f"Missing path for version {row['versionNumber']}"
                assert (
                    row["error"] == ""
                ), f"Error for version {row['versionNumber']}: {row['error']}"

        # AND the cart is empty
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.get_manifest_async(synapse_client=syn)

    async def test_download_files_empty_cart_raises(
        self,
        syn: Synapse,
    ) -> None:
        """download_files_async() on an empty cart raises SynapseHTTPError."""
        # GIVEN an empty cart
        await DownloadList.clear_async(synapse_client=syn)

        # WHEN I try to download files from an empty cart
        # THEN the operation raises because the manifest job fails
        with pytest.raises(SynapseHTTPError, match="No files available for download"):
            await DownloadList.download_files_async(synapse_client=syn)

    async def test_download_files_default_location(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """download_files_async() with download_location=None writes to CWD."""
        # GIVEN an empty cart with one file added
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file, syn)

        # WHEN I download with no explicit download_location (uses CWD)
        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                manifest_path = await DownloadList.download_files_async(
                    synapse_client=syn,
                )
                schedule_for_cleanup(manifest_path)

                # THEN the manifest is written under the CWD
                abs_manifest = os.path.abspath(manifest_path)
                assert os.path.exists(abs_manifest)
                assert abs_manifest.startswith(
                    tmpdir
                ), f"Expected manifest under {tmpdir}, got {abs_manifest}"

                with open(manifest_path, newline="") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)

                file_row = next(r for r in rows if r["ID"] == file.id)
                assert file_row["error"] == ""
            finally:
                os.chdir(original_cwd)

    async def test_get_manifest_with_custom_csv_descriptor(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """get_manifest_async() respects a custom CsvTableDescriptor."""
        # GIVEN an empty cart with one file added
        await DownloadList.clear_async(synapse_client=syn)
        file = await self._create_test_file(project, syn, schedule_for_cleanup)
        await self._add_to_cart(file, syn)

        # WHEN I request a manifest with a tab separator
        descriptor = CsvTableDescriptor(separator="\t")
        manifest_path = await DownloadList.get_manifest_async(
            csv_table_descriptor=descriptor,
            synapse_client=syn,
        )
        schedule_for_cleanup(manifest_path)

        # THEN the downloaded CSV uses tabs as the delimiter
        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = list(reader)

        await DownloadList.clear_async(synapse_client=syn)

        assert len(rows) >= 1, "Expected at least one row in the manifest"
        assert any(
            row["ID"] == file.id for row in rows
        ), f"Expected {file.id} in tab-separated manifest"
