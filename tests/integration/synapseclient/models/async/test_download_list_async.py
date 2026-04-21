"""Integration tests for DownloadList OOP model.

The Synapse download list is a user-scoped resource: every test run against
the same Synapse account shares one cart. To coexist with other tests and
concurrent CI runs, these tests track the items they add and remove only
those items on teardown, instead of calling clear_async() as a global
reset. Assertions reason only about the test's own file ids, never about
the cart being globally empty.
"""

import csv
import os
import tempfile
import uuid
from typing import Callable

import pytest
import pytest_asyncio

import synapseclient.core.utils as utils
from synapseclient import Project as Synapse_Project
from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import DownloadList, DownloadListItem, File
from synapseclient.models.table_components import CsvTableDescriptor


@pytest_asyncio.fixture
async def scheduled_for_cart_removal(syn: Synapse):
    """Track items a test adds to the cart and remove only those items on teardown."""
    scheduled: list[DownloadListItem] = []
    yield scheduled
    if scheduled:
        await DownloadList.remove_files_async(files=scheduled, synapse_client=syn)


async def _create_test_file(
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


async def _add_to_cart(
    file: File,
    syn: Synapse,
    scheduled_for_cart_removal: list,
) -> None:
    """Add a single file to the Synapse download list cart and register it
    for teardown removal."""
    item = DownloadListItem(
        file_entity_id=file.id,
        version_number=file.version_number,
    )
    await DownloadList.add_files_async(files=[item], synapse_client=syn)
    scheduled_for_cart_removal.append(item)


async def _cart_file_ids(
    syn: Synapse,
    schedule_for_cleanup: Callable[..., None],
) -> set[str]:
    """Return the set of file ids currently in the user's cart.

    An empty cart (which makes get_manifest_async raise with
    'No files available for download') returns an empty set.
    """
    try:
        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
    except SynapseHTTPError as e:
        # Synapse returns HTTP 400 with this exact message when the cart is
        # empty: the manifest async job fails rather than producing an empty
        # CSV. If this string changes server-side, update it here and in
        # DownloadList.download_files_async's documented "Raises" section.
        # See POST /download/list/manifest/async/start in the Synapse REST
        # docs (DownloadListController).
        if "No files available for download" in str(e):
            return set()
        raise
    schedule_for_cleanup(manifest_path)
    with open(manifest_path, newline="") as f:
        return {row["ID"] for row in csv.DictReader(f)}


class TestAddFilesAsync:
    """Integration tests for DownloadList.add_files_async.

    - test_add_files_multiple_files_and_versions: multiple files and versions added in one call
    - test_add_files_with_no_version_number: version_number=None adds latest version
    """

    async def test_add_files_multiple_files_and_versions(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """add_files_async() adds multiple files with multiple versions in a single call."""
        # GIVEN two files, each with two versions
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_a.path = new_path
        await file_a.store_async(synapse_client=syn)

        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_b.path = new_path
        await file_b.store_async(synapse_client=syn)
        file_b_v2 = file_b.version_number

        # WHEN I add one version of each file in one call
        items = [
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
        ]
        count = await DownloadList.add_files_async(files=items, synapse_client=syn)
        scheduled_for_cart_removal.extend(items)

        # THEN the returned count is 2
        assert count == 2, f"Expected 2 files added, got {count}"

        # AND only the added versions appear in the manifest for these file ids
        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)
        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            cart_entries = {
                (row["ID"], int(row["versionNumber"]))
                for row in reader
                if row["ID"] in {file_a.id, file_b.id}
            }

        assert cart_entries == {
            (file_a.id, file_a_v1),
            (file_b.id, file_b_v2),
        }, f"Unexpected cart contents for test files: {cart_entries}"

    async def test_add_files_with_no_version_number(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """add_files_async() with version_number=None adds the latest version."""
        # GIVEN a file with two versions
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        v1 = file.version_number

        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file.path = new_path
        await file.store_async(synapse_client=syn)
        v2 = file.version_number
        assert v2 != v1, "Expected a new version number"

        # WHEN I add the file without specifying a version number
        item_no_version = DownloadListItem(file_entity_id=file.id)
        count = await DownloadList.add_files_async(
            files=[item_no_version], synapse_client=syn
        )
        scheduled_for_cart_removal.append(item_no_version)

        # THEN the file is added to the cart with the latest version
        assert count == 1, f"Expected 1 file added, got {count}"

        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)
        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r["ID"] == file.id]

        assert len(rows) == 1, f"Expected one row for {file.id}, got {len(rows)}"
        assert (
            int(rows[0]["versionNumber"]) == v2
        ), f"Expected latest version {v2}, got {rows[0]['versionNumber']}"


class TestRemoveFilesAsync:
    """Integration tests for DownloadList.remove_files_async.

    - test_remove_files_removes_only_specified_files: selective version removal
    - test_remove_files_wrong_version_leaves_file_in_cart: wrong version is a no-op
    - test_remove_files_no_version_leaves_file_in_cart: omitted version does not match explicit version
    - test_remove_files_no_version_matches_no_version_entry: omitted version removes no-version entry
    """

    async def test_remove_files_removes_only_specified_files(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """remove_files() removes only the specified file versions, not others."""
        # GIVEN two files, each with two versions
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_a.path = new_path
        await file_a.store_async(synapse_client=syn)
        file_a_v2 = file_a.version_number

        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        file_b_v1 = file_b.version_number
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file_b.path = new_path
        await file_b.store_async(synapse_client=syn)
        file_b_v2 = file_b.version_number

        # AND all four versions are added to the cart
        added = [
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v2),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v1),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
        ]
        await DownloadList.add_files_async(files=added, synapse_client=syn)
        scheduled_for_cart_removal.extend(added)

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

        # AND the manifest (filtered to our file ids) contains only file_a v2 and file_b v1
        manifest_path = await DownloadList.get_manifest_async(synapse_client=syn)
        schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            reader = csv.DictReader(f)
            cart_entries = {
                (row["ID"], int(row["versionNumber"]))
                for row in reader
                if row["ID"] in {file_a.id, file_b.id}
            }

        assert cart_entries == {
            (file_a.id, file_a_v2),
            (file_b.id, file_b_v1),
        }, f"Unexpected cart contents for test files: {cart_entries}"

    async def test_remove_files_wrong_version_leaves_file_in_cart(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """remove_files() with a wrong version is a no-op -- the file stays in the cart."""
        # GIVEN a cart entry for a file (added with an explicit version)
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

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
        cart_ids = await _cart_file_ids(syn, schedule_for_cleanup)
        assert file.id in cart_ids, f"Expected {file.id} to remain in the cart"

    async def test_remove_files_no_version_leaves_file_in_cart(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """remove_files() with no version does not match a cart entry that was
        added with an explicit version -- the API requires an exact
        (fileEntityId, versionNumber) pair."""
        # GIVEN a cart entry for a file (added with an explicit version)
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I try to remove the file without specifying a version
        removed = await DownloadList.remove_files_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN no files are removed and the file remains in the cart
        assert removed == 0, f"Expected 0 files removed, got {removed}"
        cart_ids = await _cart_file_ids(syn, schedule_for_cleanup)
        assert file.id in cart_ids, f"Expected {file.id} to remain in the cart"

    async def test_remove_files_no_version_matches_no_version_entry(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """remove_files() with no version removes a cart entry that was also
        added without a version."""
        # GIVEN a cart entry for a file added without a version number
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        item_no_version = DownloadListItem(file_entity_id=file.id)
        await DownloadList.add_files_async(files=[item_no_version], synapse_client=syn)
        scheduled_for_cart_removal.append(item_no_version)

        # WHEN I remove the file without specifying a version
        removed = await DownloadList.remove_files_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN the file is reported as removed and no longer appears in the cart
        assert removed == 1, f"Expected 1 file removed, got {removed}"
        cart_ids = await _cart_file_ids(syn, schedule_for_cleanup)
        assert (
            file.id not in cart_ids
        ), f"Expected {file.id} to be absent from the cart"


class TestDownloadFilesAsync:
    """Integration tests for DownloadList.download_files_async.

    - test_download_files_downloads_and_removes_from_cart: sequential and parallel download
    - test_download_files_multiple_versions_of_same_file: two versions both download
    - test_download_files_default_location: omitting download_location writes to CWD
    """

    @pytest.mark.parametrize("parallel", [False, True])
    async def test_download_files_downloads_and_removes_from_cart(
        self,
        parallel: bool,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """Downloaded files are present in the manifest and removed from cart."""
        # GIVEN two files added to the cart
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file_a, syn, scheduled_for_cart_removal)
        await _add_to_cart(file_b, syn, scheduled_for_cart_removal)

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

            my_rows = [r for r in rows if r["ID"] in {file_a.id, file_b.id}]
            ids_in_manifest = {row["ID"] for row in my_rows}
            assert file_a.id in ids_in_manifest
            assert file_b.id in ids_in_manifest

            for row in my_rows:
                assert (
                    row["error"] == ""
                ), f"Unexpected error for {row['ID']}: {row['error']}"
                assert os.path.exists(
                    row["path"]
                ), f"File not downloaded: {row['path']}"

        # AND our files are no longer in the cart after successful downloads
        cart_ids = await _cart_file_ids(syn, schedule_for_cleanup)
        assert (
            file_a.id not in cart_ids
        ), f"Expected {file_a.id} to be removed from cart after download"
        assert (
            file_b.id not in cart_ids
        ), f"Expected {file_b.id} to be removed from cart after download"

    async def test_download_files_multiple_versions_of_same_file(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """Cart can hold two versions of the same file and both are downloaded."""
        # GIVEN a file with two versions, both added to the cart
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        v1_id = file.id
        v1_version = file.version_number

        # Upload a new version of the same file
        new_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(new_path)
        file.path = new_path
        await file.store_async(synapse_client=syn)
        v2_version = file.version_number
        assert v2_version != v1_version, "Expected a new version number"

        items = [
            DownloadListItem(file_entity_id=v1_id, version_number=v1_version),
            DownloadListItem(file_entity_id=v1_id, version_number=v2_version),
        ]
        await DownloadList.add_files_async(files=items, synapse_client=syn)
        scheduled_for_cart_removal.extend(items)

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

        # AND our file is no longer in the cart
        cart_ids = await _cart_file_ids(syn, schedule_for_cleanup)
        assert (
            v1_id not in cart_ids
        ), f"Expected {v1_id} to be removed from cart after download"

    async def test_download_files_default_location(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """download_files_async() with download_location=None writes to CWD."""
        # GIVEN a cart containing one of our files
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

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
                # Normalize both paths with realpath -- on macOS /var is a
                # symlink to /private/var, so tmpdir and the resolved manifest
                # path can differ even when the manifest is under tmpdir.
                assert os.path.realpath(abs_manifest).startswith(
                    os.path.realpath(tmpdir)
                ), f"Expected manifest under {tmpdir}, got {abs_manifest}"

                with open(manifest_path, newline="") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)

                file_row = next(r for r in rows if r["ID"] == file.id)
                assert file_row["error"] == ""
            finally:
                os.chdir(original_cwd)


class TestGetManifestAsync:
    """Integration tests for DownloadList.get_manifest_async.

    - test_get_manifest_with_custom_csv_descriptor: tab-separated descriptor produces TSV
    """

    async def test_get_manifest_with_custom_csv_descriptor(
        self,
        project: Synapse_Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list,
    ) -> None:
        """get_manifest_async() respects a custom CsvTableDescriptor."""
        # GIVEN a cart containing one of our files
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I request a manifest with all non-default descriptor options
        descriptor = CsvTableDescriptor(
            separator="\t",
            quote_character="'",
            escape_character="/",
            line_end="\n",
            is_first_line_header=False,
        )
        manifest_path = await DownloadList.get_manifest_async(
            csv_table_descriptor=descriptor,
            synapse_client=syn,
        )
        schedule_for_cleanup(manifest_path)

        # THEN the downloaded file uses the custom descriptor settings
        with open(manifest_path, newline="") as f:
            content = f.read()

        # AND tab separator is used
        assert "\t" in content, "Expected tab separators in manifest"

        # AND lines end with \n (not \r\n or other)
        lines = content.split("\n")
        lines = [line for line in lines if line]

        # AND our file appears in the raw content (no header row)
        assert len(lines) >= 1, "Expected at least one row in the manifest"
        assert any(
            file.id in line for line in lines
        ), f"Expected {file.id} in manifest content"

        # AND the single-quote character is used for quoting
        assert (
            '"' not in content
        ), "Expected single-quote quoting, but found double quotes in manifest"
