"""Integration tests for download_list operation functions.

The Synapse download list is a user-scoped resource: every test run against
the same Synapse account shares one cart. To coexist with other tests and
concurrent CI runs, these tests track the items they add and remove only
those items on teardown, instead of calling download_list_clear_async() as a global
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
from synapseclient import Project, Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import File
from synapseclient.models.table_components import CsvTableDescriptor
from synapseclient.operations import (
    DownloadListItem,
    download_list_add_async,
    download_list_files_async,
    download_list_manifest_async,
    download_list_remove_async,
)


@pytest_asyncio.fixture
async def scheduled_for_cart_removal(syn: Synapse):
    """Track items a test adds to the cart and remove only those items on teardown."""
    scheduled: list[DownloadListItem] = []
    yield scheduled
    if scheduled:
        try:
            await download_list_remove_async(files=scheduled, synapse_client=syn)
        except Exception as e:
            pytest.fail(
                f"Cart teardown failed — {len(scheduled)} item(s) may remain in "
                f"the cart and affect subsequent tests: {e}"
            )


async def _create_test_file(
    project: Project,
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


async def _upload_new_version(
    file: File,
    syn: Synapse,
    schedule_for_cleanup: Callable[..., None],
) -> int:
    """Upload a new version of an existing file and return the new version number."""
    new_path = utils.make_bogus_uuid_file()
    schedule_for_cleanup(new_path)
    file.path = new_path
    await file.store_async(synapse_client=syn)
    return file.version_number


async def _add_to_cart(
    file: File,
    syn: Synapse,
    scheduled_for_cart_removal: list[DownloadListItem],
) -> None:
    """Add a single file to the Synapse download list cart and register it
    for teardown removal."""
    item = DownloadListItem(
        file_entity_id=file.id,
        version_number=file.version_number,
    )
    await download_list_add_async(files=[item], synapse_client=syn)
    scheduled_for_cart_removal.append(item)


async def _cart_entries(
    syn: Synapse,
    schedule_for_cleanup: Callable[..., None],
) -> set[tuple[str, int]]:
    """Return all (file_id, version_number) pairs currently in the user's cart.

    Returns an empty set when the cart is empty. Synapse returns HTTP 400 with
    the message 'No files available for download' in that case rather than
    producing an empty CSV. If this string changes server-side, update it here
    and in download_list_files_async's documented 'Raises' section.
    See POST /download/list/manifest/async/start in the Synapse REST docs
    (DownloadListController).
    """
    try:
        manifest_path = await download_list_manifest_async(synapse_client=syn)
    except SynapseHTTPError as e:
        if "No files available for download" in str(e):
            return set()
        raise
    schedule_for_cleanup(manifest_path)
    with open(manifest_path, newline="") as f:
        return {(row["ID"], int(row["versionNumber"])) for row in csv.DictReader(f)}


class TestDownloadListAddAsync:
    """Integration tests for download_list_add_async.

    - test_adds_specific_version_of_each_file_in_one_call: multiple files and versions added in one call
    - test_download_list_add_with_no_version_number: version_number=None adds latest version
    """

    async def test_adds_specific_version_of_each_file_in_one_call(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_add_async() adds multiple files with multiple versions in a single call."""
        # GIVEN two files, each with two versions; we'll select v1 of file_a and v2 of file_b
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        await _upload_new_version(file_a, syn, schedule_for_cleanup)

        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        await _upload_new_version(file_b, syn, schedule_for_cleanup)
        file_b_v2 = file_b.version_number

        # WHEN I add file_a v1 and file_b v2 in one call
        items = [
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
        ]
        count = await download_list_add_async(files=items, synapse_client=syn)
        scheduled_for_cart_removal.extend(items)
        cart_entries = {
            e
            for e in await _cart_entries(syn, schedule_for_cleanup)
            if e[0] in {file_a.id, file_b.id}
        }

        # THEN the returned count is 2
        assert count == 2, f"Expected 2 files added, got {count}"

        # AND only the added versions appear in the manifest for these file ids
        assert cart_entries == {
            (file_a.id, file_a_v1),
            (file_b.id, file_b_v2),
        }, f"Unexpected cart contents for test files: {cart_entries}"

    async def test_download_list_add_with_no_version_number(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_add_async() with version_number=None adds the latest version."""
        # GIVEN a file with two versions
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        v1 = file.version_number
        v2 = await _upload_new_version(file, syn, schedule_for_cleanup)
        assert v2 != v1, "Expected a new version number"

        # WHEN I add the file without specifying a version number
        item_no_version = DownloadListItem(file_entity_id=file.id)
        count = await download_list_add_async(
            files=[item_no_version], synapse_client=syn
        )
        scheduled_for_cart_removal.append(item_no_version)
        cart_entries = {
            e for e in await _cart_entries(syn, schedule_for_cleanup) if e[0] == file.id
        }

        # THEN the file is added to the cart with the latest version
        assert count == 1, f"Expected 1 file added, got {count}"

        # AND the file appears in the manifest at the latest version
        assert cart_entries == {
            (file.id, v2)
        }, f"Expected one row for {file.id} at v{v2}, got {cart_entries}"


class TestDownloadListRemoveAsync:
    """Integration tests for download_list_remove_async.

    - test_download_list_remove_removes_only_specified_files: selective version removal
    - test_download_list_remove_wrong_version_leaves_file_in_cart: wrong version is a no-op
    - test_download_list_remove_no_version_leaves_file_in_cart: omitted version does not match explicit version
    - test_download_list_remove_no_version_matches_no_version_entry: omitted version removes no-version entry
    """

    async def test_download_list_remove_removes_only_specified_files(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_remove_async() removes only the specified file versions, not others."""
        # GIVEN two files, each with two versions
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_a_v1 = file_a.version_number
        file_a_v2 = await _upload_new_version(file_a, syn, schedule_for_cleanup)

        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        file_b_v1 = file_b.version_number
        file_b_v2 = await _upload_new_version(file_b, syn, schedule_for_cleanup)

        # AND all four versions are added to the cart
        added = [
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
            DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v2),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v1),
            DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
        ]
        await download_list_add_async(files=added, synapse_client=syn)
        scheduled_for_cart_removal.extend(added)

        # WHEN I remove file_a v1 and file_b v2
        removed = await download_list_remove_async(
            files=[
                DownloadListItem(file_entity_id=file_a.id, version_number=file_a_v1),
                DownloadListItem(file_entity_id=file_b.id, version_number=file_b_v2),
            ],
            synapse_client=syn,
        )
        our_ids = {file_a.id, file_b.id}
        cart_entries = {
            e for e in await _cart_entries(syn, schedule_for_cleanup) if e[0] in our_ids
        }

        # THEN exactly 2 items were removed
        assert removed == 2, f"Expected 2 files removed, got {removed}"

        # AND the manifest (filtered to our file ids) contains only file_a v2 and file_b v1
        assert cart_entries == {
            (file_a.id, file_a_v2),
            (file_b.id, file_b_v1),
        }, f"Unexpected cart contents for test files: {cart_entries}"

    async def test_download_list_remove_wrong_version_leaves_file_in_cart(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_remove_async() with a wrong version is a no-op -- the file stays in the cart."""
        # GIVEN a cart entry for a file (added with an explicit version)
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I try to remove the file with a wrong version number
        removed = await download_list_remove_async(
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
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert file.id in cart_ids, f"Expected {file.id} to remain in the cart"

    async def test_download_list_remove_no_version_leaves_file_in_cart(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_remove_async() with no version does not match a cart entry that was
        added with an explicit version -- the API requires an exact
        (fileEntityId, versionNumber) pair."""
        # GIVEN a cart entry for a file (added with an explicit version)
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I try to remove the file without specifying a version
        removed = await download_list_remove_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN no files are removed and the file remains in the cart
        assert removed == 0, f"Expected 0 files removed, got {removed}"
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert file.id in cart_ids, f"Expected {file.id} to remain in the cart"

    async def test_download_list_remove_no_version_matches_no_version_entry(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_remove_async() with no version removes a cart entry that was also
        added without a version."""
        # GIVEN a cart entry for a file added without a version number
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        item_no_version = DownloadListItem(file_entity_id=file.id)
        await download_list_add_async(files=[item_no_version], synapse_client=syn)
        scheduled_for_cart_removal.append(item_no_version)

        # WHEN I remove the file without specifying a version
        removed = await download_list_remove_async(
            files=[DownloadListItem(file_entity_id=file.id)],
            synapse_client=syn,
        )

        # THEN the file is reported as removed and no longer appears in the cart
        assert removed == 1, f"Expected 1 file removed, got {removed}"
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert file.id not in cart_ids, f"Expected {file.id} to be absent from the cart"


class TestDownloadListFilesAsync:
    """Integration tests for download_list_files_async.

    - test_download_list_files_downloads_and_removes_from_cart: sequential and parallel download
    - test_download_list_files_multiple_versions_of_same_file: two versions both download
    - test_download_list_files_default_location: omitting download_location writes to CWD
    - test_download_list_files_no_version_add_is_removed_from_cart:
      no-version add is downloaded and removed from the cart
    """

    @pytest.mark.parametrize("parallel", [False, True])
    async def test_download_list_files_downloads_and_removes_from_cart(
        self,
        parallel: bool,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """Downloaded files are present in the manifest and removed from cart."""
        # GIVEN two files added to the cart
        file_a = await _create_test_file(project, syn, schedule_for_cleanup)
        file_b = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file_a, syn, scheduled_for_cart_removal)
        await _add_to_cart(file_b, syn, scheduled_for_cart_removal)

        # WHEN I download the files
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await download_list_files_async(
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
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert (
            file_a.id not in cart_ids
        ), f"Expected {file_a.id} to be removed from cart after download"
        assert (
            file_b.id not in cart_ids
        ), f"Expected {file_b.id} to be removed from cart after download"

    async def test_download_list_files_multiple_versions_of_same_file(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """Cart can hold two versions of the same file and both are downloaded."""
        # GIVEN a file with two versions, both added to the cart
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        v1_id = file.id
        v1_version = file.version_number
        v2_version = await _upload_new_version(file, syn, schedule_for_cleanup)
        assert v2_version != v1_version, "Expected a new version number"

        items = [
            DownloadListItem(file_entity_id=v1_id, version_number=v1_version),
            DownloadListItem(file_entity_id=v1_id, version_number=v2_version),
        ]
        await download_list_add_async(files=items, synapse_client=syn)
        scheduled_for_cart_removal.extend(items)

        # WHEN I download the cart
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await download_list_files_async(
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
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert (
            v1_id not in cart_ids
        ), f"Expected {v1_id} to be removed from cart after download"

    async def test_download_list_files_default_location(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_files_async() with download_location=None writes to CWD."""
        # GIVEN a cart containing one of our files
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I download with no explicit download_location (uses CWD)
        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                manifest_path = await download_list_files_async(
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

    async def test_download_list_files_no_version_add_is_removed_from_cart(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """A file added to the cart without a version is downloaded
        successfully and removed from the cart.
        """
        # GIVEN a file added to the cart without a version number
        file = await _create_test_file(project, syn, schedule_for_cleanup)
        item_no_version = DownloadListItem(file_entity_id=file.id)
        await download_list_add_async(files=[item_no_version], synapse_client=syn)
        scheduled_for_cart_removal.append(item_no_version)

        # WHEN I download the cart contents
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = await download_list_files_async(
                download_location=tmpdir,
                synapse_client=syn,
            )
            schedule_for_cleanup(manifest_path)

            # THEN the file is downloaded successfully (no error in the manifest)
            with open(manifest_path, newline="") as f:
                rows = [r for r in csv.DictReader(f) if r["ID"] == file.id]
            assert len(rows) == 1, f"Expected 1 row for {file.id}, got {len(rows)}"
            assert (
                rows[0]["error"] == ""
            ), f"Unexpected error for {file.id}: {rows[0]['error']}"
            assert os.path.exists(
                rows[0]["path"]
            ), f"File not downloaded: {rows[0]['path']}"

        # AND the file is removed from the cart after a successful download,
        # even though it was added without a version number
        cart_ids = {id_ for id_, _ in await _cart_entries(syn, schedule_for_cleanup)}
        assert (
            file.id not in cart_ids
        ), f"Expected {file.id} to be removed from cart after download."


class TestDownloadListManifestAsync:
    """Integration tests for download_list_manifest_async."""

    async def test_download_list_manifest_with_custom_csv_descriptor(
        self,
        project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        scheduled_for_cart_removal: list[DownloadListItem],
    ) -> None:
        """download_list_manifest_async() respects a custom CsvTableDescriptor."""
        # GIVEN a cart containing a file whose name contains the quote
        # character, so the writer must emit the escape character
        path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(path)
        uuid_suffix = str(uuid.uuid4())
        file_name = f"it's_{uuid_suffix}"
        file = File(
            parent_id=project["id"],
            path=path,
            name=file_name,
        )
        file = await file.store_async(synapse_client=syn)
        schedule_for_cleanup(file.id)
        await _add_to_cart(file, syn, scheduled_for_cart_removal)

        # WHEN I request a manifest with all non-default descriptor options
        descriptor = CsvTableDescriptor(
            separator="\t",
            quote_character="'",
            escape_character="/",
            line_end="\n",
            is_first_line_header=False,
        )
        manifest_path = await download_list_manifest_async(
            csv_table_descriptor=descriptor,
            synapse_client=syn,
        )
        schedule_for_cleanup(manifest_path)

        with open(manifest_path, newline="") as f:
            content = f.read()

        # THEN tab separator is used
        assert "\t" in content, "Expected tab separators in manifest"

        # AND the escape character was used for the embedded quote in the file name
        assert "/" in content, (
            f"Expected escape sequence /' in manifest, " f"got: {content!r}"
        )

        # AND the embedded quote in the file name was escaped by doubling
        assert "''" in content, (
            f"Expected doubled quote '' in manifest (from escaping ' in file name), "
            f"got: {content!r}"
        )

        # AND line endings are LF only (no CR)
        assert "\r" not in content, "Expected LF-only line endings; found CR"

        # AND there is no header row -- the first non-empty line is the data row
        # NOTE: The cart is per-user and shared across all parallel workers (-n 8).
        # Other tests running concurrently can add items to the cart, so the manifest
        # may contain more than just this test's file.
        lines = [line for line in content.split("\n") if line]
        assert lines, "Expected at least one row in the manifest"
        assert any(file.id in line for line in lines), (
            f"Expected a data row containing {file.id} in manifest, "
            f"got: {content!r}"
        )

        # AND the name field is wrapped in single quotes (the writer quoted it
        # because it contains the quote character).
        # Search all lines since the cart may contain other items from concurrent tests.
        file_line = next((line for line in lines if file.id in line), None)
        assert file_line is not None, f"No line found for {file.id} in manifest"
        fields = file_line.split("\t")
        name_field = next((f for f in fields if uuid_suffix in f), None)
        assert (
            name_field is not None
        ), f"Name field containing {uuid_suffix!r} not found in {file_line!r}"
