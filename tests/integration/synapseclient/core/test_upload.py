"""Integration tests for uploading files, focused on avoiding redundant re-uploads via the local disk cache and MD5 comparison."""

import os
import shutil
import sys
import tempfile
import uuid
from typing import Callable
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.models import File, Project

DESCRIPTION = "This is an example file."
CONTENT_TYPE = "text/plain"
VERSION_COMMENT = "My version comment"


class TestFileStoreAvoidsRedundantUpload:
    """
    Tests that File.store_async avoids re-uploading file content that has
    not changed, even when the local path no longer matches what's recorded
    in the local disk cache (renamed, moved, or re-cased). In those cases
    `Cache.contains()` (synapseclient/core/cache.py) misses because it only
    matches by path, and it's the MD5 comparison against what Synapse
    already has on record (`_needs_upload` in synapseclient/models/file.py)
    that actually prevents the redundant upload.
    """

    async def test_reupload_after_local_rename_same_content(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # Given a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the local file is renamed, with no change to its content
        renamed_path = os.path.join(
            os.path.dirname(original_path), f"renamed_{uuid.uuid4()}.txt"
        )
        os.rename(original_path, renamed_path)
        schedule_for_cleanup(renamed_path)
        file.path = renamed_path

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN the already-uploaded file handle is reused
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the file handle's saved filename still reflects the original
        # upload, not the local rename -- Synapse never saw the new name
        assert new_file.file_handle.file_name == before_file_handle_file_name

    async def test_reupload_from_different_directory_same_content(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # Given a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the same content is re-stored from a copy of the file in a different directory
        other_dir = tempfile.mkdtemp()
        schedule_for_cleanup(other_dir)
        copied_path = os.path.join(other_dir, os.path.basename(original_path))
        shutil.copyfile(original_path, copied_path)
        file.path = copied_path

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN no redundant upload occurs because the MD5 match
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the file handle's saved filename is unchanged
        assert new_file.file_handle.file_name == before_file_handle_file_name

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason=(
            "Windows filesystems are case-insensitive, so os.path.normcase "
            "folds this casing-only rename into an exact cache hit instead "
            "of exercising the POSIX MD5-fallback path this test targets."
        ),
    )
    async def test_reupload_after_casing_change_same_content(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # Given a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the local file name is re-cased
        recased_path = os.path.join(
            os.path.dirname(original_path),
            os.path.basename(original_path).upper(),
        )
        os.rename(original_path, recased_path)
        schedule_for_cleanup(recased_path)
        file.path = recased_path

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN the MD5 comparison against Synapse's record avoids a redundant upload
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the saved filename keeps its original casing -- the re-cased
        # local name was never uploaded, so Synapse's record didn't change
        assert new_file.file_handle.file_name == before_file_handle_file_name

    async def test_reupload_after_move_and_rename_same_content(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the file is both moved to a new directory AND renamed
        other_dir = tempfile.mkdtemp()
        schedule_for_cleanup(other_dir)
        new_path = os.path.join(other_dir, f"moved_and_renamed_{uuid.uuid4()}.txt")
        shutil.move(original_path, new_path)
        file.path = new_path

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN no redundant upload occurs because the MD5 match
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the file handle's saved filename is unchanged
        assert new_file.file_handle.file_name == before_file_handle_file_name

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason=(
            "Windows filesystems are case-insensitive, so os.path.normcase "
            "folds this casing-only rename into an exact cache hit instead "
            "of exercising the POSIX MD5-fallback path this test targets."
        ),
    )
    async def test_reupload_after_casing_change_and_move_same_content(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the file is moved to a new directory AND its name is re-cased
        other_dir = tempfile.mkdtemp()
        schedule_for_cleanup(other_dir)
        new_path = os.path.join(other_dir, os.path.basename(original_path).upper())
        shutil.move(original_path, new_path)
        file.path = new_path

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN no redundant upload occurs
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the saved filename keeps its original casing
        assert new_file.file_handle.file_name == before_file_handle_file_name

    async def test_reupload_after_rename_with_force_version_false(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """
        force_version has no bearing on whether _needs_upload decides a
        re-upload is needed -- it only affects whether an actual content or
        metadata change is written as a new version. Confirms a rename with
        unchanged content is just as much a no-op with force_version=False
        as it is with the default force_version=True.
        """
        # GIVEN a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the local file is renamed, with no change to its content,
        # and force_version is explicitly disabled
        renamed_path = os.path.join(
            os.path.dirname(original_path), f"renamed_{uuid.uuid4()}.txt"
        )
        os.rename(original_path, renamed_path)
        schedule_for_cleanup(renamed_path)
        file.path = renamed_path
        file.force_version = False

        with patch(
            "synapseclient.models.file.upload_file_handle"
        ) as mocked_upload_file_handle:
            new_file = await file.store_async(synapse_client=syn)

        # THEN no redundant upload occurs
        assert not mocked_upload_file_handle.called
        assert new_file.file_handle.id == before_file_handle_id
        assert new_file.version_number == before_version_number
        # AND the file handle's saved filename is unchanged
        assert new_file.file_handle.file_name == before_file_handle_file_name

    async def test_reupload_after_rename_cache_self_heals(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
        mocker: MockerFixture,
    ) -> None:
        """
        Verifies the actual mechanism behind
        test_reupload_after_local_rename_same_content
        """
        # Given a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_file_name = file.file_handle.file_name

        # WHEN the local file is renamed, with no change to its content
        renamed_path = os.path.join(
            os.path.dirname(original_path), f"renamed_{uuid.uuid4()}.txt"
        )
        os.rename(original_path, renamed_path)
        schedule_for_cleanup(renamed_path)
        file.path = renamed_path

        spy_contains = mocker.spy(syn.cache, "contains")
        spy_add = mocker.spy(syn.cache, "add")
        new_file = await file.store_async(synapse_client=syn)

        # THEN the renamed path was a genuine miss in the local disk cache...
        spy_contains.assert_called_once_with(new_file.file_handle.id, renamed_path)
        assert spy_contains.spy_return is False

        # _needs_upload self-healed the cache under the new path
        spy_add.assert_called_once()
        _, add_kwargs = spy_add.call_args
        assert add_kwargs["file_handle_id"] == new_file.file_handle.id
        assert add_kwargs["path"] == renamed_path
        # AND the MD5-fallback match left the saved filename unchanged
        assert new_file.file_handle.file_name == before_file_handle_file_name

        # check the updated cache
        spy_contains.reset_mock()
        spy_add.reset_mock()
        file_after_cache_hit = await file.store_async(synapse_client=syn)
        spy_contains.assert_called_once_with(
            file_after_cache_hit.file_handle.id, renamed_path
        )
        assert spy_contains.spy_return
        spy_add.assert_not_called()
        # AND the fast cache-hit path also leaves the saved filename unchanged
        assert (
            file_after_cache_hit.file_handle.file_name == before_file_handle_file_name
        )

    async def test_reupload_after_rename_with_changed_content_creates_new_version(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """
        Confirms only a content change (not just rename) triggers re-upload/version.
        """
        # GIVEN a file stored in Synapse
        original_path = utils.make_bogus_uuid_file()
        schedule_for_cleanup(original_path)
        file = await File(
            path=original_path,
            description=DESCRIPTION,
            content_type=CONTENT_TYPE,
            version_comment=VERSION_COMMENT,
            version_label=str(uuid.uuid4()),
        ).store_async(parent=project_model, synapse_client=syn)
        schedule_for_cleanup(file.id)
        before_file_handle_id = file.file_handle.id
        before_version_number = file.version_number

        # WHEN the local file is renamed AND its content is changed
        renamed_path = os.path.join(
            os.path.dirname(original_path), f"renamed_{uuid.uuid4()}.txt"
        )
        os.rename(original_path, renamed_path)
        schedule_for_cleanup(renamed_path)
        with open(renamed_path, "w") as f:
            f.write("this content is different from what was uploaded")
        file.path = renamed_path

        new_file = await file.store_async(synapse_client=syn)

        # THEN a new file handle and version are created
        assert new_file.file_handle.id != before_file_handle_id
        assert new_file.version_number > before_version_number
