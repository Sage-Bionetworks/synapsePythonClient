"""Integration tests around downloading files from Synapse."""

import filecmp
import os
import shutil
import tempfile
from typing import Callable
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

import synapseclient
import synapseclient.core.utils as utils
from synapseclient import Synapse, client
from synapseclient.core.download import download_from_url, download_functions
from synapseclient.core.exceptions import SynapseMd5MismatchError
from synapseclient.models import File, Project


class TestDownloadCollisions:
    """Tests for downloading files with different collision states."""

    async def test_collision_overwrite_local(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # Spys
        spy_file_handle = mocker.spy(download_functions, "download_by_file_handle")
        spy_file_entity = mocker.spy(client, "download_file_entity")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        original_file_md5 = file.file_handle.content_md5
        schedule_for_cleanup(original_file_path)
        schedule_for_cleanup(file.id)

        # AND the file on disk is different from the file in synapse
        with open(original_file_path, "w", encoding="utf-8") as f:
            f.write("Different data")
        assert original_file_md5 != utils.md5_for_file_hex(original_file_path)

        # AND the file is not in the cache
        syn.cache.remove(file_handle_id=file.file_handle.id)

        # WHEN I download the file
        file = await File(
            id=file.id,
            if_collision="overwrite.local",
            download_location=os.path.dirname(original_file_path),
        ).get_async()

        # THEN the file is downloaded replacing the file on disk
        assert original_file_path == file.path
        assert original_file_md5 == utils.md5_for_file_hex(original_file_path)

        # AND download_by_file_handle was called
        spy_file_handle.assert_called_once()

        # AND file_entity was called
        spy_file_entity.assert_called_once()

    async def test_collision_keep_local(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # Spys
        spy_file_handle = mocker.spy(download_functions, "download_by_file_handle")
        spy_file_entity = mocker.spy(client, "download_file_entity")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(original_file_path)

        # AND the file is in the cache
        assert syn.cache.contains(
            file_handle_id=file.file_handle.id, path=original_file_path
        )

        # WHEN I download the file
        file = await File(id=file.id, if_collision="keep.local").get_async()

        # THEN the file is not downloaded again, and the file path is the same
        assert os.path.exists(file.path)
        assert os.path.exists(original_file_path)
        assert file.path == original_file_path

        # AND download_by_file_handle was not called
        spy_file_handle.assert_not_called()

        # AND file_entity was called
        spy_file_entity.assert_called_once()


class TestDownloadCaching:
    """Tests for downloading files with different cache states."""

    async def test_download_cached_file(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Tests that a file download that exists in cache is not downloaded again."""

        # Spys
        spy_file_handle = mocker.spy(download_functions, "download_by_file_handle")
        spy_file_entity = mocker.spy(client, "download_file_entity")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(original_file_path)

        # AND the file is in the cache
        assert syn.cache.contains(
            file_handle_id=file.file_handle.id, path=original_file_path
        )

        # WHEN I download the file
        file = await File(id=file.id).get_async()

        # THEN the file is not downloaded again, and the file path is the same
        assert os.path.exists(file.path)
        assert os.path.exists(original_file_path)
        assert file.path == original_file_path

        # AND download_by_file_handle was not called
        spy_file_handle.assert_not_called()

        # AND download_file_entity was called
        spy_file_entity.assert_called_once()

    async def test_download_cached_file_to_new_directory(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Tests that a file download that exists in cache is not downloaded again."""

        # Spys
        spy_file_handle = mocker.spy(download_functions, "download_by_file_handle")
        spy_file_entity = mocker.spy(client, "download_file_entity")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(original_file_path)

        # AND the file is in the cache
        assert syn.cache.contains(
            file_handle_id=file.file_handle.id, path=original_file_path
        )

        # WHEN I download the file to another location
        updated_location = os.path.join(
            os.path.dirname(original_file_path), "subdirectory"
        )
        schedule_for_cleanup(updated_location)
        file = await File(id=file.id, download_location=updated_location).get_async()
        schedule_for_cleanup(file.path)

        # THEN the file is not downloaded again, but it copied to the new location
        assert os.path.exists(file.path)
        assert os.path.exists(original_file_path)
        assert file.path != original_file_path

        # AND download_by_file_handle was not called
        spy_file_handle.assert_not_called()

        # AND download_file_entity was called
        spy_file_entity.assert_called_once()


class TestDownloadFromUrl:
    """Integration tests that will route through
    `synapseclient/core/download/download_functions.py::download_from_url`"""

    async def test_download_check_md5(
        self, project_model: Project, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        tempfile_path = utils.make_bogus_data_file()
        schedule_for_cleanup(tempfile_path)
        entity_bad_md5 = await File(
            path=tempfile_path, parent_id=project_model.id, synapse_store=False
        ).store_async()

        with pytest.raises(SynapseMd5MismatchError):
            await download_from_url(
                url=entity_bad_md5.external_url,
                destination=tempfile.gettempdir(),
                file_handle_id=entity_bad_md5.data_file_handle_id,
                expected_md5="2345a",
            )

    async def test_download_from_url_resume_partial_download(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        # GIVEN a file stored in synapse
        original_file = utils.make_bogus_data_file(40000)
        file = await File(path=original_file, parent_id=project_model.id).store_async()

        # AND the original file is stashed for comparison later
        updated_location = original_file + ".original"
        shutil.copy(original_file, updated_location)
        schedule_for_cleanup(updated_location)

        # AND an incomplete file in its place (This is simulated by truncating the file)
        tmp_path = utils.temp_download_filename(
            destination=tempfile.gettempdir(), file_handle_id=file.data_file_handle_id
        )
        shutil.move(src=original_file, dst=tmp_path)
        original_size = os.path.getsize(tmp_path)
        truncated_size = 3 * original_size // 7
        with open(tmp_path, "r+", encoding="utf-8") as f:
            f.truncate(truncated_size)

        # WHEN I resume the download
        path = await download_from_url(
            url=f"{syn.repoEndpoint}/entity/{file.id}/file",
            destination=tempfile.gettempdir(),
            file_handle_id=file.data_file_handle_id,
            expected_md5=file.file_handle.content_md5,
        )

        # THEN the file is downloaded and matches the original
        assert filecmp.cmp(original_file, path), "File comparison failed"
        # This can only be used when the integration test are running in debug mode. Which
        # is not the default. This was verified by running the test in debug mode.
        # `caplog: pytest.LogCaptureFixture` would need to be added to the args.
        # assert f"Resuming partial download to {tmp_path}. {truncated_size}/{original_size}.0 bytes already transferred." in caplog.text

    async def test_download_via_get(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Tests that a file not in cache is downloaded. Routes through the get
        function in the File class."""
        # Spy on the download_by_file_handle function
        spy = mocker.spy(download_functions, "download_by_file_handle")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(original_file_path)

        # AND the file is not in the cache
        syn.cache.remove(file_handle_id=file.file_handle.id)

        # AND the file has been moved to a different location
        updated_file_path = shutil.move(
            original_file_path,
            f"{original_file_path}.moved",
        )
        schedule_for_cleanup(updated_file_path)

        # WHEN I download the file
        file = await File(
            id=file.id, download_location=os.path.dirname(original_file_path)
        ).get_async()

        # THEN the file is downloaded to a different location and matches the original
        assert updated_file_path != file.path
        assert filecmp.cmp(updated_file_path, file.path)

        # AND download_by_file_handle was called
        spy.assert_called_once()

    async def test_download_to_default_cache(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Tests that a file not in the cache is downloaded to the default cache
        location."""
        # Spy on the download_by_file_handle function
        spy = mocker.spy(download_functions, "download_by_file_handle")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        synapse_cache_location = syn.cache.get_cache_dir(
            file_handle_id=file.data_file_handle_id
        )
        original_file_md5 = file.file_handle.content_md5
        schedule_for_cleanup(file.id)

        # AND the file is not in the cache
        syn.cache.remove(file_handle_id=file.file_handle.id)

        # AND the file has been deleted
        os.remove(original_file_path)

        # WHEN I download the file
        file = await File(id=file.id).get_async()

        # THEN the file is downloaded to a different location and matches the original
        assert original_file_path != file.path
        assert original_file_md5 == file.file_handle.content_md5
        assert os.path.dirname(file.path) == synapse_cache_location

        # AND download_by_file_handle was called
        spy.assert_called_once()


class TestDownloadFromUrlMultiThreaded:
    """Integration tests that will route through
    `synapseclient/core/download/download_functions.py::download_from_url_multi_threaded`
    """

    async def test_download_from_url_multi_threaded(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test download of a file if downloaded in multiple parts. In this case I am
        dropping the download part size to 500 bytes to force multiple parts download.
        """

        # GIVEN a file stored in synapse
        file_path = utils.make_bogus_data_file()
        file = await File(path=file_path, parent_id=project_model.id).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(file_path)
        file_md5 = file.file_handle.content_md5
        assert file_md5 is not None
        assert os.path.exists(file_path)

        # AND the file is not in the cache
        syn.cache.remove(file_handle_id=file.file_handle.id)
        os.remove(file_path)
        assert not os.path.exists(file_path)

        with patch.object(
            synapseclient.core.download.download_functions,
            "SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE",
            new=500,
        ), patch.object(
            synapseclient.core.download.download_async,
            "SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE",
            new=500,
        ):
            # WHEN I download the file with multiple parts
            file = await File(
                id=file.id, download_location=os.path.dirname(file.path)
            ).get_async()

        # THEN the file is downloaded and the md5 matches
        assert file.file_handle.content_md5 == file_md5
        assert os.path.exists(file_path)


class TestDownloadFromS3:
    async def test_download_with_external_object_store(
        self,
        mocker: MockerFixture,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Tests that a file not in cache is downloaded. Routes through the get
        function in the File class."""
        # Spy on the download_by_file_handle function
        spy = mocker.spy(download_functions, "download_by_file_handle")

        # GIVEN a file stored in synapse
        original_file_path = utils.make_bogus_data_file()
        file = await File(
            path=original_file_path, parent_id=project_model.id
        ).store_async()
        schedule_for_cleanup(file.id)
        schedule_for_cleanup(original_file_path)

        # AND the file is not in the cache
        syn.cache.remove(file_handle_id=file.file_handle.id)

        # AND the file has been moved to a different location
        updated_file_path = shutil.move(
            original_file_path,
            f"{original_file_path}.moved",
        )
        schedule_for_cleanup(updated_file_path)

        # WHEN I download the file
        file = await File(
            id=file.id, download_location=os.path.dirname(original_file_path)
        ).get_async()

        # THEN the file is downloaded to a different location and matches the original
        assert updated_file_path != file.path
        assert filecmp.cmp(updated_file_path, file.path)

        # AND download_by_file_handle was called
        spy.assert_called_once()
