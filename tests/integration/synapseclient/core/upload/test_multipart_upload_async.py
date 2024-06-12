"""Integration tests for multipart upload."""

import filecmp
import logging
import os
import random
import string
import tempfile
import uuid
from io import open
from typing import Callable
from unittest import mock, skip

import httpx

import synapseclient.core.config
import synapseclient.core.utils as utils
from synapseclient import Synapse
from synapseclient.core.upload.multipart_upload_async import (
    MIN_PART_SIZE,
    multipart_copy_async,
    multipart_upload_file_async,
    multipart_upload_string_async,
)
from synapseclient.models import File, Project
from synapseclient.core.download import download_by_file_handle


async def test_round_trip(
    syn: Synapse, project_model: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    # GIVEN a bogus binary file
    file_handle_id = None
    filepath = utils.make_bogus_binary_file(MIN_PART_SIZE + 777771)
    try:
        # WHEN I upload the file to Synapse
        file_handle_id = await multipart_upload_file_async(syn, filepath)

        # AND Create a reference in Synapse to the file
        junk = await File(
            parent_id=project_model.id, data_file_handle_id=file_handle_id
        ).store_async()

        (_, tmp_path) = tempfile.mkstemp()
        schedule_for_cleanup(tmp_path)

        # AND I download the file from Synapse
        path_from_file_handle = await download_by_file_handle(
            file_handle_id=file_handle_id,
            synapse_id=junk.id,
            entity_type="FileEntity",
            destination=tmp_path,
        )

        # THEN I expect the files to match
        assert filecmp.cmp(filepath, path_from_file_handle)

        # AND that retrieving the file by ID also works to fill in the path
        file_by_id = await File(id=junk.id).get_async()
        assert filecmp.cmp(filepath, file_by_id.path)
    except Exception:
        syn.logger.exception("Failed to upload or download file")

    finally:
        try:
            if "junk" in locals():
                await junk.delete_async()
        except Exception:
            syn.logger.exception("Failed to cleanup")
        try:
            os.remove(filepath)
        except Exception:
            syn.logger.exception("Failed to cleanup")


async def test_single_thread_upload(syn: Synapse) -> None:
    synapseclient.core.config.single_threaded = True
    try:
        filepath = utils.make_bogus_binary_file(MIN_PART_SIZE * 2 + 1)
        assert await multipart_upload_file_async(syn, filepath) is not None
    finally:
        synapseclient.core.config.single_threaded = False


async def test_randomly_failing_parts(
    syn: Synapse, project_model: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    """Verify that we can recover gracefully with some randomly inserted errors
    while uploading parts."""

    fail_every = 3  # fail every nth request
    fail_cycle = random.randint(
        0, fail_every - 1
    )  # randomly vary which n of the request cycle we fail
    file_handle_id = None

    # GIVEN A file created on disk
    filepath = utils.make_bogus_binary_file(MIN_PART_SIZE * 2 + (MIN_PART_SIZE / 2))

    put_count = 0
    normal_put = httpx.Client.put

    def _put_chunk_or_fail_randomly(self, url, *args, **kwargs):
        # fail every nth put to aws s3
        if "s3.amazonaws.com" not in url:
            return normal_put(self, url, *args, **kwargs)

        nonlocal put_count
        put_count += 1

        if (put_count + fail_cycle) % fail_every == 0:
            raise IOError("Ooops! Artificial upload failure for testing.")

        return normal_put(self, url, *args, **kwargs)

    with mock.patch(
        "httpx.Client.put", side_effect=_put_chunk_or_fail_randomly, autospec=True
    ):
        try:
            # WHEN I upload the file to Synapse
            file_handle_id = await multipart_upload_file_async(
                syn, filepath, part_size=MIN_PART_SIZE
            )

            # AND Create a reference to the File in Synapse
            junk = await File(
                parent_id=project_model.id, data_file_handle_id=file_handle_id
            ).store_async()

            (_, tmp_path) = tempfile.mkstemp()
            schedule_for_cleanup(tmp_path)

            # AND I download the file from Synapse
            path_from_file_handle = await download_by_file_handle(
                file_handle_id=file_handle_id,
                synapse_id=junk.id,
                entity_type="FileEntity",
                destination=tmp_path,
            )

            # THEN I expect the files to match
            assert filecmp.cmp(filepath, path_from_file_handle)

            # AND that retrieving the file by ID also works to fill in the path
            file_by_id = await File(id=junk.id).get_async()
            assert filecmp.cmp(filepath, file_by_id.path)
        except Exception:
            syn.logger.exception("Failed to upload or download file")

        finally:
            try:
                if "junk" in locals():
                    await junk.delete_async()
            except Exception:
                syn.logger.exception("Failed to cleanup")
            try:
                os.remove(filepath)
            except Exception:
                syn.logger.exception("Failed to cleanup")


async def test_multipart_upload_big_string(
    syn: Synapse, project_model: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    # GIVEN A string to upload to Synapse
    cities = [
        "Seattle",
        "Portland",
        "Vancouver",
        "Victoria",
        "San Francisco",
        "Los Angeles",
        "New York",
        "Oaxaca",
        "Cancún",
        "Curaçao",
        "जोधपुर",
        "অসম",
        "ལྷ་ས།",
        "ཐིམ་ཕུ་",
        "دبي",
        "አዲስ አበባ",
        "São Paulo",
        "Buenos Aires",
        "Cartagena",
        "Amsterdam",
        "Venice",
        "Rome",
        "Dubrovnik",
        "Sarajevo",
        "Madrid",
        "Barcelona",
        "Paris",
        "Αθήνα",
        "Ρόδος",
        "København",
        "Zürich",
        "金沢市",
        "서울",
        "แม่ฮ่องสอน",
        "Москва",
    ]

    text = "Places I wanna go:\n"
    while len(text.encode("utf-8")) < MIN_PART_SIZE:
        text += ", ".join(random.choice(cities) for i in range(5000)) + "\n"

    # WHEN I upload the string to Synapse
    file_handle_id = await multipart_upload_string_async(syn, text)

    # AND I store a reference to the String in an Entity
    junk = await File(
        parent_id=project_model.id, data_file_handle_id=file_handle_id
    ).store_async()

    (_, tmp_path) = tempfile.mkstemp()
    schedule_for_cleanup(tmp_path)

    # AND I download the file from Synapse
    file_handle_path = await download_by_file_handle(
        file_handle_id=file_handle_id,
        synapse_id=junk.id,
        entity_type="FileEntity",
        destination=tmp_path,
    )

    # THEN I expect the data to match
    with open(file_handle_path, encoding="utf-8") as f:
        retrieved_text = f.read()

    assert retrieved_text == text


async def _multipart_copy_test(
    syn: Synapse,
    project_model: Project,
    schedule_for_cleanup: Callable[..., None],
    part_size: int,
) -> None:
    logging.basicConfig()
    logging.getLogger(synapseclient.client.DEFAULT_LOGGER_NAME).setLevel(logging.DEBUG)

    dest_folder_name = "test_multipart_copy_{}".format(uuid.uuid4())

    # GIVEN A new folder with an S3 storage location that we can copy to
    dest_folder, storage_location_setting, _ = syn.create_s3_storage_location(
        parent=project_model.id, folder_name=dest_folder_name
    )

    # AND A file with some data
    part_size = part_size
    file_size = int(part_size * 1.1)

    base_string = "".join(random.choices(string.ascii_lowercase, k=1024))

    file_content = base_string
    while len(file_content) < file_size:
        file_content += base_string

    tmp = tempfile.NamedTemporaryFile(delete=False)
    schedule_for_cleanup(tmp.name)

    with open(tmp.name, "w") as tmp_out:
        tmp_out.write(file_content)

    # WHEN I upload the file to Synapse in a default location
    file = await File(path=tmp.name, parent_id=project_model.id).store_async()

    # AND I copy the file to the new folder
    dest_file_name = "{}_copy".format(file.name)
    source_file_handle_assocation = {
        "fileHandleId": file.data_file_handle_id,
        "associateObjectId": file.id,
        "associateObjectType": "FileEntity",
    }

    dest_storage_location = storage_location_setting["storageLocationId"]

    copied_fhid = await multipart_copy_async(
        syn,
        source_file_handle_assocation,
        dest_file_name,
        part_size,
        dest_storage_location,
    )

    # AND store a reference to the copied file
    dest_file = await File(
        name=dest_file_name,
        parent_id=dest_folder["id"],
        data_file_handle_id=copied_fhid,
    ).store_async()

    # AND Download the copied file from Synapse
    await dest_file.get_async()

    # THEN I expect the data to match
    with open(dest_file.path, "r") as dest_file_in:
        dest_file_content = dest_file_in.read()

    assert file_content == dest_file_content

    # AND retrieving the file again should also work by ID
    dest_file_copy = await File(dest_file.id).get_async()

    with open(dest_file_copy.path, "r") as dest_file_in:
        dest_file_content = dest_file_in.read()

    assert file_content == dest_file_content


async def test_multipart_copy(
    syn: Synapse, project: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    """Test multi part copy using the minimum part size."""
    await _multipart_copy_test(syn, project, schedule_for_cleanup, MIN_PART_SIZE)


@skip("Skip in normal testing because the large size makes it slow")
async def test_multipart_copy__big_parts(
    syn: Synapse, project: Project, schedule_for_cleanup: Callable[..., None]
) -> None:
    await _multipart_copy_test(syn, project, schedule_for_cleanup, 100 * utils.MB)
