"""Integration tests for the synapseclient.models.File class."""

import os
import asyncio
from unittest.mock import patch
import uuid

from typing import Callable
import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError

from synapseclient.models import (
    Project,
    Folder,
    File,
    Activity,
    UsedURL,
    UsedEntity,
)


class TestFileStore:
    """Tests for the synapseclient.models.File.store method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(self, schedule_for_cleanup: Callable[..., None]) -> None:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        )

    @pytest.mark.asyncio
    async def test_store_in_project(self, project_model: Project, file: File) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # WHEN I store the file
        file_copy_object = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored
        assert file.id is not None
        assert file_copy_object.id is not None
        assert file_copy_object == file
        assert file.parent_id == project_model.id
        assert file.content_type == "text/plain"
        assert file.version_comment == "My version comment"
        assert file.version_label is not None
        assert file.version_number == 1
        assert file.created_by is not None
        assert file.created_on is not None
        assert file.modified_by is not None
        assert file.modified_on is not None
        assert file.data_file_handle_id is not None
        assert file.file_handle is not None
        assert file.file_handle.id is not None
        assert file.file_handle.etag is not None
        assert file.file_handle.created_by is not None
        assert file.file_handle.created_on is not None
        assert file.file_handle.modified_on is not None
        assert file.file_handle.concrete_type is not None
        assert file.file_handle.content_type is not None
        assert file.file_handle.content_md5 is not None
        assert file.file_handle.file_name is not None
        assert file.file_handle.storage_location_id is not None
        assert file.file_handle.content_size is not None
        assert file.file_handle.status is not None
        assert file.file_handle.bucket_name is not None
        assert file.file_handle.key is not None
        assert file.file_handle.external_url is None

    @pytest.mark.asyncio
    async def test_store_in_folder(self, project_model: Project, file: File) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # AND a folder to store the file in
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store()
        self.schedule_for_cleanup(folder.id)

        # WHEN I store the file
        file_copy_object = await file.store(parent=folder)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored
        assert file.id is not None
        assert file_copy_object.id is not None
        assert file_copy_object == file
        assert file.parent_id == folder.id
        assert file.content_type == "text/plain"
        assert file.version_comment == "My version comment"
        assert file.version_label is not None
        assert file.version_number == 1
        assert file.created_by is not None
        assert file.created_on is not None
        assert file.modified_by is not None
        assert file.modified_on is not None
        assert file.data_file_handle_id is not None
        assert file.file_handle is not None
        assert file.file_handle.id is not None
        assert file.file_handle.etag is not None
        assert file.file_handle.created_by is not None
        assert file.file_handle.created_on is not None
        assert file.file_handle.modified_on is not None
        assert file.file_handle.concrete_type is not None
        assert file.file_handle.content_type is not None
        assert file.file_handle.content_md5 is not None
        assert file.file_handle.file_name is not None
        assert file.file_handle.storage_location_id is not None
        assert file.file_handle.content_size is not None
        assert file.file_handle.status is not None
        assert file.file_handle.bucket_name is not None
        assert file.file_handle.key is not None
        assert file.file_handle.external_url is None

    @pytest.mark.asyncio
    async def test_store_multiple_files(self, project_model: Project) -> None:
        # GIVEN a file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_1 = File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        )

        # AND a second file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_2 = File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        )

        # WHEN I store both the file
        files = await asyncio.gather(
            *[file_1.store(parent=project_model), file_2.store(parent=project_model)]
        )
        for file in files:
            self.schedule_for_cleanup(file.id)

            # THEN I expect the files to be stored
            assert file.id is not None
            assert file == file_1 or file == file_2
            assert file.parent_id == project_model.id
            assert file.content_type == "text/plain"
            assert file.version_comment == "My version comment"
            assert file.version_label is not None
            assert file.version_number == 1
            assert file.created_by is not None
            assert file.created_on is not None
            assert file.modified_by is not None
            assert file.modified_on is not None
            assert file.data_file_handle_id is not None
            assert file.file_handle is not None
            assert file.file_handle.id is not None
            assert file.file_handle.etag is not None
            assert file.file_handle.created_by is not None
            assert file.file_handle.created_on is not None
            assert file.file_handle.modified_on is not None
            assert file.file_handle.concrete_type is not None
            assert file.file_handle.content_type is not None
            assert file.file_handle.content_md5 is not None
            assert file.file_handle.file_name is not None
            assert file.file_handle.storage_location_id is not None
            assert file.file_handle.content_size is not None
            assert file.file_handle.status is not None
            assert file.file_handle.bucket_name is not None
            assert file.file_handle.key is not None
            assert file.file_handle.external_url is None

    @pytest.mark.asyncio
    async def test_store_change_filename(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())
        file.parent_id = project_model.id

        # WHEN I store the file
        file = await file.store()
        self.schedule_for_cleanup(file.id)
        before_etag = file.etag

        # AND I change the filename
        changed_file_name = str(uuid.uuid4())
        file.name = changed_file_name

        # AND I store the file again
        file = await file.store()

        # THEN I expect the file to be changed
        assert file.name == changed_file_name
        assert before_etag is not None
        assert file.etag is not None
        assert before_etag != file.etag

    @pytest.mark.asyncio
    async def test_store_move_file(self, project_model: Project, file: File) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())
        file.parent_id = project_model.id

        # AND a folder to store the file in
        folder = await Folder(
            name=str(uuid.uuid4()), parent_id=project_model.id
        ).store()
        self.schedule_for_cleanup(folder.id)

        # WHEN I store the file in the project
        file = await file.store()
        self.schedule_for_cleanup(file.id)
        assert file.parent_id == project_model.id
        before_file_id = file.id

        # AND I store the file under a new parent
        file = await file.store(parent=folder)

        # THEN I expect the file to have been moved
        assert file.parent_id == folder.id

        # AND the file does not have an updated ID
        assert before_file_id == file.id

    @pytest.mark.asyncio
    async def test_store_same_data_file_handle_id(self, project_model: Project) -> None:
        # GIVEN a file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_1 = await File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        ).store(parent=project_model)

        # AND a second file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file_2 = await File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        ).store(parent=project_model)
        assert file_1.data_file_handle_id is not None
        assert file_2.data_file_handle_id is not None
        assert file_1.data_file_handle_id != file_2.data_file_handle_id
        file_2_etag = file_2.etag

        # WHEN I store the data_file_handle_id onto the second file
        file_2.data_file_handle_id = file_1.data_file_handle_id
        await file_2.store()

        # THEN I expect the file handles to match
        assert file_2_etag != file_2.etag
        assert (await file_1.get()).file_handle == file_2.file_handle

    @pytest.mark.asyncio
    async def test_store_updated_file(self, project_model: Project) -> None:
        # GIVEN a file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file = await File(
            path=filename,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
        ).store(parent=project_model)
        before_etag = file.etag
        before_id = file.id
        before_file_handle_id = file.file_handle.id

        # WHEN I update the file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file.path = filename
        await file.store()

        # THEN I expect the file to be updated
        assert before_etag is not None
        assert file.etag is not None
        assert before_etag != file.etag
        assert before_id == file.id
        assert before_file_handle_id is not None
        assert file.file_handle.id is not None
        assert before_file_handle_id != file.file_handle.id

    @pytest.mark.asyncio
    async def test_store_and_get_activity(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN an activity
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )

        # AND a file with the activity
        file.name = str(uuid.uuid4())
        file.activity = activity

        # WHEN I store the file
        file = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # AND I get the file with the activity
        file_copy = await File(id=file.id, download_file=False).get(
            include_activity=True
        )

        # THEN I expect that the activity is returned
        assert file_copy.activity is not None
        assert file_copy.activity.name == "some_name"
        assert file_copy.activity.description == "some_description"
        assert file_copy.activity.used[0].name == "example"
        assert file_copy.activity.used[0].url == "https://www.synapse.org/"
        assert file_copy.activity.used[1].target_id == "syn456"
        assert file_copy.activity.used[1].target_version_number == 1
        assert file_copy.activity.executed[0].name == "example"
        assert file_copy.activity.executed[0].url == "https://www.synapse.org/"
        assert file_copy.activity.executed[1].target_id == "syn789"
        assert file_copy.activity.executed[1].target_version_number == 1

        # WHEN I get the file without the activity flag
        file_copy_2 = await File(id=file.id, download_file=False).get()

        # THEN I expect that the activity is not returned
        assert file_copy_2.activity is None

    @pytest.mark.asyncio
    async def test_store_annotations(self, project_model: Project, file: File) -> None:
        # GIVEN an annotation
        annotations_for_my_file = {
            "my_single_key_string": "a",
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }

        # AND a file with the annotation
        file.name = str(uuid.uuid4())
        file.annotations = annotations_for_my_file

        # WHEN I store the file
        file = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file annotations to have been stored
        assert file.annotations.keys() == annotations_for_my_file.keys()
        assert file.annotations["my_single_key_string"] == ["a"]
        assert file.annotations["my_key_string"] == ["b", "a", "c"]
        assert file.annotations["my_key_bool"] == [False, False, False]
        assert file.annotations["my_key_double"] == [1.2, 3.4, 5.6]
        assert file.annotations["my_key_long"] == [1, 2, 3]

        # WHEN I update the annotations and store the file again
        file.annotations["my_key_string"] = ["new", "values", "here"]
        await file.store()

        # THEN I expect the file annotations to have been updated
        assert file.annotations.keys() == annotations_for_my_file.keys()
        assert file.annotations["my_single_key_string"] == ["a"]
        assert file.annotations["my_key_string"] == ["new", "values", "here"]
        assert file.annotations["my_key_bool"] == [False, False, False]
        assert file.annotations["my_key_double"] == [1.2, 3.4, 5.6]
        assert file.annotations["my_key_long"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_store_without_upload(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # AND the file is not to be uploaded
        file.synapse_store = False

        # WHEN I store the file
        file = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored as an external file
        assert file.id is not None
        assert file.parent_id == project_model.id
        assert file.content_type == "text/plain"
        assert file.version_comment == "My version comment"
        assert file.version_label is not None
        assert file.version_number == 1
        assert file.created_by is not None
        assert file.created_on is not None
        assert file.modified_by is not None
        assert file.modified_on is not None
        assert file.data_file_handle_id is not None
        assert file.file_handle is not None
        assert file.file_handle.id is not None
        assert file.file_handle.etag is not None
        assert file.file_handle.created_by is not None
        assert file.file_handle.created_on is not None
        assert file.file_handle.modified_on is not None
        assert (
            file.file_handle.concrete_type
            == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
        )
        assert file.file_handle.content_type == "text/plain"
        assert file.file_handle.content_md5 is not None
        assert file.file_handle.file_name is not None
        assert file.file_handle.content_size is not None
        assert file.file_handle.status is not None
        assert file.file_handle.bucket_name is None
        assert file.file_handle.key is None

    @pytest.mark.asyncio
    async def test_store_conflict_with_existing_object(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # WHEN I store the file
        file_copy_object = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored
        assert file.id is not None
        assert file_copy_object.id is not None
        assert file_copy_object == file
        assert file.parent_id == project_model.id
        assert file.content_type == "text/plain"
        assert file.version_comment == "My version comment"
        assert file.version_label is not None
        assert file.version_number == 1
        assert file.created_by is not None
        assert file.created_on is not None
        assert file.modified_by is not None
        assert file.modified_on is not None
        assert file.data_file_handle_id is not None
        assert file.file_handle is not None
        assert file.file_handle.id is not None
        assert file.file_handle.etag is not None
        assert file.file_handle.created_by is not None
        assert file.file_handle.created_on is not None
        assert file.file_handle.modified_on is not None
        assert file.file_handle.concrete_type is not None
        assert file.file_handle.content_type is not None
        assert file.file_handle.content_md5 is not None
        assert file.file_handle.file_name is not None
        assert file.file_handle.storage_location_id is not None
        assert file.file_handle.content_size is not None
        assert file.file_handle.status is not None
        assert file.file_handle.bucket_name is not None
        assert file.file_handle.key is not None
        assert file.file_handle.external_url is None

        # WHEN I store a file with the same properties
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        new_file = File(
            path=filename,
            parent_id=project_model.id,
            name=file.name,
            create_or_update=False,
        )

        # THEN I expect a SynapseHTTPError to be raised
        with pytest.raises(SynapseHTTPError) as e:
            await new_file.store()

        assert (
            str(e.value)
            == f"409 Client Error: \nAn entity with the name: {file.name} already exists with a parentId: {project_model.id}"
        )

    @pytest.mark.asyncio
    async def test_store_force_version(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # WHEN I store the file
        await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored
        assert file.id is not None
        assert file.version_number == 1

        # WHEN I store the file again with force_version=False
        file.force_version = False
        await file.store()

        # THEN the version should not be updated
        assert file.version_number == 1

        # WHEN I store the file again with force_version=True
        file.force_version = True
        await file.store()

        # THEN the version should be updated
        assert file.version_number == 2

    @pytest.mark.asyncio
    async def test_store_is_restricted(
        self, project_model: Project, file: File
    ) -> None:
        """Tests that setting is_restricted is calling the correct Synapse method. We are
        not testing the actual behavior of the method, only that it is called with the
        correct arguments. We do not want to actually restrict the file in Synapse as it
        would send an email to our ACT team."""
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # AND the file is restricted
        file.is_restricted = True

        with patch(
            "synapseclient.client.Synapse._createAccessRequirementIfNone"
        ) as intercepted:
            # WHEN I store the file
            await file.store(parent=project_model)
            self.schedule_for_cleanup(file.id)

            # THEN I expect the file to be restricted
            assert intercepted.called

    @pytest.mark.asyncio
    async def test_store_and_get_with_activity(
        self, project_model: Project, file: File
    ) -> None:
        # GIVEN a file
        file.name = str(uuid.uuid4())

        # AND an activity
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )
        file.activity = activity

        # WHEN I store the file
        file_copy_object = await file.store(parent=project_model)
        self.schedule_for_cleanup(file.id)

        # THEN I expect the file to be stored
        assert file.id is not None
        assert file_copy_object.id is not None
        assert file_copy_object == file
        assert file.parent_id == project_model.id
        assert file.content_type == "text/plain"
        assert file.version_comment == "My version comment"
        assert file.version_label is not None
        assert file.version_number == 1
        assert file.created_by is not None
        assert file.created_on is not None
        assert file.modified_by is not None
        assert file.modified_on is not None
        assert file.data_file_handle_id is not None
        assert file.file_handle is not None
        assert file.file_handle.id is not None
        assert file.file_handle.etag is not None
        assert file.file_handle.created_by is not None
        assert file.file_handle.created_on is not None
        assert file.file_handle.modified_on is not None
        assert file.file_handle.concrete_type is not None
        assert file.file_handle.content_type is not None
        assert file.file_handle.content_md5 is not None
        assert file.file_handle.file_name is not None
        assert file.file_handle.storage_location_id is not None
        assert file.file_handle.content_size is not None
        assert file.file_handle.status is not None
        assert file.file_handle.bucket_name is not None
        assert file.file_handle.key is not None
        assert file.file_handle.external_url is None


class TestChangeMetadata:
    """Tests for the synapseclient.models.File.change_metadata method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(
        self, schedule_for_cleanup: Callable[..., None], project_model: Project
    ) -> None:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        file = asyncio.run(
            File(
                path=filename,
                description="This is an example file.",
                content_type="text/plain",
                version_comment="My version comment",
                version_label=str(uuid.uuid4()),
                parent_id=project_model.id,
            ).store()
        )
        schedule_for_cleanup(file.id)
        return file

    @pytest.mark.asyncio
    async def test_change_name(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.name is not None
        assert file.content_type == "text/plain"
        current_download_as = file.file_handle.file_name

        # WHEN I change the files metadata
        new_filename = f"my_new_file_name_{str(uuid.uuid4())}.txt"
        await file.change_metadata(name=new_filename)

        # THEN I expect only the file name to have been updated
        assert file.file_handle.file_name == current_download_as
        assert file.name == new_filename
        assert file.content_type == "text/plain"
        file_copy = await File(id=file.id, download_file=False).get()
        assert file_copy.file_handle.file_name == current_download_as
        assert file_copy.name == new_filename
        assert file_copy.content_type == "text/plain"

    @pytest.mark.asyncio
    async def test_change_content_type_and_download(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.name is not None
        assert file.content_type == "text/plain"
        current_filename = file.name

        # WHEN I change the files metadata
        new_filename = f"my_new_file_name_{str(uuid.uuid4())}.txt"
        await file.change_metadata(download_as=new_filename, content_type="text/json")

        # THEN I expect the file download name to have been updated
        assert file.file_handle.file_name == new_filename
        assert file.name == current_filename
        assert file.content_type == "text/json"
        file_copy = await File(id=file.id, download_file=False).get()
        assert file_copy.file_handle.file_name == new_filename
        assert file_copy.name == current_filename
        assert file_copy.content_type == "text/json"

    @pytest.mark.asyncio
    async def test_change_content_type_and_download_and_name(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.name is not None
        assert file.content_type == "text/plain"

        # WHEN I change the files metadata
        new_filename = f"my_new_file_name_{str(uuid.uuid4())}.txt"
        await file.change_metadata(
            name=new_filename, download_as=new_filename, content_type="text/json"
        )

        # THEN I expect the file download name, entity name, and content type to have been updated
        assert file.file_handle.file_name == new_filename
        assert file.name == new_filename
        assert file.content_type == "text/json"
        file_copy = await File(id=file.id, download_file=False).get()
        assert file_copy.file_handle.file_name == new_filename
        assert file_copy.name == new_filename
        assert file_copy.content_type == "text/json"


class TestFrom:
    """Tests for the synapseclient.models.File.from_id and
    synapseclient.models.File.from_path method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(
        self, schedule_for_cleanup: Callable[..., None], project_model: Project
    ) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        file = asyncio.run(
            File(
                path=filename,
                description="This is an example file.",
                content_type="text/plain",
                version_comment="My version comment",
                version_label=str(uuid.uuid4()),
                parent_id=project_model.id,
            ).store()
        )
        schedule_for_cleanup(file.id)
        return file

    @pytest.mark.asyncio
    async def test_from_id(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None

        # WHEN I get the file by id
        file_copy = await File.from_id(file.id)

        # THEN I expect the file to be returned
        assert file_copy.id == file.id

    @pytest.mark.asyncio
    async def test_from_path(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # WHEN I get the file by path
        file_copy = await File.from_path(file.path)

        # THEN I expect the file to be returned
        assert file_copy.id == file.id


class TestDelete:
    """Tests for the synapseclient.models.File.delete method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(
        self, schedule_for_cleanup: Callable[..., None], project_model: Project
    ) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        file = asyncio.run(
            File(
                path=filename,
                description="This is an example file.",
                content_type="text/plain",
                version_comment="My version comment",
                version_label=str(uuid.uuid4()),
                parent_id=project_model.id,
            ).store()
        )
        schedule_for_cleanup(file.id)
        return file

    @pytest.mark.asyncio
    async def test_delete(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None

        # WHEN I delete the file
        await file.delete()

        # THEN I expect the file to be deleted
        with pytest.raises(SynapseHTTPError) as e:
            await file.get()
        assert str(e.value) == f"404 Client Error: \nEntity {file.id} is in trash can."


class TestGet:
    """Tests for the synapseclient.models.File.get method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(
        self, schedule_for_cleanup: Callable[..., None], project_model: Project
    ) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        file = asyncio.run(
            File(
                path=filename,
                description="This is an example file.",
                content_type="text/plain",
                version_comment="My version comment",
                version_label=str(uuid.uuid4()),
                parent_id=project_model.id,
            ).store()
        )
        schedule_for_cleanup(file.id)
        return file

    @pytest.mark.asyncio
    async def test_get_by_path(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # WHEN I get the file by path
        file_copy = await File(path=file.path).get()

        # THEN I expect the file to be returned
        assert file_copy.id == file.id

    @pytest.mark.asyncio
    async def test_get_by_id(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None

        # WHEN I get the file by id
        file_copy = await File(id=file.id).get()

        # THEN I expect the file to be returned
        assert file_copy.id == file.id

    @pytest.mark.asyncio
    async def test_get_previous_version(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.version_number == 1

        # WHEN I update the file
        await file.store()

        # AND I get the file by version
        file_copy = await File(id=file.id, version_number=1).get()

        # THEN I expect the file to be returned
        assert file_copy.id == file.id
        assert file_copy.version_number == 1

    @pytest.mark.asyncio
    async def test_get_keep_both_for_matching_filenames(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None
        assert file.name is not None

        # AND I store a second file in another location
        filename = utils.make_bogus_uuid_file()
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        file_2 = await File(
            path=filename,
            name=file.name,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store()
        self.schedule_for_cleanup(file_2.id)

        # AND I change the download name of the second file to the first file
        await file_2.change_metadata(download_as=file.name)

        # WHEN I get the file with the default collision of `keep.both`
        file_2 = await File(
            id=file_2.id, download_location=os.path.dirname(file.path)
        ).get()

        # THEN I expect both files to exist
        assert file.path != file_2.path
        assert os.path.exists(file.path)
        assert os.path.exists(file_2.path)

        base_name, extension = os.path.splitext(os.path.basename(file.path))

        # AND the second file to have a different path
        assert os.path.basename(file_2.path) == f"{base_name}(1){extension}"

    @pytest.mark.asyncio
    async def test_get_overwrite_local_for_matching_filenames(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None
        assert file.name is not None
        file_1_md5 = utils.md5_for_file(file.path).hexdigest()

        # AND I store a second file in another location
        filename = utils.make_bogus_uuid_file()
        file_2_md5 = utils.md5_for_file(filename).hexdigest()
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        file_2 = await File(
            path=filename,
            name=file.name,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store()
        self.schedule_for_cleanup(file_2.id)

        # AND I change the download name of the second file to the first file
        await file_2.change_metadata(download_as=file.name)

        # WHEN I get the file with the default collision of `overwrite.local`
        file_2 = await File(
            id=file_2.id,
            download_location=os.path.dirname(file.path),
            if_collision="overwrite.local",
        ).get()

        # THEN I expect only the newer file to exist
        assert os.path.exists(file_2.path)
        assert file_1_md5 != file_2_md5
        assert utils.md5_for_file(file_2.path).hexdigest() == file_2_md5

    @pytest.mark.asyncio
    async def test_get_keep_local_for_matching_filenames(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None
        assert file.name is not None
        file_1_md5 = utils.md5_for_file(file.path).hexdigest()

        # AND I store a second file in another location
        filename = utils.make_bogus_uuid_file()
        file_2_md5 = utils.md5_for_file(filename).hexdigest()
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        file_2 = await File(
            path=filename,
            name=file.name,
            description="This is an example file.",
            content_type="text/plain",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
            parent_id=folder.id,
        ).store()
        self.schedule_for_cleanup(file_2.id)

        # AND I change the download name of the second file to the first file
        await file_2.change_metadata(download_as=file.name)

        # WHEN I get the file with the default collision of `keep.local`
        file_2 = await File(
            id=file_2.id,
            download_location=os.path.dirname(file.path),
            if_collision="keep.local",
        ).get()

        # THEN I expect only the newer file to exist
        assert file_2.path is None
        assert os.path.exists(file.path)
        assert file_1_md5 != file_2_md5
        assert utils.md5_for_file(file.path).hexdigest() == file_1_md5

    @pytest.mark.asyncio
    async def test_get_by_path_limit_search(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # AND I store a copy of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await file.copy(destination_id=folder.id)

        # WHEN I get the file by path and limit the search to the folder
        file_by_path = await File(
            path=file.path, synapse_container_limit=folder.id, download_file=False
        ).get()

        # THEN I expect the file in the folder to be returned
        assert file_by_path.id == file_copy.id

        # WHEN I get the file by path and limit the search to the project
        file_by_path = await File(
            path=file.path, synapse_container_limit=file.parent_id, download_file=False
        ).get()

        # THEN I expect the file at the project level to be returned
        assert file.id == file_by_path.id


class TestCopy:
    """Tests for the synapseclient.models.File.copy method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def file(
        self, schedule_for_cleanup: Callable[..., None], project_model: Project
    ) -> File:
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        file = asyncio.run(
            File(
                path=filename,
                description="This is an example file.",
                content_type="text/plain",
                version_comment="My version comment",
                version_label=str(uuid.uuid4()),
                parent_id=project_model.id,
            ).store()
        )
        schedule_for_cleanup(file.id)
        return file

    @pytest.mark.asyncio
    async def test_copy_same_path(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # AND I store a copy of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await file.copy(destination_id=folder.id)

        # WHEN I get both files by ID:
        file_1 = await File(id=file.id, download_file=False).get()
        file_2 = await File(id=file_copy.id, download_file=False).get()

        # THEN I expect the paths to be the same file
        assert file_1.path == file_2.path

    @pytest.mark.asyncio
    async def test_copy_annotations_and_activity(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # AND I store activites and annotations on the file
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )
        annotations_for_my_file = {
            "my_single_key_string": "a",
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        file.activity = activity
        file.annotations = annotations_for_my_file
        await file.store()

        # AND I store a copy of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await file.copy(destination_id=folder.id)

        # WHEN I get both files by ID:
        file_1 = await File(id=file.id, download_file=False).get()
        file_2 = await File(id=file_copy.id, download_file=False).get()

        # THEN I expect the activities and annotations to be the same
        assert file_1.annotations == file_2.annotations
        assert file_1.activity == file_2.activity

    @pytest.mark.asyncio
    async def test_copy_activity_only(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # AND I store activites and annotations on the file
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )
        annotations_for_my_file = {
            "my_single_key_string": "a",
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        file.activity = activity
        file.annotations = annotations_for_my_file
        await file.store()

        # AND I store a copy of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await file.copy(destination_id=folder.id, copy_annotations=False)

        # WHEN I get both files by ID:
        file_1 = await File(id=file.id, download_file=False).get()
        file_2 = await File(id=file_copy.id, download_file=False).get()

        # THEN I expect the activities to be the same and annotations on the second to be None
        assert file_1.annotations != file_2.annotations
        assert file_2.annotations is None
        assert file_1.activity == file_2.activity

    @pytest.mark.asyncio
    async def test_copy_with_no_activity_or_annotations(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None

        # AND I store activites and annotations on the file
        activity = Activity(
            name="some_name",
            description="some_description",
            used=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn456", target_version_number=1),
            ],
            executed=[
                UsedURL(name="example", url="https://www.synapse.org/"),
                UsedEntity(target_id="syn789", target_version_number=1),
            ],
        )
        annotations_for_my_file = {
            "my_single_key_string": "a",
            "my_key_string": ["b", "a", "c"],
            "my_key_bool": [False, False, False],
            "my_key_double": [1.2, 3.4, 5.6],
            "my_key_long": [1, 2, 3],
        }
        file.activity = activity
        file.annotations = annotations_for_my_file
        await file.store()

        # AND I store a copy of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await file.copy(
            destination_id=folder.id, copy_annotations=False, copy_activity=None
        )

        # WHEN I get both files by ID:
        file_1 = await File(id=file.id, download_file=False).get(include_activity=True)
        file_2 = await File(id=file_copy.id, download_file=False).get(
            include_activity=True
        )

        # THEN I expect the activities to be the same and annotations on the second to be None
        assert file_1.annotations != file_2.annotations
        assert file_1.activity != file_2.activity
        assert file_2.annotations is None
        assert file_2.activity is None

    @pytest.mark.asyncio
    async def test_copy_previous_version(self, file: File) -> None:
        # GIVEN a file stored in synapse
        assert file.id is not None
        assert file.path is not None
        assert file.version_number == 1
        file_first_md5 = utils.md5_for_file(file.path).hexdigest()

        # AND the file MD5 is updated
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file.path = filename
        await file.store()
        assert file.version_number == 2
        second_first_md5 = utils.md5_for_file(file.path).hexdigest()

        # WHEN I store a copy of the first version_number of the file in a folder
        folder = Folder(parent_id=file.parent_id, name=str(uuid.uuid4()))
        await folder.store()
        self.schedule_for_cleanup(folder.id)
        file_copy = await File(id=file.id, version_number=1).copy(
            destination_id=folder.id, copy_annotations=False, copy_activity=None
        )

        # THEN I expect the first version of the file to have been stored
        assert file_copy.file_handle.content_md5 == file_first_md5
        assert second_first_md5 != file_first_md5
