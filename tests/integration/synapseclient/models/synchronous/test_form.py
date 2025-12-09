"""
Integration tests for the synapseclient.models.Form class.
"""
import tempfile
import uuid
from typing import Callable

import pytest

import synapseclient.core.utils as utils
from synapseclient import Synapse
from synapseclient.models import File, FormData, FormGroup, Project
from synapseclient.models.mixins.form import StateEnum


class TestFormGroup:
    def test_create_form_group(
        self, syn, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        """Test creating a form group."""
        unique_name = str(uuid.uuid4())
        form_group = FormGroup(name=unique_name).create(synapse_client=syn)

        assert form_group is not None
        assert form_group.group_id is not None
        assert form_group.name == unique_name

        schedule_for_cleanup(form_group.group_id)

    def test_raise_error_on_missing_name(self, syn) -> None:
        """Test that creating a form group without a name raises an error."""
        form_group = FormGroup()

        with pytest.raises(ValueError) as e:
            form_group.create(synapse_client=syn)
        assert (
            str(e.value) == "FormGroup 'name' must be provided to create a FormGroup."
        )


class TestFormData:
    @pytest.fixture(autouse=True, scope="session")
    def test_form_group(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> FormGroup:
        """Create a test form group for use in form data tests."""
        unique_name = "test_form_group_" + str(uuid.uuid4())
        form_group = FormGroup(name=unique_name)
        form_group = form_group.create(synapse_client=syn)

        schedule_for_cleanup(form_group.group_id)

        return form_group

    @pytest.fixture(autouse=True, scope="session")
    def test_file(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> File:
        """Create a test file for use in form data tests."""
        # Create a test project and a test file to get a file handle ID
        project_name = str(uuid.uuid4())
        project = Project(name=project_name)
        project = project.store(synapse_client=syn)

        file_path = utils.make_bogus_data_file()
        file = File(path=file_path, parent_id=project.id).store(synapse_client=syn)

        schedule_for_cleanup(file.id)
        schedule_for_cleanup(file_path)
        schedule_for_cleanup(project.id)

        return file

    def test_create_form_data(
        self,
        syn: Synapse,
        test_form_group: FormGroup,
        test_file: File,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test creating form data."""
        unique_name = "test_form_data_" + str(uuid.uuid4())

        form_data = FormData(
            name=unique_name,
            group_id=test_form_group.group_id,
            data_file_handle_id=test_file.file_handle.id,
        ).create(synapse_client=syn)

        assert form_data is not None
        assert form_data.form_data_id is not None
        assert form_data.name == unique_name
        assert form_data.group_id == test_form_group.group_id
        assert form_data.data_file_handle_id == test_file.file_handle.id
        assert form_data.submission_status.state.value == "WAITING_FOR_SUBMISSION"

        schedule_for_cleanup(form_data.form_data_id)

    def test_create_raise_error_on_missing_fields(self, syn: Synapse) -> None:
        """Test that creating form data without required fields raises an error."""
        form_data = FormData()

        with pytest.raises(ValueError) as e:
            form_data.create(synapse_client=syn)
        assert (
            str(e.value)
            == "'group_id', 'name', and 'data_file_handle_id' must be provided to create a FormData."
        )

    def test_list_form_data_reviewer_false(
        self,
        syn: Synapse,
        test_form_group: FormGroup,
        test_file: File,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test listing form data."""
        # Create multiple form data entries
        form_data_ids = []
        for i in range(3):
            unique_name = f"test_form_data_{i}_" + str(uuid.uuid4())
            form_data = FormData(
                name=unique_name,
                group_id=test_form_group.group_id,
                data_file_handle_id=test_file.file_handle.id,
            ).create(synapse_client=syn)
            form_data_ids.append(form_data.form_data_id)
            schedule_for_cleanup(form_data.form_data_id)

        # List form data owned by the caller
        retrieved_ids = []
        for form_data in FormData(group_id=test_form_group.group_id).list(
            synapse_client=syn,
            filter_by_state=[StateEnum.WAITING_FOR_SUBMISSION],
            as_reviewer=False,
        ):
            retrieved_ids.append(form_data.form_data_id)

        for form_data_id in form_data_ids:
            assert form_data_id in retrieved_ids

    def test_list_form_data_reviewer_true(
        self,
        syn: Synapse,
        test_form_group: FormGroup,
        test_file: File,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test listing form data as reviewer."""
        # Create multiple form data entries
        form_data_ids = []
        for i in range(3):
            unique_name = f"test_form_data_{i}_" + str(uuid.uuid4())
            form_data = FormData(
                name=unique_name,
                group_id=test_form_group.group_id,
                data_file_handle_id=test_file.file_handle.id,
            ).create(synapse_client=syn)
            #  Submit the form data
            syn.restPOST(uri=f"/form/data/{form_data.form_data_id}/submit", body={})
            form_data_ids.append(form_data.form_data_id)
            schedule_for_cleanup(form_data.form_data_id)

        # List form data as reviewer
        retrieved_ids = []
        for form_data in FormData(group_id=test_form_group.group_id).list(
            synapse_client=syn,
            filter_by_state=[StateEnum.SUBMITTED_WAITING_FOR_REVIEW],
            as_reviewer=True,
        ):
            retrieved_ids.append(form_data.form_data_id)

        for form_data_id in form_data_ids:
            assert form_data_id in retrieved_ids

    def test_download_form_data(
        self,
        syn: Synapse,
        test_form_group: FormGroup,
        test_file: File,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test downloading form data."""
        unique_name = "test_form_data_" + str(uuid.uuid4())

        form_data = FormData(
            name=unique_name,
            group_id=test_form_group.group_id,
            data_file_handle_id=test_file.file_handle.id,
        ).create(synapse_client=syn)

        schedule_for_cleanup(form_data.form_data_id)

        downloaded_form_path = FormData(
            data_file_handle_id=test_file.file_handle.id
        ).download(synapse_client=syn, synapse_id=form_data.form_data_id)

        schedule_for_cleanup(downloaded_form_path)

        assert test_file.file_handle.id in downloaded_form_path

    def test_download_form_data_with_directory(
        self,
        syn: Synapse,
        test_form_group: FormGroup,
        test_file: File,
        schedule_for_cleanup: Callable[..., None],
    ) -> None:
        """Test downloading form data to a specific directory."""
        unique_name = "test_form_data_" + str(uuid.uuid4())

        form_data = FormData(
            name=unique_name,
            group_id=test_form_group.group_id,
            data_file_handle_id=test_file.file_handle.id,
        ).create(synapse_client=syn)
        tmp_dir = tempfile.mkdtemp()
        schedule_for_cleanup(tmp_dir)

        downloaded_form_path = FormData(
            data_file_handle_id=test_file.file_handle.id
        ).download(
            synapse_client=syn,
            synapse_id=form_data.form_data_id,
            download_location=tmp_dir,
        )

        schedule_for_cleanup(form_data.form_data_id)

        assert test_file.file_handle.id in downloaded_form_path
        assert str(downloaded_form_path).startswith(str(tmp_dir))
