import os
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient.models import FormData, FormGroup
from synapseclient.models.mixins import StateEnum


class TestFormGroup:
    """Unit tests for the FormGroup model."""

    @pytest.fixture
    def mock_response(self):
        """Mock API response from create_form_group_async"""
        return {
            "groupId": "12345",
            "name": "my_test_form_group",
            "createdOn": "2023-12-01T10:00:00.000Z",
            "createdBy": "3350396",
            "modifiedOn": "2023-12-01T10:00:00.000Z",
        }

    async def test_create_async_success(self, syn, mock_response):
        """Test successful form group creation"""
        # GIVEN a FormGroup with a name
        form_group = FormGroup(name="my_test_form_group")

        # WHEN creating the form group
        with patch(
            "synapseclient.api.form_services.create_form_group",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_create:
            result = await form_group.create_async(synapse_client=syn)

            # THEN the API should be called with correct parameters
            mock_create.assert_called_once_with(
                synapse_client=syn,
                name="my_test_form_group",
            )

            # AND the result should be a FormGroup with populated fields
            assert isinstance(result, FormGroup)
            assert result.name == "my_test_form_group"
            assert result.group_id == "12345"
            assert result.created_by == "3350396"
            assert result.created_on == "2023-12-01T10:00:00.000Z"

    async def test_create_async_without_name_raises_error(self, syn):
        """Test that creating without a name raises ValueError"""
        # GIVEN a FormGroup without a name
        form_group = FormGroup()

        # WHEN creating the form group
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="FormGroup 'name' must be provided"):
            await form_group.create_async(synapse_client=syn)


class TestFormData:
    """Unit tests for the FormData model."""

    @pytest.fixture
    def mock_response(self):
        """Mock API response from create_form_data"""
        return {
            "formDataId": "67890",
            "groupId": "12345",
            "name": "my_test_form_data",
            "dataFileHandleId": "54321",
            "createdOn": "2023-12-01T11:00:00.000Z",
            "createdBy": "3350396",
            "modifiedOn": "2023-12-01T11:00:00.000Z",
            "submissionStatus": {
                "state": "SUBMITTED_WAITING_FOR_REVIEW",
                "submittedOn": "2023-12-01T11:05:00.000Z",
                "reviewedBy": None,
                "reviewedOn": None,
                "rejectionReason": None,
            },
        }

    async def test_create_async_success(self, syn, mock_response):
        """Test successful form data creation"""
        # GIVEN a FormData with required fields
        form_data = FormData(
            group_id="12345",
            name="my_test_form_data",
            data_file_handle_id="54321",
        )

        # WHEN creating the form data
        with patch(
            "synapseclient.api.create_form_data",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_create_form:
            result = await form_data.create_async(synapse_client=syn)

            # THEN the API should be called with correct parameters
            mock_create_form.assert_called_once_with(
                synapse_client=syn,
                group_id="12345",
                form_change_request={
                    "name": "my_test_form_data",
                    "fileHandleId": "54321",
                },
            )

            # AND the result should be a FormData with populated fields
            assert isinstance(result, FormData)
            assert result.name == "my_test_form_data"
            assert result.form_data_id == "67890"
            assert result.group_id == "12345"
            assert result.data_file_handle_id == "54321"
            assert result.created_by == "3350396"
            assert (
                result.submission_status.state.value == "SUBMITTED_WAITING_FOR_REVIEW"
            )

    async def test_create_async_without_required_fields_raises_error(self, syn):
        """Test that creating without required fields raises ValueError"""
        # GIVEN a FormData missing required fields
        form_data = FormData(name="incomplete_form_data")

        # WHEN creating the form data
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError,
            match="'group_id', 'name', and 'data_file_handle_id' are required",
        ):
            await form_data.create_async(synapse_client=syn)

    @pytest.mark.parametrize(
        "as_reviewer,filter_by_state",
        [
            # Test for non-reviewers - allow all possible state filters
            (
                False,
                [
                    StateEnum.WAITING_FOR_SUBMISSION,
                    StateEnum.SUBMITTED_WAITING_FOR_REVIEW,
                    StateEnum.ACCEPTED,
                    StateEnum.REJECTED,
                ],
            ),
            # Test for reviewers - only allow review-related state filters
            (
                True,
                [
                    StateEnum.SUBMITTED_WAITING_FOR_REVIEW,
                    StateEnum.ACCEPTED,
                    StateEnum.REJECTED,
                ],
            ),
            # Test for non-reviewers - only allow selected state filters
            (False, [StateEnum.ACCEPTED, StateEnum.REJECTED]),
            # Test for reviewers - only allow selected state filters
            (True, [StateEnum.SUBMITTED_WAITING_FOR_REVIEW, StateEnum.REJECTED]),
        ],
    )
    async def test_list_async(self, syn, as_reviewer, filter_by_state):
        """Test listing form data asynchronously"""
        # GIVEN a FormData with a group_id
        form_data = FormData(group_id="12345")

        async def mock_form_data_list():
            yield {
                "formDataId": "11111",
                "groupId": "12345",
                "name": "form_data_1",
                "dataFileHandleId": "fh_1",
            }
            yield {
                "formDataId": "22222",
                "groupId": "12345",
                "name": "form_data_2",
                "dataFileHandleId": "fh_2",
            }
            yield {
                "formDataId": "33333",
                "groupId": "12345",
                "name": "form_data_3",
                "dataFileHandleId": "fh_3",
            }

        async def mock_generator():
            async for item in mock_form_data_list():
                yield item

        # WHEN listing the form data
        with patch(
            "synapseclient.api.list_form_data",
            return_value=mock_generator(),
        ) as mock_list_form:
            results = []

            async for item in form_data.list_async(
                synapse_client=syn,
                filter_by_state=filter_by_state,
                as_reviewer=as_reviewer,
            ):
                results.append(item)

            # THEN the results should be a list of FormData objects
            assert len(results) == 3

            assert all(isinstance(item, FormData) for item in results)
            assert results[0].form_data_id == "11111"
            assert results[1].form_data_id == "22222"
            assert results[2].form_data_id == "33333"

            # THEN the API should be called with correct parameters
            mock_list_form.assert_called_once_with(
                synapse_client=syn,
                group_id="12345",
                filter_by_state=filter_by_state,
                as_reviewer=as_reviewer,
            )

    @pytest.mark.parametrize(
        "as_reviewer,filter_by_state, expected",
        [
            # Test for non-reviewers - WAITING_FOR_SUBMISSION is allowed
            (False, [StateEnum.WAITING_FOR_SUBMISSION, StateEnum.ACCEPTED], None),
            # Test for reviewers - invalid state filter
            (True, [StateEnum.WAITING_FOR_SUBMISSION], ValueError),
        ],
    )
    async def test_validate_filter_by_state_raises_error_for_invalid_states(
        self, as_reviewer, filter_by_state, expected
    ):
        """Test that invalid state filters raise ValueError"""
        # GIVEN a FormData with a group_id
        form_data = FormData(group_id="12345")

        # WHEN validating filter_by_state with invalid states for non-reviewer
        # THEN it should raise ValueError
        if expected is ValueError:
            with pytest.raises(ValueError):
                # Call the private method directly for testing
                form_data._validate_filter_by_state(
                    filter_by_state=filter_by_state,
                    as_reviewer=as_reviewer,
                )

    async def test_download_async(self, syn):
        """Test downloading form data asynchronously"""
        # GIVEN a FormData with a form_data_id
        form_data = FormData(form_data_id="67890", data_file_handle_id="54321")

        # WHEN downloading the form data
        with patch(
            "synapseclient.core.download.download_functions.download_by_file_handle",
            new_callable=AsyncMock,
        ) as mock_download_file_handle, patch.object(syn, "cache") as mock_cache, patch(
            "synapseclient.core.download.download_functions.ensure_download_location_is_directory",
        ) as mock_ensure_dir:
            mock_cache.get.side_effect = "/tmp/foo"
            mock_ensure_dir.return_value = (
                mock_cache.get_cache_dir.return_value
            ) = "/tmp/download"
            mock_file_name = f"SYNAPSE_FORM_{form_data.data_file_handle_id}.csv"

            await form_data.download_async(
                synapse_client=syn, synapse_id="mock synapse_id"
            )

            # THEN the API should be called with correct parameters
            mock_download_file_handle.assert_called_once_with(
                file_handle_id=form_data.data_file_handle_id,
                synapse_id="mock synapse_id",
                entity_type="FileEntity",
                destination=os.path.join(mock_ensure_dir.return_value, mock_file_name),
                synapse_client=syn,
            )

    async def test_download_async_without_data_file_handle_id_raises_error(self, syn):
        """Test that downloading without data_file_handle_id raises ValueError"""
        # GIVEN a FormData without data_file_handle_id
        form_data = FormData(form_data_id="67890")

        # WHEN downloading the form data
        # THEN it should raise ValueError
        with pytest.raises(
            ValueError, match="data_file_handle_id must be set to download the file."
        ):
            await form_data.download_async(
                synapse_client=syn, synapse_id="mock synapse_id"
            )
