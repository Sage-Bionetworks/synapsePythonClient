from synapseclient.models.mixins import (
    FormChangeRequest,
    FormData,
    FormGroup,
    StateEnum,
    SubmissionStatus,
)


class TestFormGroupMixin:
    def test_fill_from_dict_with_valid_data(self) -> None:
        """Test fill_from_dict with all fields populated"""
        response_dict = {
            "groupId": "12345",
            "name": "Test Form Group",
            "createdBy": "67890",
            "createdOn": "2024-01-01T00:00:00.000Z",
        }

        properties = FormGroup().fill_from_dict(response_dict)

        assert properties.group_id == "12345"
        assert properties.name == "Test Form Group"
        assert properties.created_by == "67890"
        assert properties.created_on == "2024-01-01T00:00:00.000Z"

    def test_fill_from_dict_missing_fields(self) -> None:
        """Test fill_from_dict with some missing fields"""
        response_dict = {
            "groupId": "12345",
            # 'name' is missing
            "createdBy": "67890",
            # 'createdOn' is missing
        }

        properties = FormGroup().fill_from_dict(response_dict)

        assert properties.group_id == "12345"
        assert properties.name is None
        assert properties.created_by == "67890"
        assert properties.created_on is None

    def test_fill_from_dict_empty_dict(self) -> None:
        """Test fill_from_dict with an empty dictionary"""
        response_dict = {}

        properties = FormGroup().fill_from_dict(response_dict)

        assert properties.group_id is None
        assert properties.name is None
        assert properties.created_by is None
        assert properties.created_on is None


class TestFormChangeRequest:
    """Unit tests for FormChangeRequest.to_dict()"""

    def test_to_dict_with_all_fields(self):
        """Test to_dict with all fields populated"""
        # GIVEN a FormChangeRequest with all fields
        form_request = FormChangeRequest(name="my_form_name", file_handle_id="123456")

        # WHEN converting to dict
        result = form_request.to_dict()

        # THEN all fields should be present
        assert result == {"name": "my_form_name", "fileHandleId": "123456"}


class TestSubmissionStatus:
    """Unit tests for SubmissionStatus dataclass"""

    def test_submission_status_initialization(self) -> None:
        """Test initialization of SubmissionStatus with all fields"""
        status = SubmissionStatus(
            submitted_on="2024-01-01T00:00:00.000Z",
            reviewed_on="2024-01-02T00:00:00.000Z",
            reviewed_by="user_123",
        )

        assert status.submitted_on == "2024-01-01T00:00:00.000Z"
        assert status.reviewed_on == "2024-01-02T00:00:00.000Z"
        assert status.reviewed_by == "user_123"

    def test_fill_from_dict(self) -> None:
        """Test fill_from_dict method of SubmissionStatus"""
        response_dict = {
            "submittedOn": "2024-01-01T00:00:00.000Z",
            "reviewedOn": "2024-01-02T00:00:00.000Z",
            "reviewedBy": "user_123",
        }

        status = SubmissionStatus().fill_from_dict(response_dict)

        assert status.submitted_on == "2024-01-01T00:00:00.000Z"
        assert status.reviewed_on == "2024-01-02T00:00:00.000Z"
        assert status.reviewed_by == "user_123"

    def test_fill_from_dict_missing_fields(self) -> None:
        """Test fill_from_dict with missing fields"""
        response_dict = {
            "submittedOn": "2024-01-01T00:00:00.000Z"
            # 'reviewedOn' and 'reviewedBy' are missing
        }

        status = SubmissionStatus().fill_from_dict(response_dict)

        assert status.submitted_on == "2024-01-01T00:00:00.000Z"
        assert status.reviewed_on is None
        assert status.reviewed_by is None


class TestFormDataMixin:
    def test_fill_from_dict_with_valid_data(self) -> None:
        """Test fill_from_dict with all fields populated"""
        response_dict = {
            "formDataId": "54321",
            "groupId": "12345",
            "name": "Test Form Data",
            "dataFileHandleId": "67890",
            "submissionStatus": {
                "submittedOn": "2024-01-01T00:00:00.000Z",
                "reviewedOn": "2024-01-02T00:00:00.000Z",
                "reviewedBy": "user_123",
                "state": "SUBMITTED_WAITING_FOR_REVIEW",
                "rejectionMessage": None,
            },
        }

        form_data = FormData().fill_from_dict(response_dict)

        assert form_data.form_data_id == "54321"
        assert form_data.group_id == "12345"
        assert form_data.name == "Test Form Data"
        assert form_data.data_file_handle_id == "67890"
        assert form_data.submission_status.submitted_on == "2024-01-01T00:00:00.000Z"
        assert form_data.submission_status.reviewed_on == "2024-01-02T00:00:00.000Z"
        assert form_data.submission_status.reviewed_by == "user_123"
        assert (
            form_data.submission_status.state == StateEnum.SUBMITTED_WAITING_FOR_REVIEW
        )
        assert form_data.submission_status.rejection_message is None

    def test_fill_from_dict_missing_fields(self) -> None:
        """Test fill_from_dict with some missing fields"""
        response_dict = {
            "formDataId": "54321",
            # 'groupId' is missing
            "name": "Test Form Data",
            # 'dataFileHandleId' is missing
            # 'submissionStatus' is missing
        }

        form_data = FormData().fill_from_dict(response_dict)

        assert form_data.form_data_id == "54321"
        assert form_data.group_id is None
        assert form_data.name == "Test Form Data"
        assert form_data.data_file_handle_id is None
        assert form_data.submission_status is None
