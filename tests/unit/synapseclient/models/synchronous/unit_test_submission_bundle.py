"""Unit tests for the synapseclient.models.SubmissionBundle class synchronous methods."""

from typing import Dict, Union
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models import Submission, SubmissionBundle, SubmissionStatus

SUBMISSION_ID = "9999999"
SUBMISSION_STATUS_ID = "9999999"
ENTITY_ID = "syn123456"
EVALUATION_ID = "9614543"
USER_ID = "123456"
ETAG = "etag_value"
MODIFIED_ON = "2023-01-01T00:00:00.000Z"
CREATED_ON = "2023-01-01T00:00:00.000Z"
STATUS = "RECEIVED"


class TestSubmissionBundleSync:
    """Tests for the synapseclient.models.SubmissionBundle class synchronous methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_submission_dict(self) -> Dict[str, Union[str, int, Dict]]:
        """Return example submission data from REST API."""
        return {
            "id": SUBMISSION_ID,
            "userId": USER_ID,
            "submitterAlias": "test_user",
            "entityId": ENTITY_ID,
            "versionNumber": 1,
            "name": "Test Submission",
            "createdOn": CREATED_ON,
            "evaluationId": EVALUATION_ID,
            "entityBundle": {
                "entity": {
                    "id": ENTITY_ID,
                    "name": "test_entity",
                    "concreteType": "org.sagebionetworks.repo.model.FileEntity",
                },
                "entityType": "org.sagebionetworks.repo.model.FileEntity",
            },
        }

    def get_example_submission_status_dict(
        self,
    ) -> Dict[str, Union[str, int, bool, Dict]]:
        """Return example submission status data from REST API."""
        return {
            "id": SUBMISSION_STATUS_ID,
            "etag": ETAG,
            "modifiedOn": MODIFIED_ON,
            "status": STATUS,
            "entityId": ENTITY_ID,
            "versionNumber": 1,
            "statusVersion": 1,
            "canCancel": False,
            "cancelRequested": False,
            "submissionAnnotations": {"score": [85.5], "feedback": ["Good work!"]},
        }

    def get_example_submission_bundle_dict(self) -> Dict[str, Dict]:
        """Return example submission bundle data from REST API."""
        return {
            "submission": self.get_example_submission_dict(),
            "submissionStatus": self.get_example_submission_status_dict(),
        }

    def get_example_submission_bundle_minimal_dict(self) -> Dict[str, Dict]:
        """Return example minimal submission bundle data from REST API."""
        return {
            "submission": {
                "id": SUBMISSION_ID,
                "entityId": ENTITY_ID,
                "evaluationId": EVALUATION_ID,
            },
            "submissionStatus": None,
        }

    def test_init_submission_bundle(self) -> None:
        """Test creating a SubmissionBundle with basic attributes."""
        # GIVEN submission and submission status objects
        submission = Submission(
            id=SUBMISSION_ID,
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
        )
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            entity_id=ENTITY_ID,
        )

        # WHEN I create a SubmissionBundle object
        bundle = SubmissionBundle(
            submission=submission,
            submission_status=submission_status,
        )

        # THEN the SubmissionBundle should have the expected attributes
        assert bundle.submission == submission
        assert bundle.submission_status == submission_status
        assert bundle.submission.id == SUBMISSION_ID
        assert bundle.submission_status.id == SUBMISSION_STATUS_ID

    def test_init_submission_bundle_empty(self) -> None:
        """Test creating an empty SubmissionBundle."""
        # WHEN I create an empty SubmissionBundle object
        bundle = SubmissionBundle()

        # THEN the SubmissionBundle should have None attributes
        assert bundle.submission is None
        assert bundle.submission_status is None

    def test_fill_from_dict_complete(self) -> None:
        """Test filling a SubmissionBundle from complete REST API response."""
        # GIVEN a complete submission bundle response
        bundle_data = self.get_example_submission_bundle_dict()

        # WHEN I fill a SubmissionBundle from the response
        bundle = SubmissionBundle().fill_from_dict(bundle_data)

        # THEN all fields should be populated correctly
        assert bundle.submission is not None
        assert bundle.submission_status is not None

        # Check submission fields
        assert bundle.submission.id == SUBMISSION_ID
        assert bundle.submission.entity_id == ENTITY_ID
        assert bundle.submission.evaluation_id == EVALUATION_ID
        assert bundle.submission.user_id == USER_ID

        # Check submission status fields
        assert bundle.submission_status.id == SUBMISSION_STATUS_ID
        assert bundle.submission_status.status == STATUS
        assert bundle.submission_status.entity_id == ENTITY_ID
        assert (
            bundle.submission_status.evaluation_id == EVALUATION_ID
        )  # set from submission

        # Check submission annotations
        assert "score" in bundle.submission_status.submission_annotations
        assert bundle.submission_status.submission_annotations["score"] == [85.5]

    def test_fill_from_dict_minimal(self) -> None:
        """Test filling a SubmissionBundle from minimal REST API response."""
        # GIVEN a minimal submission bundle response
        bundle_data = self.get_example_submission_bundle_minimal_dict()

        # WHEN I fill a SubmissionBundle from the response
        bundle = SubmissionBundle().fill_from_dict(bundle_data)

        # THEN submission should be populated but submission_status should be None
        assert bundle.submission is not None
        assert bundle.submission_status is None

        # Check submission fields
        assert bundle.submission.id == SUBMISSION_ID
        assert bundle.submission.entity_id == ENTITY_ID
        assert bundle.submission.evaluation_id == EVALUATION_ID

    def test_fill_from_dict_no_submission(self) -> None:
        """Test filling a SubmissionBundle with no submission data."""
        # GIVEN a bundle response with no submission
        bundle_data = {
            "submission": None,
            "submissionStatus": self.get_example_submission_status_dict(),
        }

        # WHEN I fill a SubmissionBundle from the response
        bundle = SubmissionBundle().fill_from_dict(bundle_data)

        # THEN submission should be None but submission_status should be populated
        assert bundle.submission is None
        assert bundle.submission_status is not None
        assert bundle.submission_status.id == SUBMISSION_STATUS_ID
        assert bundle.submission_status.status == STATUS

    def test_fill_from_dict_evaluation_id_setting(self) -> None:
        """Test that evaluation_id is properly set from submission to submission_status."""
        # GIVEN a bundle response where submission_status doesn't have evaluation_id
        submission_dict = self.get_example_submission_dict()
        status_dict = self.get_example_submission_status_dict()
        # Remove evaluation_id from status_dict to simulate API response
        status_dict.pop("evaluationId", None)

        bundle_data = {
            "submission": submission_dict,
            "submissionStatus": status_dict,
        }

        # WHEN I fill a SubmissionBundle from the response
        bundle = SubmissionBundle().fill_from_dict(bundle_data)

        # THEN submission_status should get evaluation_id from submission
        assert bundle.submission is not None
        assert bundle.submission_status is not None
        assert bundle.submission.evaluation_id == EVALUATION_ID
        assert bundle.submission_status.evaluation_id == EVALUATION_ID

    def test_get_evaluation_submission_bundles(self) -> None:
        """Test getting submission bundles for an evaluation using sync method."""
        # GIVEN mock response data
        mock_response = {
            "results": [
                {
                    "submission": {
                        "id": "123",
                        "entityId": ENTITY_ID,
                        "evaluationId": EVALUATION_ID,
                        "userId": USER_ID,
                    },
                    "submissionStatus": {
                        "id": "123",
                        "status": "RECEIVED",
                        "entityId": ENTITY_ID,
                    },
                },
                {
                    "submission": {
                        "id": "456",
                        "entityId": ENTITY_ID,
                        "evaluationId": EVALUATION_ID,
                        "userId": USER_ID,
                    },
                    "submissionStatus": {
                        "id": "456",
                        "status": "SCORED",
                        "entityId": ENTITY_ID,
                    },
                },
            ]
        }

        # WHEN I call get_evaluation_submission_bundles (sync method)
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_submission_bundles"
        ) as mock_get_bundles:
            # Create an async generator function that yields bundle data
            async def mock_async_gen(*args, **kwargs):
                for bundle_data in mock_response["results"]:
                    yield bundle_data

            # Make the mock return our async generator when called
            mock_get_bundles.side_effect = mock_async_gen

            result = list(
                SubmissionBundle.get_evaluation_submission_bundles(
                    evaluation_id=EVALUATION_ID,
                    status="RECEIVED",
                    synapse_client=self.syn,
                )
            )

            # THEN the service should be called with correct parameters
            mock_get_bundles.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                status="RECEIVED",
                synapse_client=self.syn,
            )

            # AND the result should contain SubmissionBundle objects
            assert len(result) == 2
            assert all(isinstance(bundle, SubmissionBundle) for bundle in result)

            # Check first bundle
            assert result[0].submission is not None
            assert result[0].submission.id == "123"
            assert result[0].submission_status is not None
            assert result[0].submission_status.id == "123"
            assert result[0].submission_status.status == "RECEIVED"
            assert (
                result[0].submission_status.evaluation_id == EVALUATION_ID
            )  # set from submission

            # Check second bundle
            assert result[1].submission is not None
            assert result[1].submission.id == "456"
            assert result[1].submission_status is not None
            assert result[1].submission_status.id == "456"
            assert result[1].submission_status.status == "SCORED"

    def test_get_evaluation_submission_bundles_empty_response(self) -> None:
        """Test getting submission bundles with empty response using sync method."""
        # GIVEN empty mock response
        mock_response = {"results": []}

        # WHEN I call get_evaluation_submission_bundles (sync method)
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_submission_bundles"
        ) as mock_get_bundles:
            # Create an async generator function that yields no data
            async def mock_async_gen(*args, **kwargs):
                return
                yield

            # Make the mock return our async generator when called
            mock_get_bundles.side_effect = mock_async_gen

            result = list(
                SubmissionBundle.get_evaluation_submission_bundles(
                    evaluation_id=EVALUATION_ID,
                    synapse_client=self.syn,
                )
            )

            # THEN the service should be called
            mock_get_bundles.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                status=None,
                synapse_client=self.syn,
            )

            # AND the result should be an empty list
            assert len(result) == 0

    def test_get_user_submission_bundles(self) -> None:
        """Test getting user submission bundles using sync method."""
        # GIVEN mock response data
        mock_response = {
            "results": [
                {
                    "submission": {
                        "id": "789",
                        "entityId": ENTITY_ID,
                        "evaluationId": EVALUATION_ID,
                        "userId": USER_ID,
                        "name": "User Submission 1",
                    },
                    "submissionStatus": {
                        "id": "789",
                        "status": "VALIDATED",
                        "entityId": ENTITY_ID,
                    },
                },
            ]
        }

        # WHEN I call get_user_submission_bundles (sync method)
        with patch(
            "synapseclient.api.evaluation_services.get_user_submission_bundles"
        ) as mock_get_user_bundles:
            # Create an async generator function that yields bundle data
            async def mock_async_gen(*args, **kwargs):
                for bundle_data in mock_response["results"]:
                    yield bundle_data

            # Make the mock return our async generator when called
            mock_get_user_bundles.side_effect = mock_async_gen

            result = list(
                SubmissionBundle.get_user_submission_bundles(
                    evaluation_id=EVALUATION_ID,
                    synapse_client=self.syn,
                )
            )

            # THEN the service should be called with correct parameters
            mock_get_user_bundles.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND the result should contain SubmissionBundle objects
            assert len(result) == 1
            assert isinstance(result[0], SubmissionBundle)

            # Check bundle contents
            assert result[0].submission is not None
            assert result[0].submission.id == "789"
            assert result[0].submission.name == "User Submission 1"
            assert result[0].submission_status is not None
            assert result[0].submission_status.id == "789"
            assert result[0].submission_status.status == "VALIDATED"
            assert result[0].submission_status.evaluation_id == EVALUATION_ID

    def test_get_user_submission_bundles_default_params(self) -> None:
        """Test getting user submission bundles with default parameters using sync method."""
        # GIVEN mock response
        mock_response = {"results": []}

        # WHEN I call get_user_submission_bundles with defaults (sync method)
        with patch(
            "synapseclient.api.evaluation_services.get_user_submission_bundles"
        ) as mock_get_user_bundles:
            # Create an async generator function that yields no data
            async def mock_async_gen(*args, **kwargs):
                return
                yield  # This will never execute

            # Make the mock return our async generator when called
            mock_get_user_bundles.side_effect = mock_async_gen

            result = list(
                SubmissionBundle.get_user_submission_bundles(
                    evaluation_id=EVALUATION_ID,
                    synapse_client=self.syn,
                )
            )

            # THEN the service should be called with default parameters
            mock_get_user_bundles.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND the result should be empty
            assert len(result) == 0

    def test_dataclass_equality(self) -> None:
        """Test dataclass equality comparison."""
        # GIVEN two SubmissionBundle objects with the same data
        submission = Submission(id=SUBMISSION_ID, entity_id=ENTITY_ID)
        status = SubmissionStatus(id=SUBMISSION_STATUS_ID, status=STATUS)

        bundle1 = SubmissionBundle(submission=submission, submission_status=status)
        bundle2 = SubmissionBundle(submission=submission, submission_status=status)

        # THEN they should be equal
        assert bundle1 == bundle2

        # WHEN I modify one of them
        bundle2.submission_status = SubmissionStatus(id="different", status="DIFFERENT")

        # THEN they should not be equal
        assert bundle1 != bundle2

    def test_dataclass_equality_with_none(self) -> None:
        """Test dataclass equality with None values."""
        # GIVEN two SubmissionBundle objects with None values
        bundle1 = SubmissionBundle(submission=None, submission_status=None)
        bundle2 = SubmissionBundle(submission=None, submission_status=None)

        # THEN they should be equal
        assert bundle1 == bundle2

        # WHEN I add a submission to one
        bundle2.submission = Submission(id=SUBMISSION_ID)

        # THEN they should not be equal
        assert bundle1 != bundle2

    def test_repr_and_str(self) -> None:
        """Test string representation of SubmissionBundle."""
        # GIVEN a SubmissionBundle with some data
        submission = Submission(id=SUBMISSION_ID, entity_id=ENTITY_ID)
        status = SubmissionStatus(id=SUBMISSION_STATUS_ID, status=STATUS)
        bundle = SubmissionBundle(submission=submission, submission_status=status)

        # WHEN I get the string representation
        repr_str = repr(bundle)
        str_str = str(bundle)

        # THEN it should contain relevant information
        assert "SubmissionBundle" in repr_str
        assert SUBMISSION_ID in repr_str
        assert SUBMISSION_STATUS_ID in repr_str

        # AND str should be the same as repr for dataclasses
        assert str_str == repr_str

    def test_repr_with_none_values(self) -> None:
        """Test string representation with None values."""
        # GIVEN a SubmissionBundle with None values
        bundle = SubmissionBundle(submission=None, submission_status=None)

        # WHEN I get the string representation
        repr_str = repr(bundle)

        # THEN it should show None values
        assert "SubmissionBundle" in repr_str
        assert "submission=None" in repr_str
        assert "submission_status=None" in repr_str

    def test_protocol_implementation(self) -> None:
        """Test that SubmissionBundle implements the synchronous protocol correctly."""
        # THEN it should have all the required synchronous methods
        assert hasattr(SubmissionBundle, "get_evaluation_submission_bundles")
        assert hasattr(SubmissionBundle, "get_user_submission_bundles")

        # AND the methods should be callable
        assert callable(SubmissionBundle.get_evaluation_submission_bundles)
        assert callable(SubmissionBundle.get_user_submission_bundles)
