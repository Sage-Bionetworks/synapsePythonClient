"""Unit tests for the synapseclient.models.SubmissionStatus class."""

from typing import Dict, Union
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models import SubmissionStatus

SUBMISSION_STATUS_ID = "9999999"
ENTITY_ID = "syn123456"
EVALUATION_ID = "9614543"
ETAG = "etag_value"
MODIFIED_ON = "2023-01-01T00:00:00.000Z"
STATUS = "RECEIVED"
SCORE = 85.5
REPORT = "Test report"
VERSION_NUMBER = 1
STATUS_VERSION = 1
CAN_CANCEL = False
CANCEL_REQUESTED = False
PRIVATE_STATUS_ANNOTATIONS = True


class TestSubmissionStatus:
    """Tests for the synapseclient.models.SubmissionStatus class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_submission_status_dict(
        self,
    ) -> Dict[str, Union[str, int, bool, Dict]]:
        """Return example submission status data from REST API."""
        return {
            "id": SUBMISSION_STATUS_ID,
            "etag": ETAG,
            "modifiedOn": MODIFIED_ON,
            "status": STATUS,
            "score": SCORE,
            "report": REPORT,
            "entityId": ENTITY_ID,
            "versionNumber": VERSION_NUMBER,
            "statusVersion": STATUS_VERSION,
            "canCancel": CAN_CANCEL,
            "cancelRequested": CANCEL_REQUESTED,
            "annotations": {
                "objectId": SUBMISSION_STATUS_ID,
                "scopeId": EVALUATION_ID,
                "stringAnnos": [
                    {
                        "key": "internal_note",
                        "isPrivate": True,
                        "value": "This is internal",
                    }
                ],
                "doubleAnnos": [
                    {"key": "validation_score", "isPrivate": True, "value": 95.0}
                ],
                "longAnnos": [],
            },
            "submissionAnnotations": {"feedback": ["Great work!"], "score": [92.5]},
        }

    def get_example_submission_dict(self) -> Dict[str, str]:
        """Return example submission data from REST API."""
        return {
            "id": SUBMISSION_STATUS_ID,
            "evaluationId": EVALUATION_ID,
            "entityId": ENTITY_ID,
            "versionNumber": VERSION_NUMBER,
            "userId": "123456",
            "submitterAlias": "test_user",
            "createdOn": "2023-01-01T00:00:00.000Z",
        }

    def test_init_submission_status(self) -> None:
        """Test creating a SubmissionStatus with basic attributes."""
        # WHEN I create a SubmissionStatus object
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
        )

        # THEN the SubmissionStatus should have the expected attributes
        assert submission_status.id == SUBMISSION_STATUS_ID
        assert submission_status.status == STATUS
        assert submission_status.entity_id == ENTITY_ID
        assert submission_status.evaluation_id == EVALUATION_ID
        assert submission_status.can_cancel is False  # default value
        assert submission_status.cancel_requested is False  # default value
        assert submission_status.private_status_annotations is True  # default value

    def test_fill_from_dict(self) -> None:
        """Test filling a SubmissionStatus from a REST API response."""
        # GIVEN an example submission status response
        submission_status_data = self.get_example_submission_status_dict()

        # WHEN I fill a SubmissionStatus from the response
        submission_status = SubmissionStatus().fill_from_dict(submission_status_data)

        # THEN all fields should be populated correctly
        assert submission_status.id == SUBMISSION_STATUS_ID
        assert submission_status.etag == ETAG
        assert submission_status.modified_on == MODIFIED_ON
        assert submission_status.status == STATUS
        assert submission_status.score == SCORE
        assert submission_status.report == REPORT
        assert submission_status.entity_id == ENTITY_ID
        assert submission_status.version_number == VERSION_NUMBER
        assert submission_status.status_version == STATUS_VERSION
        assert submission_status.can_cancel is CAN_CANCEL
        assert submission_status.cancel_requested is CANCEL_REQUESTED

        # Check annotations
        assert submission_status.annotations is not None
        assert "objectId" in submission_status.annotations
        assert "scopeId" in submission_status.annotations
        assert "stringAnnos" in submission_status.annotations
        assert "doubleAnnos" in submission_status.annotations

        # Check submission annotations
        assert "feedback" in submission_status.submission_annotations
        assert "score" in submission_status.submission_annotations
        assert submission_status.submission_annotations["feedback"] == ["Great work!"]
        assert submission_status.submission_annotations["score"] == [92.5]

    def test_fill_from_dict_minimal(self) -> None:
        """Test filling a SubmissionStatus from minimal REST API response."""
        # GIVEN a minimal submission status response
        minimal_data = {"id": SUBMISSION_STATUS_ID, "status": STATUS}

        # WHEN I fill a SubmissionStatus from the response
        submission_status = SubmissionStatus().fill_from_dict(minimal_data)

        # THEN basic fields should be populated
        assert submission_status.id == SUBMISSION_STATUS_ID
        assert submission_status.status == STATUS
        # AND optional fields should have default values
        assert submission_status.etag is None
        assert submission_status.can_cancel is False
        assert submission_status.cancel_requested is False

    async def test_get_async(self) -> None:
        """Test retrieving a SubmissionStatus by ID."""
        # GIVEN a SubmissionStatus with an ID
        submission_status = SubmissionStatus(id=SUBMISSION_STATUS_ID)

        # WHEN I call get_async
        with patch(
            "synapseclient.api.evaluation_services.get_submission_status",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_status_dict(),
        ) as mock_get_status, patch(
            "synapseclient.api.evaluation_services.get_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_dict(),
        ) as mock_get_submission:
            result = await submission_status.get_async(synapse_client=self.syn)

            # THEN the submission status should be retrieved
            mock_get_status.assert_called_once_with(
                submission_id=SUBMISSION_STATUS_ID, synapse_client=self.syn
            )
            mock_get_submission.assert_called_once_with(
                submission_id=SUBMISSION_STATUS_ID, synapse_client=self.syn
            )

            # AND the result should have the expected data
            assert result.id == SUBMISSION_STATUS_ID
            assert result.status == STATUS
            assert result.evaluation_id == EVALUATION_ID
            assert result._last_persistent_instance is not None

    async def test_get_async_without_id(self) -> None:
        """Test that getting a SubmissionStatus without ID raises ValueError."""
        # GIVEN a SubmissionStatus without an ID
        submission_status = SubmissionStatus()

        # WHEN I call get_async
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="The submission status must have an ID to get"
        ):
            await submission_status.get_async(synapse_client=self.syn)

    async def test_store_async(self) -> None:
        """Test storing a SubmissionStatus."""
        # GIVEN a SubmissionStatus with required attributes
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            etag=ETAG,
            status_version=STATUS_VERSION,
            status="SCORED",
            evaluation_id=EVALUATION_ID,
        )
        submission_status._set_last_persistent_instance()

        # AND I modify the status
        submission_status.status = "VALIDATED"

        # WHEN I call store_async
        with patch(
            "synapseclient.api.evaluation_services.update_submission_status",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_status_dict(),
        ) as mock_update:
            result = await submission_status.store_async(synapse_client=self.syn)

            # THEN the submission status should be updated
            mock_update.assert_called_once()
            call_args = mock_update.call_args
            assert call_args.kwargs["submission_id"] == SUBMISSION_STATUS_ID
            assert call_args.kwargs["synapse_client"] == self.syn

            # AND the result should have updated data
            assert result.id == SUBMISSION_STATUS_ID
            assert result.status == STATUS  # from mock response
            assert result._last_persistent_instance is not None

    async def test_store_async_without_id(self) -> None:
        """Test that storing a SubmissionStatus without ID raises ValueError."""
        # GIVEN a SubmissionStatus without an ID
        submission_status = SubmissionStatus(status="SCORED")

        # WHEN I call store_async
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="The submission status must have an ID to update"
        ):
            await submission_status.store_async(synapse_client=self.syn)

    async def test_store_async_without_changes(self) -> None:
        """Test storing a SubmissionStatus without changes."""
        # GIVEN a SubmissionStatus that hasn't been modified
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            etag=ETAG,
            status_version=STATUS_VERSION,
            status=STATUS,
        )
        submission_status._set_last_persistent_instance()

        # WHEN I call store_async without making changes
        result = await submission_status.store_async(synapse_client=self.syn)

        # THEN it should return the same instance (no update sent to Synapse)
        assert result is submission_status

    def test_to_synapse_request_missing_id(self) -> None:
        """Test to_synapse_request with missing ID."""
        # GIVEN a SubmissionStatus without an ID
        submission_status = SubmissionStatus()

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'id' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    def test_to_synapse_request_missing_etag(self) -> None:
        """Test to_synapse_request with missing etag."""
        # GIVEN a SubmissionStatus with ID but no etag
        submission_status = SubmissionStatus(id=SUBMISSION_STATUS_ID)

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'etag' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    def test_to_synapse_request_missing_status_version(self) -> None:
        """Test to_synapse_request with missing status_version."""
        # GIVEN a SubmissionStatus with ID and etag but no status_version
        submission_status = SubmissionStatus(id=SUBMISSION_STATUS_ID, etag=ETAG)

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'status_version' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    def test_to_synapse_request_missing_evaluation_id_with_annotations(self) -> None:
        """Test to_synapse_request with annotations but missing evaluation_id."""
        # GIVEN a SubmissionStatus with annotations but no evaluation_id
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            etag=ETAG,
            status_version=STATUS_VERSION,
            annotations={"test": "value"},
        )

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'evaluation_id' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    def test_to_synapse_request_valid(self) -> None:
        """Test to_synapse_request with valid attributes."""
        # GIVEN a SubmissionStatus with all required attributes
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            etag=ETAG,
            status_version=STATUS_VERSION,
            status="SCORED",
            evaluation_id=EVALUATION_ID,
            submission_annotations={"score": 85.5},
            annotations={"internal_note": "test"},
        )

        # WHEN I call to_synapse_request
        request_body = submission_status.to_synapse_request(synapse_client=self.syn)

        # THEN the request should have the required fields
        assert request_body["id"] == SUBMISSION_STATUS_ID
        assert request_body["etag"] == ETAG
        assert request_body["statusVersion"] == STATUS_VERSION
        assert request_body["status"] == "SCORED"
        assert "submissionAnnotations" in request_body
        assert "annotations" in request_body

    def test_has_changed_property_new_instance(self) -> None:
        """Test has_changed property for a new instance."""
        # GIVEN a new SubmissionStatus instance
        submission_status = SubmissionStatus(id=SUBMISSION_STATUS_ID)

        # THEN has_changed should be True (no persistent instance)
        assert submission_status.has_changed is True

    def test_has_changed_property_after_get(self) -> None:
        """Test has_changed property after retrieving from Synapse."""
        # GIVEN a SubmissionStatus that was retrieved (has persistent instance)
        submission_status = SubmissionStatus(id=SUBMISSION_STATUS_ID, status=STATUS)
        submission_status._set_last_persistent_instance()

        # THEN has_changed should be False
        assert submission_status.has_changed is False

        # WHEN I modify a field
        submission_status.status = "VALIDATED"

        # THEN has_changed should be True
        assert submission_status.has_changed is True

    def test_has_changed_property_annotations(self) -> None:
        """Test has_changed property with annotation changes."""
        # GIVEN a SubmissionStatus with annotations
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            annotations={"original": "value"},
            submission_annotations={"score": 85.0},
        )
        submission_status._set_last_persistent_instance()

        # THEN has_changed should be False initially
        assert submission_status.has_changed is False

        # WHEN I modify annotations
        submission_status.annotations = {"modified": "value"}

        # THEN has_changed should be True
        assert submission_status.has_changed is True

        # WHEN I reset annotations to original and modify submission_annotations
        submission_status.annotations = {"original": "value"}
        submission_status.submission_annotations = {"score": 90.0}

        # THEN has_changed should still be True
        assert submission_status.has_changed is True

    async def test_get_all_submission_statuses_async(self) -> None:
        """Test getting all submission statuses for an evaluation."""
        # GIVEN mock response data
        mock_response = {
            "results": [
                {
                    "id": "123",
                    "status": "RECEIVED",
                    "entityId": ENTITY_ID,
                    "evaluationId": EVALUATION_ID,
                },
                {
                    "id": "456",
                    "status": "SCORED",
                    "entityId": ENTITY_ID,
                    "evaluationId": EVALUATION_ID,
                },
            ]
        }

        # WHEN I call get_all_submission_statuses_async
        with patch(
            "synapseclient.api.evaluation_services.get_all_submission_statuses",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_get_all:
            result = await SubmissionStatus.get_all_submission_statuses_async(
                evaluation_id=EVALUATION_ID,
                status="RECEIVED",
                limit=50,
                offset=0,
                synapse_client=self.syn,
            )

            # THEN the service should be called with correct parameters
            mock_get_all.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                status="RECEIVED",
                limit=50,
                offset=0,
                synapse_client=self.syn,
            )

            # AND the result should contain SubmissionStatus objects
            assert len(result) == 2
            assert all(isinstance(status, SubmissionStatus) for status in result)
            assert result[0].id == "123"
            assert result[0].status == "RECEIVED"
            assert result[1].id == "456"
            assert result[1].status == "SCORED"

    async def test_batch_update_submission_statuses_async(self) -> None:
        """Test batch updating submission statuses."""
        # GIVEN a list of SubmissionStatus objects
        statuses = [
            SubmissionStatus(
                id="123",
                etag="etag1",
                status_version=1,
                status="VALIDATED",
                evaluation_id=EVALUATION_ID,
            ),
            SubmissionStatus(
                id="456",
                etag="etag2",
                status_version=1,
                status="SCORED",
                evaluation_id=EVALUATION_ID,
            ),
        ]

        # AND mock response
        mock_response = {"batchToken": "token123"}

        # WHEN I call batch_update_submission_statuses_async
        with patch(
            "synapseclient.api.evaluation_services.batch_update_submission_statuses",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_batch_update:
            result = await SubmissionStatus.batch_update_submission_statuses_async(
                evaluation_id=EVALUATION_ID,
                statuses=statuses,
                is_first_batch=True,
                is_last_batch=True,
                synapse_client=self.syn,
            )

            # THEN the service should be called with correct parameters
            mock_batch_update.assert_called_once()
            call_args = mock_batch_update.call_args
            assert call_args.kwargs["evaluation_id"] == EVALUATION_ID
            assert call_args.kwargs["synapse_client"] == self.syn

            # Check request body structure
            request_body = call_args.kwargs["request_body"]
            assert request_body["isFirstBatch"] is True
            assert request_body["isLastBatch"] is True
            assert "statuses" in request_body
            assert len(request_body["statuses"]) == 2

            # AND the result should be the mock response
            assert result == mock_response

    async def test_batch_update_with_batch_token(self) -> None:
        """Test batch update with batch token for subsequent batches."""
        # GIVEN a list of SubmissionStatus objects and a batch token
        statuses = [
            SubmissionStatus(
                id="123",
                etag="etag1",
                status_version=1,
                status="VALIDATED",
                evaluation_id=EVALUATION_ID,
            )
        ]
        batch_token = "previous_batch_token"

        # WHEN I call batch_update_submission_statuses_async with a batch token
        with patch(
            "synapseclient.api.evaluation_services.batch_update_submission_statuses",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_batch_update:
            await SubmissionStatus.batch_update_submission_statuses_async(
                evaluation_id=EVALUATION_ID,
                statuses=statuses,
                is_first_batch=False,
                is_last_batch=True,
                batch_token=batch_token,
                synapse_client=self.syn,
            )

            # THEN the batch token should be included in the request
            call_args = mock_batch_update.call_args
            request_body = call_args.kwargs["request_body"]
            assert request_body["batchToken"] == batch_token
            assert request_body["isFirstBatch"] is False

    def test_set_last_persistent_instance(self) -> None:
        """Test setting the last persistent instance."""
        # GIVEN a SubmissionStatus
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            annotations={"test": "value"},
        )

        # WHEN I set the last persistent instance
        submission_status._set_last_persistent_instance()

        # THEN the persistent instance should be set
        assert submission_status._last_persistent_instance is not None
        assert submission_status._last_persistent_instance.id == SUBMISSION_STATUS_ID
        assert submission_status._last_persistent_instance.status == STATUS
        assert submission_status._last_persistent_instance.annotations == {
            "test": "value"
        }

        # AND modifying the current instance shouldn't affect the persistent one
        submission_status.status = "MODIFIED"
        assert submission_status._last_persistent_instance.status == STATUS

    def test_dataclass_equality(self) -> None:
        """Test dataclass equality comparison."""
        # GIVEN two SubmissionStatus objects with the same data
        status1 = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            entity_id=ENTITY_ID,
        )
        status2 = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            entity_id=ENTITY_ID,
        )

        # THEN they should be equal
        assert status1 == status2

        # WHEN I modify one of them
        status2.status = "DIFFERENT"

        # THEN they should not be equal
        assert status1 != status2

    def test_dataclass_fields_excluded_from_comparison(self) -> None:
        """Test that certain fields are excluded from comparison."""
        # GIVEN two SubmissionStatus objects that differ only in comparison-excluded fields
        status1 = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            etag="etag1",
            modified_on="2023-01-01",
            cancel_requested=False,
        )
        status2 = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            etag="etag2",  # different etag
            modified_on="2023-01-02",  # different modified_on
            cancel_requested=True,  # different cancel_requested
        )

        # THEN they should still be equal (these fields are excluded from comparison)
        assert status1 == status2

    def test_repr_and_str(self) -> None:
        """Test string representation of SubmissionStatus."""
        # GIVEN a SubmissionStatus with some data
        submission_status = SubmissionStatus(
            id=SUBMISSION_STATUS_ID,
            status=STATUS,
            entity_id=ENTITY_ID,
        )

        # WHEN I get the string representation
        repr_str = repr(submission_status)
        str_str = str(submission_status)

        # THEN it should contain the relevant information
        assert SUBMISSION_STATUS_ID in repr_str
        assert STATUS in repr_str
        assert ENTITY_ID in repr_str
        assert "SubmissionStatus" in repr_str

        # AND str should be the same as repr for dataclasses
        assert str_str == repr_str
