"""Integration tests for the synapseclient.models.SubmissionStatus class async methods."""

import os
import tempfile
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.annotations import from_submission_status_annotations
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Evaluation, File, Project, Submission, SubmissionStatus


class TestSubmissionStatusRetrieval:
    """Tests for retrieving SubmissionStatus objects async."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission status tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission status tests",
            content_source=project_model.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest.fixture(scope="function")
    async def test_file(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission status tests."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission status testing.")
            temp_file_path = temp_file.name

        try:
            file = File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=project_model.id,
            )
            stored_file = await file.store_async(synapse_client=syn)
            schedule_for_cleanup(stored_file.id)
            return stored_file
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    @pytest.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        """Create a test submission for status tests."""
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=syn)
        schedule_for_cleanup(created_submission.id)
        return created_submission

    async def test_get_submission_status_by_id(
        self, test_submission: Submission, test_evaluation: Evaluation
    ):
        """Test retrieving a submission status by ID async."""
        # WHEN I get a submission status by ID
        submission_status = await SubmissionStatus(id=test_submission.id).get_async(
            synapse_client=self.syn
        )

        # THEN the submission status should be retrieved correctly
        assert submission_status.id == test_submission.id
        assert submission_status.entity_id == test_submission.entity_id
        assert submission_status.evaluation_id == test_evaluation.id
        assert (
            submission_status.status is not None
        )  # Should have some status (e.g., "RECEIVED")
        assert submission_status.etag is not None
        assert submission_status.status_version is not None
        assert submission_status.modified_on is not None

    async def test_get_submission_status_without_id(self):
        """Test that getting a submission status without ID raises ValueError async."""
        # WHEN I try to get a submission status without an ID
        submission_status = SubmissionStatus()

        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="The submission status must have an ID to get"
        ):
            await submission_status.get_async(synapse_client=self.syn)

    async def test_get_submission_status_with_invalid_id(self):
        """Test that getting a submission status with invalid ID raises exception async."""
        # WHEN I try to get a submission status with an invalid ID
        submission_status = SubmissionStatus(id="syn999999999999")

        # THEN it should raise a SynapseHTTPError (404)
        with pytest.raises(SynapseHTTPError):
            await submission_status.get_async(synapse_client=self.syn)


class TestSubmissionStatusUpdates:
    """Tests for updating SubmissionStatus objects async."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission status tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission status tests",
            content_source=project_model.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest.fixture(scope="function")
    async def test_file(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission status tests."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission status testing.")
            temp_file_path = temp_file.name

        try:
            file = File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=project_model.id,
            )
            stored_file = await file.store_async(synapse_client=syn)
            schedule_for_cleanup(stored_file.id)
            return stored_file
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    @pytest.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        """Create a test submission for status tests."""
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=syn)
        schedule_for_cleanup(created_submission.id)
        return created_submission

    @pytest.fixture(scope="function")
    async def test_submission_status(
        self, test_submission: Submission
    ) -> SubmissionStatus:
        """Create a test submission status by getting the existing one."""
        submission_status = await SubmissionStatus(id=test_submission.id).get_async(
            synapse_client=self.syn
        )
        return submission_status

    async def test_store_submission_status_with_status_change(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with a status change async."""
        # GIVEN a submission status that exists
        original_status = test_submission_status.status
        original_etag = test_submission_status.etag
        original_status_version = test_submission_status.status_version

        # WHEN I update the status
        test_submission_status.status = "VALIDATED"
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN the submission status should be updated
        assert updated_status.id == test_submission_status.id
        assert updated_status.status == "VALIDATED"
        assert updated_status.status != original_status
        assert updated_status.etag != original_etag  # etag should change
        assert updated_status.status_version > original_status_version

    async def test_store_submission_status_with_submission_annotations(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with submission annotations async."""
        # WHEN I add submission annotations and store
        test_submission_status.submission_annotations = {
            "score": 85.5,
            "feedback": "Good work!",
        }
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN the submission annotations should be saved
        assert updated_status.submission_annotations is not None
        assert "score" in updated_status.submission_annotations
        assert updated_status.submission_annotations["score"] == [85.5]
        assert updated_status.submission_annotations["feedback"] == ["Good work!"]

    async def test_store_submission_status_with_legacy_annotations(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with legacy annotations async."""
        # WHEN I add legacy annotations and store
        test_submission_status.annotations = {
            "internal_score": 92.3,
            "reviewer_notes": "Excellent submission",
        }
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )
        assert updated_status.annotations is not None

        converted_annotations = from_submission_status_annotations(
            updated_status.annotations
        )

        # THEN the legacy annotations should be saved
        assert "internal_score" in converted_annotations
        assert converted_annotations["internal_score"] == 92.3
        assert converted_annotations["reviewer_notes"] == "Excellent submission"

    async def test_store_submission_status_with_combined_annotations(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with both types of annotations async."""
        # WHEN I add both submission and legacy annotations
        test_submission_status.submission_annotations = {
            "public_score": 78.0,
            "category": "Bronze",
        }
        test_submission_status.annotations = {
            "internal_review": True,
            "notes": "Needs minor improvements",
        }
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN both types of annotations should be saved
        assert updated_status.submission_annotations is not None
        assert "public_score" in updated_status.submission_annotations
        assert updated_status.submission_annotations["public_score"] == [78.0]

        assert updated_status.annotations is not None
        converted_annotations = from_submission_status_annotations(
            updated_status.annotations
        )
        assert "internal_review" in converted_annotations
        assert converted_annotations["internal_review"] == "true"

    async def test_store_submission_status_with_private_annotations_false(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with private_status_annotations set to False async."""
        # WHEN I add legacy annotations with private_status_annotations set to False
        test_submission_status.annotations = {
            "public_internal_score": 88.5,
            "public_notes": "This should be visible",
        }
        test_submission_status.private_status_annotations = False

        # WHEN I store the submission status
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN they should be properly stored
        assert updated_status.annotations is not None
        converted_annotations = from_submission_status_annotations(
            updated_status.annotations
        )
        assert "public_internal_score" in converted_annotations
        assert converted_annotations["public_internal_score"] == 88.5
        assert converted_annotations["public_notes"] == "This should be visible"

        # AND the annotations should be marked as not private
        for annos_type in ["stringAnnos", "doubleAnnos"]:
            annotations = updated_status.annotations[annos_type]
            assert all(not anno["isPrivate"] for anno in annotations)

    async def test_store_submission_status_with_private_annotations_true(
        self, test_submission_status: SubmissionStatus
    ):
        """Test updating a submission status with private_status_annotations set to True (default) async."""
        # WHEN I add legacy annotations with private_status_annotations set to True (default)
        test_submission_status.annotations = {
            "private_internal_score": 95.0,
            "private_notes": "This should be private",
        }
        test_submission_status.private_status_annotations = True

        # AND I create the request body to inspect it
        request_body = test_submission_status.to_synapse_request(
            synapse_client=self.syn
        )

        # WHEN I store the submission status
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN they should be properly stored
        assert updated_status.annotations is not None
        converted_annotations = from_submission_status_annotations(
            updated_status.annotations
        )
        assert "private_internal_score" in converted_annotations
        assert converted_annotations["private_internal_score"] == 95.0
        assert converted_annotations["private_notes"] == "This should be private"

        # AND the annotations should be marked as private
        for annos_type in ["stringAnnos", "doubleAnnos"]:
            annotations = updated_status.annotations[annos_type]
            print(annotations)
            assert all(anno["isPrivate"] for anno in annotations)

    async def test_store_submission_status_without_id(self):
        """Test that storing a submission status without ID raises ValueError async."""
        # WHEN I try to store a submission status without an ID
        submission_status = SubmissionStatus(status="SCORED")

        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="The submission status must have an ID to update"
        ):
            await submission_status.store_async(synapse_client=self.syn)

    async def test_store_submission_status_without_changes(
        self, test_submission_status: SubmissionStatus
    ):
        """Test that storing a submission status without changes shows warning async."""
        # GIVEN a submission status that hasn't been modified
        # (it already has _last_persistent_instance set from get())

        # WHEN I try to store it without making changes
        result = await test_submission_status.store_async(synapse_client=self.syn)

        # THEN it should return the same instance (no update sent to Synapse)
        assert result is test_submission_status

    async def test_store_submission_status_change_tracking(
        self, test_submission_status: SubmissionStatus
    ):
        """Test that change tracking works correctly async."""
        # GIVEN a submission status that was retrieved (has_changed should be False)
        assert not test_submission_status.has_changed

        # WHEN I make a change
        test_submission_status.status = "SCORED"

        # THEN has_changed should be True
        assert test_submission_status.has_changed

        # WHEN I store the changes
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )

        # THEN has_changed should be False again
        assert not updated_status.has_changed

    async def test_has_changed_property_edge_cases(
        self, test_submission_status: SubmissionStatus
    ):
        """Test the has_changed property with various edge cases and detailed scenarios async."""
        # GIVEN a submission status that was just retrieved
        assert not test_submission_status.has_changed
        original_annotations = (
            test_submission_status.annotations.copy()
            if test_submission_status.annotations
            else {}
        )

        # WHEN I modify only annotations (not submission_annotations)
        test_submission_status.annotations = {"test_key": "test_value"}

        # THEN has_changed should be True
        assert test_submission_status.has_changed

        # WHEN I reset annotations to the original value (should be the same as the persistent instance)
        test_submission_status.annotations = original_annotations

        # THEN has_changed should be False (same as original)
        assert not test_submission_status.has_changed

        # WHEN I add a different annotation value
        test_submission_status.annotations = {"different_key": "different_value"}

        # THEN has_changed should be True
        assert test_submission_status.has_changed

        # WHEN I store and get a fresh copy
        updated_status = await test_submission_status.store_async(
            synapse_client=self.syn
        )
        fresh_status = await SubmissionStatus(id=updated_status.id).get_async(
            synapse_client=self.syn
        )

        # THEN the fresh copy should not have changes
        assert not fresh_status.has_changed

        # WHEN I modify only submission_annotations
        fresh_status.submission_annotations = {"new_key": ["new_value"]}

        # THEN has_changed should be True
        assert fresh_status.has_changed

        # WHEN I modify a scalar field
        fresh_status.status = "VALIDATED"

        # THEN has_changed should still be True
        assert fresh_status.has_changed


class TestSubmissionStatusBulkOperations:
    """Tests for bulk SubmissionStatus operations async."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission status tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission status tests",
            content_source=project_model.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest.fixture(scope="function")
    async def test_files(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> list[File]:
        """Create multiple test files for submission status tests."""
        files = []
        for i in range(3):
            # Create a temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt"
            ) as temp_file:
                temp_file.write(
                    f"This is test content {i} for submission status testing."
                )
                temp_file_path = temp_file.name

            try:
                file = File(
                    path=temp_file_path,
                    name=f"test_file_{i}_{uuid.uuid4()}.txt",
                    parent_id=project_model.id,
                )
                stored_file = await file.store_async(synapse_client=syn)
                schedule_for_cleanup(stored_file.id)
                files.append(stored_file)
            finally:
                # Clean up the temporary file
                os.unlink(temp_file_path)

        return files

    @pytest.fixture(scope="function")
    async def test_submissions(
        self,
        test_evaluation: Evaluation,
        test_files: list[File],
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> list[Submission]:
        """Create multiple test submissions for status tests."""
        submissions = []
        for i, file in enumerate(test_files):
            submission = Submission(
                entity_id=file.id,
                evaluation_id=test_evaluation.id,
                name=f"Test Submission {i} {uuid.uuid4()}",
            )
            created_submission = await submission.store_async(synapse_client=syn)
            schedule_for_cleanup(created_submission.id)
            submissions.append(created_submission)
        return submissions

    async def test_get_all_submission_statuses(
        self, test_evaluation: Evaluation, test_submissions: list[Submission]
    ):
        """Test getting all submission statuses for an evaluation async."""
        # WHEN I get all submission statuses for the evaluation
        statuses = await SubmissionStatus.get_all_submission_statuses_async(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN I should get submission statuses for all submissions
        assert len(statuses) >= len(test_submissions)
        status_ids = [status.id for status in statuses]

        # AND all test submissions should have their statuses in the results
        for submission in test_submissions:
            assert submission.id in status_ids

        # AND each status should have proper attributes
        for status in statuses:
            assert status.id is not None
            assert status.evaluation_id == test_evaluation.id
            assert status.status is not None
            assert status.etag is not None

    async def test_get_all_submission_statuses_with_status_filter(
        self, test_evaluation: Evaluation, test_submissions: list[Submission]
    ):
        """Test getting submission statuses with status filter async."""
        # WHEN I get submission statuses filtered by status
        statuses = await SubmissionStatus.get_all_submission_statuses_async(
            evaluation_id=test_evaluation.id,
            status="RECEIVED",
            synapse_client=self.syn,
        )

        # THEN I should only get statuses with the specified status
        for status in statuses:
            assert status.status == "RECEIVED"
            assert status.evaluation_id == test_evaluation.id

    async def test_get_all_submission_statuses_with_pagination(
        self, test_evaluation: Evaluation, test_submissions: list[Submission]
    ):
        """Test getting submission statuses with pagination async."""
        # WHEN I get submission statuses with pagination
        statuses_page1 = await SubmissionStatus.get_all_submission_statuses_async(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=0,
            synapse_client=self.syn,
        )

        # THEN I should get at most 2 statuses
        assert len(statuses_page1) <= 2

        # WHEN I get the next page
        statuses_page2 = await SubmissionStatus.get_all_submission_statuses_async(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=2,
            synapse_client=self.syn,
        )

        # THEN the results should be different (assuming more than 2 submissions exist)
        if len(statuses_page1) == 2 and len(statuses_page2) > 0:
            page1_ids = {status.id for status in statuses_page1}
            page2_ids = {status.id for status in statuses_page2}
            assert page1_ids != page2_ids  # Should be different sets

    async def test_batch_update_submission_statuses(
        self, test_evaluation: Evaluation, test_submissions: list[Submission]
    ):
        """Test batch updating multiple submission statuses async."""
        # GIVEN multiple submission statuses
        statuses = []
        for submission in test_submissions:
            status = await SubmissionStatus(id=submission.id).get_async(
                synapse_client=self.syn
            )
            # Update each status
            status.status = "VALIDATED"
            status.submission_annotations = {
                "batch_score": 90.0 + (len(statuses) * 2),
                "batch_processed": True,
            }
            statuses.append(status)

        # WHEN I batch update the statuses
        response = await SubmissionStatus.batch_update_submission_statuses_async(
            evaluation_id=test_evaluation.id,
            statuses=statuses,
            synapse_client=self.syn,
        )

        # THEN the batch update should succeed
        assert response is not None
        assert "batchToken" in response or response == {}  # Response format may vary

        # AND I should be able to verify the updates by retrieving the statuses
        for original_status in statuses:
            updated_status = await SubmissionStatus(id=original_status.id).get_async(
                synapse_client=self.syn
            )
            assert updated_status.status == "VALIDATED"
            converted_submission_annotations = from_submission_status_annotations(
                updated_status.submission_annotations
            )
            assert "batch_score" in converted_submission_annotations
            assert converted_submission_annotations["batch_processed"] == ["true"]


class TestSubmissionStatusCancellation:
    """Tests for SubmissionStatus cancellation functionality async."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission status tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission status tests",
            content_source=project_model.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest.fixture(scope="function")
    async def test_file(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission status tests."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission status testing.")
            temp_file_path = temp_file.name

        try:
            file = File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=project_model.id,
            )
            stored_file = await file.store_async(synapse_client=syn)
            schedule_for_cleanup(stored_file.id)
            return stored_file
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    @pytest.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        """Create a test submission for status tests."""
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=syn)
        schedule_for_cleanup(created_submission.id)
        return created_submission

    async def test_submission_cancellation_workflow(self, test_submission: Submission):
        """Test the complete submission cancellation workflow async."""
        # GIVEN a submission that exists
        submission_id = test_submission.id

        # WHEN I get the initial submission status
        initial_status = await SubmissionStatus(id=submission_id).get_async(
            synapse_client=self.syn
        )

        # THEN initially it should not be cancellable or cancelled
        assert initial_status.can_cancel is False
        assert initial_status.cancel_requested is False

        # WHEN I update the submission status to allow cancellation
        initial_status.can_cancel = True
        updated_status = await initial_status.store_async(synapse_client=self.syn)

        # THEN the submission should be marked as cancellable
        assert updated_status.can_cancel is True
        assert updated_status.cancel_requested is False

        # WHEN I cancel the submission
        await test_submission.cancel_async()

        # THEN I should be able to retrieve the updated status showing cancellation was requested
        final_status = await SubmissionStatus(id=submission_id).get_async(
            synapse_client=self.syn
        )
        assert final_status.can_cancel is True
        assert final_status.cancel_requested is True


class TestSubmissionStatusValidation:
    """Tests for SubmissionStatus validation and error handling async."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_to_synapse_request_missing_required_attributes(self):
        """Test that to_synapse_request validates required attributes async."""
        # WHEN I try to create a request with missing required attributes
        submission_status = SubmissionStatus(id="123")  # Missing etag, status_version

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'etag' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

        # WHEN I add etag but still missing status_version
        submission_status.etag = "some-etag"

        # THEN it should raise a ValueError for status_version
        with pytest.raises(ValueError, match="missing the 'status_version' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    async def test_to_synapse_request_with_annotations_missing_evaluation_id(self):
        """Test that annotations require evaluation_id async."""
        # WHEN I try to create a request with annotations but no evaluation_id
        submission_status = SubmissionStatus(
            id="123", etag="some-etag", status_version=1, annotations={"test": "value"}
        )

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'evaluation_id' attribute"):
            submission_status.to_synapse_request(synapse_client=self.syn)

    async def test_to_synapse_request_valid_attributes(self):
        """Test that to_synapse_request works with valid attributes async."""
        # WHEN I create a request with all required attributes
        submission_status = SubmissionStatus(
            id="123",
            etag="some-etag",
            status_version=1,
            status="SCORED",
            evaluation_id="eval123",
            submission_annotations={"score": 85.5},
        )

        # THEN it should create a valid request body
        request_body = submission_status.to_synapse_request(synapse_client=self.syn)

        # AND the request should have the required fields
        assert request_body["id"] == "123"
        assert request_body["etag"] == "some-etag"
        assert request_body["statusVersion"] == 1
        assert request_body["status"] == "SCORED"
        assert "submissionAnnotations" in request_body

    async def test_fill_from_dict_with_complete_response(self):
        """Test filling a SubmissionStatus from a complete API response async."""
        # GIVEN a complete API response
        api_response = {
            "id": "123456",
            "etag": "abcd-1234",
            "modifiedOn": "2023-01-01T00:00:00.000Z",
            "status": "SCORED",
            "entityId": "syn789",
            "versionNumber": 1,
            "statusVersion": 2,
            "canCancel": False,
            "cancelRequested": False,
            "annotations": {
                "objectId": "123456",
                "scopeId": "9617645",
                "stringAnnos": [
                    {
                        "key": "internal_note",
                        "isPrivate": True,
                        "value": "This is internal",
                    },
                    {
                        "key": "reviewer_notes",
                        "isPrivate": True,
                        "value": "Excellent work",
                    },
                ],
                "doubleAnnos": [
                    {"key": "validation_score", "isPrivate": True, "value": 95.0}
                ],
                "longAnnos": [],
            },
            "submissionAnnotations": {"feedback": ["Great work!"], "score": [92.5]},
        }

        # WHEN I fill a SubmissionStatus from the response
        submission_status = SubmissionStatus()
        result = submission_status.fill_from_dict(api_response)

        # THEN all fields should be populated correctly
        assert result.id == "123456"
        assert result.etag == "abcd-1234"
        assert result.modified_on == "2023-01-01T00:00:00.000Z"
        assert result.status == "SCORED"
        assert result.entity_id == "syn789"
        assert result.version_number == 1
        assert result.status_version == 2
        assert result.can_cancel is False
        assert result.cancel_requested is False

        # The annotations field should contain the raw submission status format
        assert result.annotations is not None
        assert "objectId" in result.annotations
        assert "scopeId" in result.annotations
        assert "stringAnnos" in result.annotations
        assert "doubleAnnos" in result.annotations
        assert len(result.annotations["stringAnnos"]) == 2
        assert len(result.annotations["doubleAnnos"]) == 1

        # The submission_annotations should be in simple key-value format
        assert "feedback" in result.submission_annotations
        assert "score" in result.submission_annotations
        assert result.submission_annotations["feedback"] == ["Great work!"]
        assert result.submission_annotations["score"] == [92.5]

    async def test_fill_from_dict_with_minimal_response(self):
        """Test filling a SubmissionStatus from a minimal API response async."""
        # GIVEN a minimal API response
        api_response = {"id": "123456", "status": "RECEIVED"}

        # WHEN I fill a SubmissionStatus from the response
        submission_status = SubmissionStatus()
        result = submission_status.fill_from_dict(api_response)

        # THEN basic fields should be populated
        assert result.id == "123456"
        assert result.status == "RECEIVED"
        # AND optional fields should have default values
        assert result.etag is None
        assert result.can_cancel is False
        assert result.cancel_requested is False
