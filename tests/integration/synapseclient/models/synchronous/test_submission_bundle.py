"""Integration tests for the synapseclient.models.SubmissionBundle class."""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Evaluation,
    File,
    Project,
    Submission,
    SubmissionBundle,
    SubmissionStatus,
)


class TestSubmissionBundleRetrieval:
    """Tests for retrieving SubmissionBundle objects."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        project = Project(name=f"test_project_{uuid.uuid4()}").store(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="Test evaluation for SubmissionBundle testing",
            content_source=test_project.id,
            submission_instructions_message="Submit your files here",
            submission_receipt_message="Thank you for your submission!",
        ).store(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    @pytest.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        file_content = f"Test file content for submission bundle tests {uuid.uuid4()}"
        with open("test_file_for_submission_bundle.txt", "w") as f:
            f.write(file_content)

        file_entity = File(
            path="test_file_for_submission_bundle.txt",
            name=f"test_submission_file_{uuid.uuid4()}",
            parent_id=test_project.id,
        ).store(synapse_client=syn)
        schedule_for_cleanup(file_entity.id)
        return file_entity

    @pytest.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        submission = Submission(
            name=f"test_submission_{uuid.uuid4()}",
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            submitter_alias="test_user_bundle",
        ).store(synapse_client=syn)
        schedule_for_cleanup(submission.id)
        return submission

    @pytest.fixture(scope="function")
    async def multiple_submissions(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> list[Submission]:
        """Create multiple submissions for testing pagination and filtering."""
        submissions = []
        for i in range(3):
            submission = Submission(
                name=f"test_submission_{uuid.uuid4()}_{i}",
                entity_id=test_file.id,
                evaluation_id=test_evaluation.id,
                submitter_alias=f"test_user_{i}",
            ).store(synapse_client=syn)
            schedule_for_cleanup(submission.id)
            submissions.append(submission)
        return submissions

    async def test_get_evaluation_submission_bundles_basic(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        """Test getting submission bundles for an evaluation."""
        # WHEN I get submission bundles for an evaluation
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN the bundles should be retrieved
        assert bundles is not None
        assert len(bundles) >= 1  # At least our test submission

        # AND each bundle should have proper structure
        found_test_bundle = False
        for bundle in bundles:
            assert isinstance(bundle, SubmissionBundle)
            assert bundle.submission is not None
            assert bundle.submission.id is not None
            assert bundle.submission.evaluation_id == test_evaluation.id

            if bundle.submission.id == test_submission.id:
                found_test_bundle = True
                assert bundle.submission.entity_id == test_submission.entity_id
                assert bundle.submission.name == test_submission.name

        # AND our test submission should be found
        assert found_test_bundle, "Test submission should be found in bundles"

    async def test_get_evaluation_submission_bundles_with_status_filter(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        """Test getting submission bundles filtered by status."""
        # WHEN I get submission bundles filtered by "RECEIVED" status
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            status="RECEIVED",
            synapse_client=self.syn,
        )

        # THEN the bundles should be retrieved
        assert bundles is not None

        # AND all bundles should have RECEIVED status (if any exist)
        for bundle in bundles:
            if bundle.submission_status:
                assert bundle.submission_status.status == "RECEIVED"

        # WHEN I attempt to get submission bundles with an invalid status
        with pytest.raises(SynapseHTTPError) as exc_info:
            SubmissionBundle.get_evaluation_submission_bundles(
                evaluation_id=test_evaluation.id,
                status="NONEXISTENT_STATUS",
                synapse_client=self.syn,
            )
        # THEN it should raise a SynapseHTTPError (400 for invalid enum)
        assert exc_info.value.response.status_code == 400
        assert "No enum constant" in str(exc_info.value)
        assert "NONEXISTENT_STATUS" in str(exc_info.value)

    async def test_get_evaluation_submission_bundles_with_pagination(
        self, test_evaluation: Evaluation, multiple_submissions: list[Submission]
    ):
        """Test pagination when getting submission bundles."""
        # WHEN I get submission bundles with a limit
        bundles_page1 = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=0,
            synapse_client=self.syn,
        )

        # THEN I should get at most 2 bundles
        assert bundles_page1 is not None
        assert len(bundles_page1) <= 2

        # WHEN I get the next page
        bundles_page2 = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=2,
            synapse_client=self.syn,
        )

        # THEN I should get different bundles (if there are more than 2 total)
        assert bundles_page2 is not None

        # AND the bundle IDs should not overlap if we have enough submissions
        if len(bundles_page1) == 2 and len(bundles_page2) > 0:
            page1_ids = {
                bundle.submission.id for bundle in bundles_page1 if bundle.submission
            }
            page2_ids = {
                bundle.submission.id for bundle in bundles_page2 if bundle.submission
            }
            assert page1_ids.isdisjoint(
                page2_ids
            ), "Pages should not have overlapping submissions"

    async def test_get_evaluation_submission_bundles_invalid_evaluation(self):
        """Test getting submission bundles for invalid evaluation ID."""
        # WHEN I try to get submission bundles for a non-existent evaluation
        with pytest.raises(SynapseHTTPError) as exc_info:
            SubmissionBundle.get_evaluation_submission_bundles(
                evaluation_id="syn999999999999",
                synapse_client=self.syn,
            )

        # THEN it should raise a SynapseHTTPError (likely 403 or 404)
        assert exc_info.value.response.status_code in [403, 404]

    async def test_get_user_submission_bundles_basic(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        """Test getting user submission bundles for an evaluation."""
        # WHEN I get user submission bundles for an evaluation
        bundles = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN the bundles should be retrieved
        assert bundles is not None
        assert len(bundles) >= 1  # At least our test submission

        # AND each bundle should have proper structure
        found_test_bundle = False
        for bundle in bundles:
            assert isinstance(bundle, SubmissionBundle)
            assert bundle.submission is not None
            assert bundle.submission.id is not None
            assert bundle.submission.evaluation_id == test_evaluation.id

            if bundle.submission.id == test_submission.id:
                found_test_bundle = True
                assert bundle.submission.entity_id == test_submission.entity_id
                assert bundle.submission.name == test_submission.name

        # AND our test submission should be found
        assert found_test_bundle, "Test submission should be found in user bundles"

    async def test_get_user_submission_bundles_with_pagination(
        self, test_evaluation: Evaluation, multiple_submissions: list[Submission]
    ):
        """Test pagination when getting user submission bundles."""
        # WHEN I get user submission bundles with a limit
        bundles_page1 = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=0,
            synapse_client=self.syn,
        )

        # THEN I should get at most 2 bundles
        assert bundles_page1 is not None
        assert len(bundles_page1) <= 2

        # WHEN I get the next page
        bundles_page2 = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            limit=2,
            offset=2,
            synapse_client=self.syn,
        )

        # THEN I should get different bundles (if there are more than 2 total)
        assert bundles_page2 is not None

        # AND the bundle IDs should not overlap if we have enough submissions
        if len(bundles_page1) == 2 and len(bundles_page2) > 0:
            page1_ids = {
                bundle.submission.id for bundle in bundles_page1 if bundle.submission
            }
            page2_ids = {
                bundle.submission.id for bundle in bundles_page2 if bundle.submission
            }
            assert page1_ids.isdisjoint(
                page2_ids
            ), "Pages should not have overlapping submissions"


class TestSubmissionBundleDataIntegrity:
    """Tests for data integrity and relationships in SubmissionBundle objects."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        project = Project(name=f"test_project_{uuid.uuid4()}").store(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="Test evaluation for data integrity testing",
            content_source=test_project.id,
            submission_instructions_message="Submit your files here",
            submission_receipt_message="Thank you for your submission!",
        ).store(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    @pytest.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        file_content = f"Test file content for data integrity tests {uuid.uuid4()}"
        with open("test_file_for_data_integrity.txt", "w") as f:
            f.write(file_content)

        file_entity = File(
            path="test_file_for_data_integrity.txt",
            name=f"test_integrity_file_{uuid.uuid4()}",
            parent_id=test_project.id,
        ).store(synapse_client=syn)
        schedule_for_cleanup(file_entity.id)
        return file_entity

    @pytest.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        submission = Submission(
            name=f"test_submission_{uuid.uuid4()}",
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            submitter_alias="test_user_integrity",
        ).store(synapse_client=syn)
        schedule_for_cleanup(submission.id)
        return submission

    async def test_submission_bundle_data_consistency(
        self, test_evaluation: Evaluation, test_submission: Submission, test_file: File
    ):
        """Test that submission bundles maintain data consistency between submission and status."""
        # WHEN I get submission bundles for the evaluation
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN I should find our test submission
        test_bundle = None
        for bundle in bundles:
            if bundle.submission and bundle.submission.id == test_submission.id:
                test_bundle = bundle
                break

        assert test_bundle is not None, "Test submission bundle should be found"

        # AND the submission data should be consistent
        assert test_bundle.submission.id == test_submission.id
        assert test_bundle.submission.entity_id == test_file.id
        assert test_bundle.submission.evaluation_id == test_evaluation.id
        assert test_bundle.submission.name == test_submission.name

        # AND if there's a submission status, it should reference the same entities
        if test_bundle.submission_status:
            assert test_bundle.submission_status.id == test_submission.id
            assert test_bundle.submission_status.entity_id == test_file.id
            assert test_bundle.submission_status.evaluation_id == test_evaluation.id

    async def test_submission_bundle_status_updates_reflected(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        """Test that submission status updates are reflected in bundles."""
        # GIVEN a submission status that I can update
        submission_status = SubmissionStatus(id=test_submission.id).get(
            synapse_client=self.syn
        )
        original_status = submission_status.status

        # WHEN I update the submission status
        submission_status.status = "VALIDATED"
        submission_status.submission_annotations = {
            "test_score": 95.5,
            "test_feedback": "Excellent work!",
        }
        updated_status = submission_status.store(synapse_client=self.syn)

        # AND I get submission bundles again
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN the bundle should reflect the updated status
        test_bundle = None
        for bundle in bundles:
            if bundle.submission and bundle.submission.id == test_submission.id:
                test_bundle = bundle
                break

        assert test_bundle is not None
        assert test_bundle.submission_status is not None
        assert test_bundle.submission_status.status == "VALIDATED"
        assert test_bundle.submission_status.submission_annotations is not None
        assert "test_score" in test_bundle.submission_status.submission_annotations
        assert test_bundle.submission_status.submission_annotations["test_score"] == [
            95.5
        ]

        # CLEANUP: Reset the status back to original
        submission_status.status = original_status
        submission_status.submission_annotations = {}
        submission_status.store(synapse_client=self.syn)

    async def test_submission_bundle_evaluation_id_propagation(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        """Test that evaluation_id is properly propagated from submission to status."""
        # WHEN I get submission bundles
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN find our test bundle
        test_bundle = None
        for bundle in bundles:
            if bundle.submission and bundle.submission.id == test_submission.id:
                test_bundle = bundle
                break

        assert test_bundle is not None

        # AND both submission and status should have the correct evaluation_id
        assert test_bundle.submission.evaluation_id == test_evaluation.id
        if test_bundle.submission_status:
            assert test_bundle.submission_status.evaluation_id == test_evaluation.id


class TestSubmissionBundleEdgeCases:
    """Tests for edge cases and error handling in SubmissionBundle operations."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        project = Project(name=f"test_project_{uuid.uuid4()}").store(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="Test evaluation for edge case testing",
            content_source=test_project.id,
            submission_instructions_message="Submit your files here",
            submission_receipt_message="Thank you for your submission!",
        ).store(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    async def test_get_evaluation_submission_bundles_empty_evaluation(
        self, test_evaluation: Evaluation
    ):
        """Test getting submission bundles from an evaluation with no submissions."""
        # WHEN I get submission bundles from an evaluation with no submissions
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN it should return an empty list (not None or error)
        assert bundles is not None
        assert isinstance(bundles, list)
        assert len(bundles) == 0

    async def test_get_user_submission_bundles_empty_evaluation(
        self, test_evaluation: Evaluation
    ):
        """Test getting user submission bundles from an evaluation with no submissions."""
        # WHEN I get user submission bundles from an evaluation with no submissions
        bundles = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN it should return an empty list (not None or error)
        assert bundles is not None
        assert isinstance(bundles, list)
        assert len(bundles) == 0

    async def test_get_evaluation_submission_bundles_large_limit(
        self, test_evaluation: Evaluation
    ):
        """Test getting submission bundles with a very large limit."""
        # WHEN I request bundles with a large limit (within API bounds)
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            limit=100,  # Maximum allowed by API
            synapse_client=self.syn,
        )

        # THEN it should work without error
        assert bundles is not None
        assert isinstance(bundles, list)
        # The actual count doesn't matter since the evaluation is empty

    async def test_get_user_submission_bundles_large_offset(
        self, test_evaluation: Evaluation
    ):
        """Test getting user submission bundles with a large offset."""
        # WHEN I request bundles with a large offset (beyond available data)
        bundles = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            offset=1000,  # Large offset beyond any real data
            synapse_client=self.syn,
        )

        # THEN it should return an empty list (not error)
        assert bundles is not None
        assert isinstance(bundles, list)
        assert len(bundles) == 0

    async def test_get_submission_bundles_with_default_parameters(
        self, test_evaluation: Evaluation
    ):
        """Test that default parameters work correctly."""
        # WHEN I call methods without optional parameters
        eval_bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )
        user_bundles = SubmissionBundle.get_user_submission_bundles(
            evaluation_id=test_evaluation.id,
            synapse_client=self.syn,
        )

        # THEN both should work with default values
        assert eval_bundles is not None
        assert user_bundles is not None
        assert isinstance(eval_bundles, list)
        assert isinstance(user_bundles, list)
