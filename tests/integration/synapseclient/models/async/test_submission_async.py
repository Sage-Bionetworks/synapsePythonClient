"""Async integration tests for the synapseclient.models.Submission class."""

import uuid
from typing import Callable

import pytest
import pytest_asyncio

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Evaluation, File, Project, Submission


class TestSubmissionCreationAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest_asyncio.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for submission tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest_asyncio.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest_asyncio.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission tests."""
        import os
        import tempfile

        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission testing.")
            temp_file_path = temp_file.name

        try:
            file = await File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=test_project.id,
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(file.id)
            return file
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    async def test_store_submission_successfully_async(
        self, test_evaluation: Evaluation, test_file: File
    ):
        # WHEN I create a submission with valid data using async method
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(created_submission.id)

        # THEN the submission should be created successfully
        assert created_submission.id is not None
        assert created_submission.entity_id == test_file.id
        assert created_submission.evaluation_id == test_evaluation.id
        assert created_submission.name == submission.name
        assert created_submission.user_id is not None
        assert created_submission.created_on is not None
        assert created_submission.version_number is not None

    async def test_store_submission_without_entity_id_async(
        self, test_evaluation: Evaluation
    ):
        # WHEN I try to create a submission without entity_id using async method
        submission = Submission(
            evaluation_id=test_evaluation.id,
            name="Test Submission",
        )

        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="entity_id is required to create a submission"
        ):
            await submission.store_async(synapse_client=self.syn)

    async def test_store_submission_without_evaluation_id_async(self, test_file: File):
        # WHEN I try to create a submission without evaluation_id using async method
        submission = Submission(
            entity_id=test_file.id,
            name="Test Submission",
        )

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'evaluation_id' attribute"):
            await submission.store_async(synapse_client=self.syn)

    # async def test_store_submission_with_docker_repository_async(
    #     self, test_evaluation: Evaluation
    # ):
    #     # GIVEN we would need a Docker repository entity (mocked for this test)
    #     # This test demonstrates the expected behavior for Docker repository submissions

    #     # WHEN I create a submission for a Docker repository entity using async method
    #     # TODO: This would require a real Docker repository entity in a full integration test
    #     submission = Submission(
    #         entity_id="syn123456789",  # Would be a Docker repository ID
    #         evaluation_id=test_evaluation.id,
    #         name=f"Docker Submission {uuid.uuid4()}",
    #     )

    #     # THEN the submission should handle Docker-specific attributes
    #     # (This test would need to be expanded with actual Docker repository setup)
    #     assert submission.entity_id == "syn123456789"
    #     assert submission.evaluation_id == test_evaluation.id


class TestSubmissionRetrievalAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest_asyncio.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for submission tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest_asyncio.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest_asyncio.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission tests."""
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission testing.")
            temp_file_path = temp_file.name

        try:
            file = await File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=test_project.id,
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(file.id)
            return file
        finally:
            os.unlink(temp_file_path)

    @pytest_asyncio.fixture(scope="function")
    async def test_submission(
        self,
        test_evaluation: Evaluation,
        test_file: File,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Submission:
        """Create a test submission for retrieval tests."""
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=syn)
        schedule_for_cleanup(created_submission.id)
        return created_submission

    async def test_get_submission_by_id_async(
        self, test_submission: Submission, test_evaluation: Evaluation, test_file: File
    ):
        # WHEN I get a submission by ID using async method
        retrieved_submission = await Submission(id=test_submission.id).get_async(
            synapse_client=self.syn
        )

        # THEN the submission should be retrieved correctly
        assert retrieved_submission.id == test_submission.id
        assert retrieved_submission.entity_id == test_file.id
        assert retrieved_submission.evaluation_id == test_evaluation.id
        assert retrieved_submission.name == test_submission.name
        assert retrieved_submission.user_id is not None
        assert retrieved_submission.created_on is not None

    async def test_get_evaluation_submissions_async(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        # WHEN I get all submissions for an evaluation using async method
        response = await Submission.get_evaluation_submissions_async(
            evaluation_id=test_evaluation.id, synapse_client=self.syn
        )

        # THEN I should get a response with submissions
        assert "results" in response
        assert len(response["results"]) > 0

        # AND the submission should be in the results
        submission_ids = [sub.get("id") for sub in response["results"]]
        assert test_submission.id in submission_ids

    async def test_get_evaluation_submissions_with_status_filter_async(
        self, test_evaluation: Evaluation, test_submission: Submission
    ):
        # WHEN I get submissions filtered by status using async method
        response = await Submission.get_evaluation_submissions_async(
            evaluation_id=test_evaluation.id,
            status="RECEIVED",
            synapse_client=self.syn,
        )

        # THEN I should get submissions with the specified status
        assert "results" in response
        for submission in response["results"]:
            if submission.get("id") == test_submission.id:
                # The submission should be in RECEIVED status initially
                break
        else:
            pytest.fail("Test submission not found in filtered results")

    async def test_get_evaluation_submissions_with_pagination_async(
        self, test_evaluation: Evaluation
    ):
        # WHEN I get submissions with pagination parameters using async method
        response = await Submission.get_evaluation_submissions_async(
            evaluation_id=test_evaluation.id,
            limit=5,
            offset=0,
            synapse_client=self.syn,
        )

        # THEN the response should respect pagination
        assert "results" in response
        assert len(response["results"]) <= 5

    async def test_get_user_submissions_async(self, test_evaluation: Evaluation):
        # WHEN I get submissions for the current user using async method
        response = await Submission.get_user_submissions_async(
            evaluation_id=test_evaluation.id, synapse_client=self.syn
        )

        # THEN I should get a response with user submissions
        assert "results" in response
        # Note: Could be empty if user hasn't made submissions to this evaluation

    async def test_get_submission_count_async(self, test_evaluation: Evaluation):
        # WHEN I get the submission count for an evaluation using async method
        response = await Submission.get_submission_count_async(
            evaluation_id=test_evaluation.id, synapse_client=self.syn
        )

        # THEN I should get a count response
        assert isinstance(response, int)


class TestSubmissionDeletionAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest_asyncio.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for submission tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest_asyncio.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest_asyncio.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission tests."""
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission testing.")
            temp_file_path = temp_file.name

        try:
            file = await File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=test_project.id,
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(file.id)
            return file
        finally:
            os.unlink(temp_file_path)

    async def test_delete_submission_successfully_async(
        self, test_evaluation: Evaluation, test_file: File
    ):
        # GIVEN a submission created with async method
        submission = Submission(
            entity_id=test_file.id,
            evaluation_id=test_evaluation.id,
            name=f"Test Submission for Deletion {uuid.uuid4()}",
        )
        created_submission = await submission.store_async(synapse_client=self.syn)

        # WHEN I delete the submission using async method
        await created_submission.delete_async(synapse_client=self.syn)

        # THEN attempting to retrieve it should raise an error
        with pytest.raises(SynapseHTTPError):
            await Submission(id=created_submission.id).get_async(
                synapse_client=self.syn
            )

    async def test_delete_submission_without_id_async(self):
        # WHEN I try to delete a submission without an ID using async method
        submission = Submission(entity_id="syn123", evaluation_id="456")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to delete"):
            await submission.delete_async(synapse_client=self.syn)


class TestSubmissionCancelAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest_asyncio.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for submission tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest_asyncio.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for submission tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for submission tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest_asyncio.fixture(scope="function")
    async def test_file(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> File:
        """Create a test file for submission tests."""
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as temp_file:
            temp_file.write("This is test content for submission testing.")
            temp_file_path = temp_file.name

        try:
            file = await File(
                path=temp_file_path,
                name=f"test_file_{uuid.uuid4()}.txt",
                parent_id=test_project.id,
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(file.id)
            return file
        finally:
            os.unlink(temp_file_path)

    # async def test_cancel_submission_successfully_async(
    #     self, test_evaluation: Evaluation, test_file: File
    # ):
    #     # GIVEN a submission created with async method
    #     submission = Submission(
    #         entity_id=test_file.id,
    #         evaluation_id=test_evaluation.id,
    #         name=f"Test Submission for Cancellation {uuid.uuid4()}",
    #     )
    #     created_submission = await submission.store_async(synapse_client=self.syn)
    #     self.schedule_for_cleanup(created_submission.id)

    #     # WHEN I cancel the submission using async method
    #     cancelled_submission = await created_submission.cancel_async(synapse_client=self.syn)

    #     # THEN the submission should be cancelled
    #     assert cancelled_submission.id == created_submission.id

    async def test_cancel_submission_without_id_async(self):
        # WHEN I try to cancel a submission without an ID using async method
        submission = Submission(entity_id="syn123", evaluation_id="456")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to cancel"):
            await submission.cancel_async(synapse_client=self.syn)


class TestSubmissionValidationAsync:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_get_submission_without_id_async(self):
        # WHEN I try to get a submission without an ID using async method
        submission = Submission(entity_id="syn123", evaluation_id="456")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to get"):
            await submission.get_async(synapse_client=self.syn)

    async def test_to_synapse_request_missing_entity_id_async(self):
        # WHEN I try to create a request without entity_id
        submission = Submission(evaluation_id="456", name="Test")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'entity_id' attribute"):
            submission.to_synapse_request()

    async def test_to_synapse_request_missing_evaluation_id_async(self):
        # WHEN I try to create a request without evaluation_id
        submission = Submission(entity_id="syn123", name="Test")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'evaluation_id' attribute"):
            submission.to_synapse_request()

    async def test_to_synapse_request_valid_data_async(self):
        # WHEN I create a request with valid required data
        submission = Submission(
            entity_id="syn123456",
            evaluation_id="789",
            name="Test Submission",
            team_id="team123",
            contributors=["user1", "user2"],
            docker_repository_name="test/repo",
            docker_digest="sha256:abc123",
        )

        request_body = submission.to_synapse_request()

        # THEN it should create a valid request body
        assert request_body["entityId"] == "syn123456"
        assert request_body["evaluationId"] == "789"
        assert request_body["name"] == "Test Submission"
        assert request_body["teamId"] == "team123"
        assert request_body["contributors"] == ["user1", "user2"]
        assert request_body["dockerRepositoryName"] == "test/repo"
        assert request_body["dockerDigest"] == "sha256:abc123"

    async def test_to_synapse_request_minimal_data_async(self):
        # WHEN I create a request with only required data
        submission = Submission(entity_id="syn123456", evaluation_id="789")

        request_body = submission.to_synapse_request()

        # THEN it should create a minimal request body
        assert request_body["entityId"] == "syn123456"
        assert request_body["evaluationId"] == "789"
        assert "name" not in request_body
        assert "teamId" not in request_body
        assert "contributors" not in request_body
        assert "dockerRepositoryName" not in request_body
        assert "dockerDigest" not in request_body

    async def test_fetch_latest_entity_success_async(self):
        # GIVEN a submission with a valid entity_id
        submission = Submission(entity_id="syn123456", evaluation_id="789")

        # Note: This test would need a real entity ID to work in practice
        # For now, we test the validation logic
        with pytest.raises(ValueError, match="Unable to fetch entity information"):
            await submission._fetch_latest_entity(synapse_client=self.syn)

    async def test_fetch_latest_entity_without_entity_id_async(self):
        # GIVEN a submission without entity_id
        submission = Submission(evaluation_id="789")

        # WHEN I try to fetch entity information
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="entity_id must be set"):
            await submission._fetch_latest_entity(synapse_client=self.syn)


class TestSubmissionDataMappingAsync:
    async def test_fill_from_dict_complete_data_async(self):
        # GIVEN a complete submission response from the REST API
        api_response = {
            "id": "123456",
            "userId": "user123",
            "submitterAlias": "testuser",
            "entityId": "syn789",
            "versionNumber": 1,
            "evaluationId": "eval456",
            "name": "Test Submission",
            "createdOn": "2023-01-01T10:00:00.000Z",
            "teamId": "team123",
            "contributors": ["user1", "user2"],
            "submissionStatus": {"status": "RECEIVED"},
            "entityBundleJSON": '{"entity": {"id": "syn789"}}',
            "dockerRepositoryName": "test/repo",
            "dockerDigest": "sha256:abc123",
        }

        # WHEN I fill a submission object from the dict
        submission = Submission()
        submission.fill_from_dict(api_response)

        # THEN all fields should be mapped correctly
        assert submission.id == "123456"
        assert submission.user_id == "user123"
        assert submission.submitter_alias == "testuser"
        assert submission.entity_id == "syn789"
        assert submission.version_number == 1
        assert submission.evaluation_id == "eval456"
        assert submission.name == "Test Submission"
        assert submission.created_on == "2023-01-01T10:00:00.000Z"
        assert submission.team_id == "team123"
        assert submission.contributors == ["user1", "user2"]
        assert submission.submission_status == {"status": "RECEIVED"}
        assert submission.entity_bundle_json == '{"entity": {"id": "syn789"}}'
        assert submission.docker_repository_name == "test/repo"
        assert submission.docker_digest == "sha256:abc123"

    async def test_fill_from_dict_minimal_data_async(self):
        # GIVEN a minimal submission response from the REST API
        api_response = {
            "id": "123456",
            "entityId": "syn789",
            "evaluationId": "eval456",
        }

        # WHEN I fill a submission object from the dict
        submission = Submission()
        submission.fill_from_dict(api_response)

        # THEN required fields should be set and optional fields should have defaults
        assert submission.id == "123456"
        assert submission.entity_id == "syn789"
        assert submission.evaluation_id == "eval456"
        assert submission.user_id is None
        assert submission.submitter_alias is None
        assert submission.version_number is None
        assert submission.name is None
        assert submission.created_on is None
        assert submission.team_id is None
        assert submission.contributors == []
        assert submission.submission_status is None
        assert submission.entity_bundle_json is None
        assert submission.docker_repository_name is None
        assert submission.docker_digest is None
