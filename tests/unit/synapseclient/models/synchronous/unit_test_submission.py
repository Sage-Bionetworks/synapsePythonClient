"""Unit tests for the synapseclient.models.Submission class."""
import uuid
from typing import Dict, List, Union
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Submission

SUBMISSION_ID = "9614543"
USER_ID = "123456"
SUBMITTER_ALIAS = "test_user"
ENTITY_ID = "syn789012"
VERSION_NUMBER = 1
EVALUATION_ID = "9999999"
SUBMISSION_NAME = "Test Submission"
CREATED_ON = "2023-01-01T10:00:00.000Z"
TEAM_ID = "team123"
CONTRIBUTORS = ["user1", "user2", "user3"]
SUBMISSION_STATUS = {"status": "RECEIVED", "score": 85.5}
ENTITY_BUNDLE_JSON = '{"entity": {"id": "syn789012", "name": "test_entity"}}'
DOCKER_REPOSITORY_NAME = "test/repository"
DOCKER_DIGEST = "sha256:abc123def456"
ETAG = "etag_value"


class TestSubmission:
    """Tests for the synapseclient.models.Submission class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_submission_response(self) -> Dict[str, Union[str, int, List, Dict]]:
        """Get a complete example submission response from the REST API."""
        return {
            "id": SUBMISSION_ID,
            "userId": USER_ID,
            "submitterAlias": SUBMITTER_ALIAS,
            "entityId": ENTITY_ID,
            "versionNumber": VERSION_NUMBER,
            "evaluationId": EVALUATION_ID,
            "name": SUBMISSION_NAME,
            "createdOn": CREATED_ON,
            "teamId": TEAM_ID,
            "contributors": CONTRIBUTORS,
            "submissionStatus": SUBMISSION_STATUS,
            "entityBundleJSON": ENTITY_BUNDLE_JSON,
            "dockerRepositoryName": DOCKER_REPOSITORY_NAME,
            "dockerDigest": DOCKER_DIGEST,
        }

    def get_minimal_submission_response(self) -> Dict[str, str]:
        """Get a minimal example submission response from the REST API."""
        return {
            "id": SUBMISSION_ID,
            "entityId": ENTITY_ID,
            "evaluationId": EVALUATION_ID,
        }

    def get_example_entity_response(self) -> Dict[str, Union[str, int]]:
        """Get an example entity response for testing entity fetching."""
        return {
            "id": ENTITY_ID,
            "etag": ETAG,
            "versionNumber": VERSION_NUMBER,
            "name": "test_entity",
            "concreteType": "org.sagebionetworks.repo.model.FileEntity",
        }

    def get_example_docker_entity_response(self) -> Dict[str, Union[str, int]]:
        """Get an example Docker repository entity response for testing."""
        return {
            "id": ENTITY_ID,
            "etag": ETAG,
            "name": "test_docker_repo",
            "concreteType": "org.sagebionetworks.repo.model.docker.DockerRepository",
            "repositoryName": "test/repository",
        }

    def get_example_docker_tag_response(self) -> Dict[str, Union[str, int, List]]:
        """Get an example Docker tag response for testing."""
        return {
            "totalNumberOfResults": 2,
            "results": [
                {
                    "tag": "v1.0",
                    "digest": "sha256:older123def456",
                    "createdOn": "2024-01-01T10:00:00.000Z",
                },
                {
                    "tag": "v2.0",
                    "digest": "sha256:latest456abc789",
                    "createdOn": "2024-06-01T15:30:00.000Z",
                },
            ],
        }

    def get_complex_docker_tag_response(self) -> Dict[str, Union[str, int, List]]:
        """Get a more complex Docker tag response with multiple versions to test sorting."""
        return {
            "totalNumberOfResults": 4,
            "results": [
                {
                    "tag": "v1.0",
                    "digest": "sha256:version1",
                    "createdOn": "2024-01-01T10:00:00.000Z",
                },
                {
                    "tag": "v3.0",
                    "digest": "sha256:version3",
                    "createdOn": "2024-08-15T12:00:00.000Z",  # This should be selected (latest)
                },
                {
                    "tag": "v2.0",
                    "digest": "sha256:version2",
                    "createdOn": "2024-06-01T15:30:00.000Z",
                },
                {
                    "tag": "v1.5",
                    "digest": "sha256:version1_5",
                    "createdOn": "2024-03-15T08:45:00.000Z",
                },
            ],
        }

    def test_fill_from_dict_complete_data(self) -> None:
        # GIVEN a complete submission response from the REST API
        # WHEN I call fill_from_dict with the example submission response
        submission = Submission().fill_from_dict(self.get_example_submission_response())

        # THEN the Submission object should be filled with all the data
        assert submission.id == SUBMISSION_ID
        assert submission.user_id == USER_ID
        assert submission.submitter_alias == SUBMITTER_ALIAS
        assert submission.entity_id == ENTITY_ID
        assert submission.version_number == VERSION_NUMBER
        assert submission.evaluation_id == EVALUATION_ID
        assert submission.name == SUBMISSION_NAME
        assert submission.created_on == CREATED_ON
        assert submission.team_id == TEAM_ID
        assert submission.contributors == CONTRIBUTORS
        assert submission.submission_status == SUBMISSION_STATUS
        assert submission.entity_bundle_json == ENTITY_BUNDLE_JSON
        assert submission.docker_repository_name == DOCKER_REPOSITORY_NAME
        assert submission.docker_digest == DOCKER_DIGEST

    def test_fill_from_dict_minimal_data(self) -> None:
        # GIVEN a minimal submission response from the REST API
        # WHEN I call fill_from_dict with the minimal submission response
        submission = Submission().fill_from_dict(self.get_minimal_submission_response())

        # THEN the Submission object should be filled with required data and defaults for optional data
        assert submission.id == SUBMISSION_ID
        assert submission.entity_id == ENTITY_ID
        assert submission.evaluation_id == EVALUATION_ID
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

    def test_to_synapse_request_complete_data(self) -> None:
        # GIVEN a submission with all optional fields set
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
            team_id=TEAM_ID,
            contributors=CONTRIBUTORS,
            docker_repository_name=DOCKER_REPOSITORY_NAME,
            docker_digest=DOCKER_DIGEST,
            version_number=VERSION_NUMBER,
        )

        # WHEN I call to_synapse_request
        request_body = submission.to_synapse_request()

        # THEN the request body should contain all fields in the correct format
        assert request_body["entityId"] == ENTITY_ID
        assert request_body["evaluationId"] == EVALUATION_ID
        assert request_body["versionNumber"] == VERSION_NUMBER
        assert request_body["name"] == SUBMISSION_NAME
        assert request_body["teamId"] == TEAM_ID
        assert request_body["contributors"] == CONTRIBUTORS
        assert request_body["dockerRepositoryName"] == DOCKER_REPOSITORY_NAME
        assert request_body["dockerDigest"] == DOCKER_DIGEST

    def test_to_synapse_request_minimal_data(self) -> None:
        # GIVEN a submission with only required fields
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            version_number=VERSION_NUMBER,
        )

        # WHEN I call to_synapse_request
        request_body = submission.to_synapse_request()

        # THEN the request body should contain only required fields
        assert request_body["entityId"] == ENTITY_ID
        assert request_body["evaluationId"] == EVALUATION_ID
        assert request_body["versionNumber"] == VERSION_NUMBER
        assert "name" not in request_body
        assert "teamId" not in request_body
        assert "contributors" not in request_body
        assert "dockerRepositoryName" not in request_body
        assert "dockerDigest" not in request_body

    def test_to_synapse_request_missing_entity_id(self) -> None:
        # GIVEN a submission without entity_id
        submission = Submission(evaluation_id=EVALUATION_ID)

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'entity_id' attribute"):
            submission.to_synapse_request()

    def test_to_synapse_request_missing_evaluation_id(self) -> None:
        # GIVEN a submission without evaluation_id
        submission = Submission(entity_id=ENTITY_ID)

        # WHEN I call to_synapse_request
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'evaluation_id' attribute"):
            submission.to_synapse_request()

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_success(self) -> None:
        # GIVEN a submission with an entity_id
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity with a mocked successful response
        with patch(
            "synapseclient.api.entity_services.get_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_entity_response(),
        ) as mock_get_entity:
            entity_info = await submission._fetch_latest_entity(synapse_client=self.syn)

            # THEN it should return the entity information
            assert entity_info["id"] == ENTITY_ID
            assert entity_info["etag"] == ETAG
            assert entity_info["versionNumber"] == VERSION_NUMBER
            mock_get_entity.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_docker_repository(self) -> None:
        # GIVEN a submission with a Docker repository entity_id
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity with mocked Docker repository responses
        with patch(
            "synapseclient.api.entity_services.get_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_docker_entity_response(),
        ) as mock_get_entity, patch(
            "synapseclient.api.docker_commit_services.get_docker_tag",
            new_callable=AsyncMock,
            return_value=self.get_example_docker_tag_response(),
        ) as mock_get_docker_tag:
            entity_info = await submission._fetch_latest_entity(synapse_client=self.syn)

            # THEN it should return the entity information with latest docker tag info
            assert entity_info["id"] == ENTITY_ID
            assert entity_info["etag"] == ETAG
            assert entity_info["repositoryName"] == "test/repository"
            # Should have the latest tag information (v2.0 based on createdOn date)
            assert entity_info["tag"] == "v2.0"
            assert entity_info["digest"] == "sha256:latest456abc789"
            assert entity_info["createdOn"] == "2024-06-01T15:30:00.000Z"

            # Verify both API functions were called
            mock_get_entity.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )
            mock_get_docker_tag.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_docker_empty_results(self) -> None:
        # GIVEN a submission with a Docker repository entity_id
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity with empty docker tag results
        with patch(
            "synapseclient.api.entity_services.get_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_docker_entity_response(),
        ) as mock_get_entity, patch(
            "synapseclient.api.docker_commit_services.get_docker_tag",
            new_callable=AsyncMock,
            return_value={"totalNumberOfResults": 0, "results": []},
        ) as mock_get_docker_tag:
            entity_info = await submission._fetch_latest_entity(synapse_client=self.syn)

            # THEN it should return the entity information without docker tag info
            assert entity_info["id"] == ENTITY_ID
            assert entity_info["etag"] == ETAG
            assert entity_info["repositoryName"] == "test/repository"
            # Should not have docker tag fields since results were empty
            assert "tag" not in entity_info
            assert "digest" not in entity_info
            assert "createdOn" not in entity_info

            # Verify both API functions were called
            mock_get_entity.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )
            mock_get_docker_tag.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )
            assert "tag" not in entity_info
            assert "digest" not in entity_info
            assert "createdOn" not in entity_info

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_docker_complex_tag_selection(self) -> None:
        # GIVEN a submission with a Docker repository with multiple tags
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity with multiple docker tags with different dates
        with patch(
            "synapseclient.api.entity_services.get_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_docker_entity_response(),
        ) as mock_get_entity, patch(
            "synapseclient.api.docker_commit_services.get_docker_tag",
            new_callable=AsyncMock,
            return_value=self.get_complex_docker_tag_response(),
        ) as mock_get_docker_tag:
            entity_info = await submission._fetch_latest_entity(synapse_client=self.syn)

            # THEN it should select the tag with the latest createdOn timestamp (v3.0)
            assert entity_info["tag"] == "v3.0"
            assert entity_info["digest"] == "sha256:version3"
            assert entity_info["createdOn"] == "2024-08-15T12:00:00.000Z"

            # Verify both API functions were called
            mock_get_entity.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )
            mock_get_docker_tag.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_without_entity_id(self) -> None:
        # GIVEN a submission without entity_id
        submission = Submission(evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="entity_id must be set to fetch entity information"
        ):
            await submission._fetch_latest_entity(synapse_client=self.syn)

    @pytest.mark.asyncio
    async def test_fetch_latest_entity_api_error(self) -> None:
        # GIVEN a submission with an entity_id
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call _fetch_latest_entity and the API returns an error
        with patch(
            "synapseclient.api.entity_services.get_entity",
            new_callable=AsyncMock,
            side_effect=SynapseHTTPError("Entity not found"),
        ) as mock_get_entity:
            # THEN it should raise a LookupError with context about the original error
            with pytest.raises(
                LookupError, match=f"Unable to fetch entity information for {ENTITY_ID}"
            ):
                await submission._fetch_latest_entity(synapse_client=self.syn)

            mock_get_entity.assert_called_once_with(
                entity_id=ENTITY_ID, synapse_client=self.syn
            )

    @pytest.mark.asyncio
    async def test_store_async_success(self) -> None:
        # GIVEN a submission with valid data
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
        )

        # WHEN I call store_async with mocked dependencies
        with patch.object(
            submission,
            "_fetch_latest_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_entity_response(),
        ) as mock_fetch_entity, patch(
            "synapseclient.api.evaluation_services.create_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_response(),
        ) as mock_create_submission:
            stored_submission = await submission.store_async(synapse_client=self.syn)

            # THEN it should fetch entity information, create the submission, and fill the object
            mock_fetch_entity.assert_called_once_with(synapse_client=self.syn)
            mock_create_submission.assert_called_once()

            # Check the call arguments to create_submission
            call_args = mock_create_submission.call_args
            request_body = call_args[0][0]
            etag = call_args[0][1]

            assert request_body["entityId"] == ENTITY_ID
            assert request_body["evaluationId"] == EVALUATION_ID
            assert request_body["name"] == SUBMISSION_NAME
            assert request_body["versionNumber"] == VERSION_NUMBER
            assert etag == ETAG

            # Verify the submission is filled with response data
            assert stored_submission.id == SUBMISSION_ID
            assert stored_submission.entity_id == ENTITY_ID
            assert stored_submission.evaluation_id == EVALUATION_ID

    @pytest.mark.asyncio
    async def test_store_async_docker_repository_success(self) -> None:
        # GIVEN a submission with valid data for Docker repository
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
        )

        # WHEN I call store_async with mocked Docker repository entity
        docker_entity_with_tag = self.get_example_docker_entity_response()
        docker_entity_with_tag.update(
            {
                "tag": "v2.0",
                "digest": "sha256:latest456abc789",
                "createdOn": "2024-06-01T15:30:00.000Z",
            }
        )

        with patch.object(
            submission,
            "_fetch_latest_entity",
            new_callable=AsyncMock,
            return_value=docker_entity_with_tag,
        ) as mock_fetch_entity, patch(
            "synapseclient.api.evaluation_services.create_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_response(),
        ) as mock_create_submission:
            stored_submission = await submission.store_async(synapse_client=self.syn)

            # THEN it should handle Docker repository specific logic
            mock_fetch_entity.assert_called_once_with(synapse_client=self.syn)
            mock_create_submission.assert_called_once()

            # Verify Docker repository attributes are set correctly
            assert submission.version_number == 1  # Docker repos get version 1
            assert submission.docker_repository_name == "test/repository"
            assert stored_submission.docker_digest == DOCKER_DIGEST

    @pytest.mark.asyncio
    async def test_store_async_with_team_data_success(self) -> None:
        # GIVEN a submission with team information
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
            team_id=TEAM_ID,
            contributors=CONTRIBUTORS,
        )

        # WHEN I call store_async with mocked dependencies
        with patch.object(
            submission,
            "_fetch_latest_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_entity_response(),
        ) as mock_fetch_entity, patch(
            "synapseclient.api.evaluation_services.create_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_response(),
        ) as mock_create_submission:
            stored_submission = await submission.store_async(synapse_client=self.syn)

            # THEN it should preserve team information in the stored submission
            mock_fetch_entity.assert_called_once_with(synapse_client=self.syn)
            mock_create_submission.assert_called_once()

            # Verify team data is preserved
            assert stored_submission.team_id == TEAM_ID
            assert stored_submission.contributors == CONTRIBUTORS
            assert stored_submission.id == SUBMISSION_ID
            assert stored_submission.entity_id == ENTITY_ID
            assert stored_submission.evaluation_id == EVALUATION_ID

    @pytest.mark.asyncio
    async def test_store_async_missing_entity_id(self) -> None:
        # GIVEN a submission without entity_id
        submission = Submission(evaluation_id=EVALUATION_ID, name=SUBMISSION_NAME)

        # WHEN I call store_async
        # THEN it should raise a ValueError during to_synapse_request
        with pytest.raises(
            ValueError, match="entity_id is required to create a submission"
        ):
            await submission.store_async(synapse_client=self.syn)

    @pytest.mark.asyncio
    async def test_store_async_entity_fetch_failure(self) -> None:
        # GIVEN a submission with valid data but entity fetch fails
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
        )

        # WHEN I call store_async and entity fetching fails
        with patch.object(
            submission,
            "_fetch_latest_entity",
            new_callable=AsyncMock,
            side_effect=ValueError("Unable to fetch entity information"),
        ) as mock_fetch_entity:
            # THEN it should propagate the ValueError
            with pytest.raises(ValueError, match="Unable to fetch entity information"):
                await submission.store_async(synapse_client=self.syn)

    @pytest.mark.asyncio
    async def test_get_async_success(self) -> None:
        # GIVEN a submission with an ID
        submission = Submission(id=SUBMISSION_ID)

        # WHEN I call get_async with a mocked successful response
        with patch(
            "synapseclient.api.evaluation_services.get_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_response(),
        ) as mock_get_submission:
            retrieved_submission = await submission.get_async(synapse_client=self.syn)

            # THEN it should call the API and fill the object
            mock_get_submission.assert_called_once_with(
                submission_id=SUBMISSION_ID,
                synapse_client=self.syn,
            )
            assert retrieved_submission.id == SUBMISSION_ID
            assert retrieved_submission.entity_id == ENTITY_ID
            assert retrieved_submission.evaluation_id == EVALUATION_ID

    @pytest.mark.asyncio
    async def test_get_async_without_id(self) -> None:
        # GIVEN a submission without an ID
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call get_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to get"):
            await submission.get_async(synapse_client=self.syn)

    @pytest.mark.asyncio
    async def test_delete_async_success(self) -> None:
        # GIVEN a submission with an ID
        submission = Submission(id=SUBMISSION_ID)

        # WHEN I call delete_async with mocked dependencies
        with patch(
            "synapseclient.api.evaluation_services.delete_submission",
            new_callable=AsyncMock,
        ) as mock_delete_submission, patch(
            "synapseclient.Synapse.get_client",
            return_value=self.syn,
        ):
            # Mock the logger
            self.syn.logger = MagicMock()

            await submission.delete_async(synapse_client=self.syn)

            # THEN it should call the API and log the deletion
            mock_delete_submission.assert_called_once_with(
                submission_id=SUBMISSION_ID,
                synapse_client=self.syn,
            )
            self.syn.logger.info.assert_called_once_with(
                f"Submission {SUBMISSION_ID} has successfully been deleted."
            )

    @pytest.mark.asyncio
    async def test_delete_async_without_id(self) -> None:
        # GIVEN a submission without an ID
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call delete_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to delete"):
            await submission.delete_async(synapse_client=self.syn)

    @pytest.mark.asyncio
    async def test_cancel_async_success(self) -> None:
        # GIVEN a submission with an ID
        submission = Submission(id=SUBMISSION_ID)

        # WHEN I call cancel_async with mocked dependencies
        with patch(
            "synapseclient.api.evaluation_services.cancel_submission",
            new_callable=AsyncMock,
            return_value=self.get_example_submission_response(),
        ) as mock_cancel_submission, patch(
            "synapseclient.Synapse.get_client",
            return_value=self.syn,
        ):
            # Mock the logger
            self.syn.logger = MagicMock()

            await submission.cancel_async(synapse_client=self.syn)

            # THEN it should call the API, log the cancellation, and update the object
            mock_cancel_submission.assert_called_once_with(
                submission_id=SUBMISSION_ID,
                synapse_client=self.syn,
            )
            self.syn.logger.info.assert_called_once_with(
                f"A request to cancel Submission {SUBMISSION_ID} has been submitted."
            )

    @pytest.mark.asyncio
    async def test_cancel_async_without_id(self) -> None:
        # GIVEN a submission without an ID
        submission = Submission(entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID)

        # WHEN I call cancel_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="must have an ID to cancel"):
            await submission.cancel_async(synapse_client=self.syn)

    def test_get_evaluation_submissions(self) -> None:
        # GIVEN evaluation parameters
        evaluation_id = EVALUATION_ID
        status = "SCORED"

        # WHEN I call get_evaluation_submissions
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_submissions"
        ) as mock_get_submissions:
            # Create an async generator function that yields submission data
            async def mock_async_gen(*args, **kwargs):
                submission_data = self.get_example_submission_response()
                yield submission_data

            # Make the mock return our async generator when called
            mock_get_submissions.side_effect = mock_async_gen

            submissions = []
            for submission in Submission.get_evaluation_submissions(
                evaluation_id=evaluation_id,
                status=status,
                synapse_client=self.syn,
            ):
                submissions.append(submission)

            # THEN it should call the API with correct parameters and yield Submission objects
            mock_get_submissions.assert_called_once_with(
                evaluation_id=evaluation_id,
                status=status,
                synapse_client=self.syn,
            )
            assert len(submissions) == 1
            assert isinstance(submissions[0], Submission)
            assert submissions[0].id == SUBMISSION_ID

    def test_get_user_submissions(self) -> None:
        # GIVEN user submission parameters
        evaluation_id = EVALUATION_ID
        user_id = USER_ID

        # WHEN I call get_user_submissions
        with patch(
            "synapseclient.api.evaluation_services.get_user_submissions"
        ) as mock_get_user_submissions:
            # Create an async generator function that yields submission data
            async def mock_async_gen(*args, **kwargs):
                submission_data = self.get_example_submission_response()
                yield submission_data

            # Make the mock return our async generator when called
            mock_get_user_submissions.side_effect = mock_async_gen

            submissions = []
            for submission in Submission.get_user_submissions(
                evaluation_id=evaluation_id,
                user_id=user_id,
                synapse_client=self.syn,
            ):
                submissions.append(submission)

            # THEN it should call the API with correct parameters and yield Submission objects
            mock_get_user_submissions.assert_called_once_with(
                evaluation_id=evaluation_id,
                user_id=user_id,
                synapse_client=self.syn,
            )
            assert len(submissions) == 1
            assert isinstance(submissions[0], Submission)
            assert submissions[0].id == SUBMISSION_ID

    def test_get_submission_count(self) -> None:
        # GIVEN submission count parameters
        evaluation_id = EVALUATION_ID
        status = "VALID"

        expected_response = 42

        # WHEN I call get_submission_count
        with patch(
            "synapseclient.api.evaluation_services.get_submission_count",
            new_callable=AsyncMock,
            return_value=expected_response,
        ) as mock_get_count:
            response = Submission.get_submission_count(
                evaluation_id=evaluation_id,
                status=status,
                synapse_client=self.syn,
            )

            # THEN it should call the API with correct parameters
            mock_get_count.assert_called_once_with(
                evaluation_id=evaluation_id,
                status=status,
                synapse_client=self.syn,
            )
            assert response == expected_response

    def test_default_values(self) -> None:
        # GIVEN a new Submission object with no parameters
        submission = Submission()

        # THEN all attributes should have their default values
        assert submission.id is None
        assert submission.user_id is None
        assert submission.submitter_alias is None
        assert submission.entity_id is None
        assert submission.version_number is None
        assert submission.evaluation_id is None
        assert submission.name is None
        assert submission.created_on is None
        assert submission.team_id is None
        assert submission.contributors == []
        assert submission.submission_status is None
        assert submission.entity_bundle_json is None
        assert submission.docker_repository_name is None
        assert submission.docker_digest is None
        assert submission.etag is None

    def test_constructor_with_values(self) -> None:
        # GIVEN specific values for submission attributes
        # WHEN I create a Submission object with those values
        submission = Submission(
            id=SUBMISSION_ID,
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=SUBMISSION_NAME,
            team_id=TEAM_ID,
            contributors=CONTRIBUTORS,
            docker_repository_name=DOCKER_REPOSITORY_NAME,
            docker_digest=DOCKER_DIGEST,
        )

        # THEN the object should be initialized with those values
        assert submission.id == SUBMISSION_ID
        assert submission.entity_id == ENTITY_ID
        assert submission.evaluation_id == EVALUATION_ID
        assert submission.name == SUBMISSION_NAME
        assert submission.team_id == TEAM_ID
        assert submission.contributors == CONTRIBUTORS
        assert submission.docker_repository_name == DOCKER_REPOSITORY_NAME
        assert submission.docker_digest == DOCKER_DIGEST

    def test_to_synapse_request_with_none_values(self) -> None:
        # GIVEN a submission with some None values for optional fields
        submission = Submission(
            entity_id=ENTITY_ID,
            evaluation_id=EVALUATION_ID,
            name=None,  # Explicitly None
            team_id=None,  # Explicitly None
            contributors=[],  # Empty list (falsy)
        )

        # WHEN I call to_synapse_request
        request_body = submission.to_synapse_request()

        # THEN None and empty values should not be included
        assert request_body["entityId"] == ENTITY_ID
        assert request_body["evaluationId"] == EVALUATION_ID
        assert "name" not in request_body
        assert "teamId" not in request_body
        assert "contributors" not in request_body
