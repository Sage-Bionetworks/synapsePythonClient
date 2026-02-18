"""Unit tests for evaluation_services utility functions."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import synapseclient.api.evaluation_services as evaluation_services

EVALUATION_ID = "9614112"
EVALUATION_NAME = "My Test Evaluation"
PROJECT_ID = "syn123456"
SUBMISSION_ID = "12345"
ETAG = "abc-def-ghi"
BATCH_TOKEN = "batch-token-123"


class TestCreateOrUpdateEvaluation:
    """Tests for create_or_update_evaluation function."""

    @patch("synapseclient.Synapse")
    async def test_create_evaluation(self, mock_synapse):
        """Test creating a new evaluation (no id in request body)."""
        # GIVEN a mock client that returns a created evaluation
        mock_client = AsyncMock()
        mock_client.logger = MagicMock()
        mock_synapse.get_client.return_value = mock_client
        request_body = {"name": EVALUATION_NAME, "contentSource": PROJECT_ID}
        expected_response = {
            "id": EVALUATION_ID,
            "name": EVALUATION_NAME,
            "contentSource": PROJECT_ID,
        }
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call create_or_update_evaluation without an id
        result = await evaluation_services.create_or_update_evaluation(
            request_body=request_body, synapse_client=None
        )

        # THEN I expect a POST to /evaluation
        assert result == expected_response
        mock_client.rest_post_async.assert_awaited_once_with(
            "/evaluation", body=json.dumps(request_body)
        )

    @patch("synapseclient.Synapse")
    async def test_update_evaluation(self, mock_synapse):
        """Test updating an existing evaluation (id present in request body)."""
        # GIVEN a mock client that returns an updated evaluation
        mock_client = AsyncMock()
        mock_client.logger = MagicMock()
        mock_synapse.get_client.return_value = mock_client
        request_body = {
            "id": EVALUATION_ID,
            "name": EVALUATION_NAME,
            "contentSource": PROJECT_ID,
        }
        expected_response = dict(request_body)
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call create_or_update_evaluation with an id
        result = await evaluation_services.create_or_update_evaluation(
            request_body=request_body, synapse_client=None
        )

        # THEN I expect a PUT to /evaluation/{id}
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}", body=json.dumps(request_body)
        )


class TestGetEvaluation:
    """Tests for get_evaluation function."""

    @patch("synapseclient.Synapse")
    async def test_get_evaluation_by_id(self, mock_synapse):
        """Test getting an evaluation by ID."""
        # GIVEN a mock client that returns an evaluation
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": EVALUATION_ID, "name": EVALUATION_NAME}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_evaluation with an evaluation_id
        result = await evaluation_services.get_evaluation(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/{id}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_evaluation_by_name(self, mock_synapse):
        """Test getting an evaluation by name."""
        # GIVEN a mock client that returns an evaluation
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": EVALUATION_ID, "name": EVALUATION_NAME}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_evaluation with a name
        result = await evaluation_services.get_evaluation(
            name=EVALUATION_NAME, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/name/{name}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/name/{EVALUATION_NAME}"
        )

    @patch("synapseclient.Synapse")
    async def test_get_evaluation_no_id_or_name_raises(self, mock_synapse):
        """Test that ValueError is raised when neither id nor name is provided."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call get_evaluation without id or name
        # THEN I expect a ValueError
        with pytest.raises(
            ValueError, match="Either 'evaluation_id' or 'name' must be provided"
        ):
            await evaluation_services.get_evaluation(synapse_client=None)


class TestGetEvaluationsByProject:
    """Tests for get_evaluations_by_project function."""

    @patch("synapseclient.Synapse")
    async def test_get_evaluations_by_project_minimal(self, mock_synapse):
        """Test getting evaluations by project with minimal params."""
        # GIVEN a mock client that returns a list of evaluations
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = [{"id": EVALUATION_ID, "name": EVALUATION_NAME}]
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_evaluations_by_project with only project_id
        result = await evaluation_services.get_evaluations_by_project(
            project_id=PROJECT_ID, synapse_client=None
        )

        # THEN I expect a GET to /entity/{project_id}/evaluation with no extra params
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/entity/{PROJECT_ID}/evaluation", params={}
        )

    @patch("synapseclient.Synapse")
    async def test_get_evaluations_by_project_all_params(self, mock_synapse):
        """Test getting evaluations by project with all optional params."""
        # GIVEN a mock client that returns evaluations
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = [{"id": EVALUATION_ID}]
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_evaluations_by_project with all optional params
        result = await evaluation_services.get_evaluations_by_project(
            project_id=PROJECT_ID,
            access_type="READ",
            active_only=True,
            evaluation_ids=["111", "222"],
            offset=5,
            limit=20,
            synapse_client=None,
        )

        # THEN I expect a GET with all params populated
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/entity/{PROJECT_ID}/evaluation",
            params={
                "accessType": "READ",
                "activeOnly": "true",
                "evaluationIds": "111,222",
                "offset": 5,
                "limit": 20,
            },
        )


class TestGetAllEvaluations:
    """Tests for get_all_evaluations function."""

    @patch("synapseclient.Synapse")
    async def test_get_all_evaluations_minimal(self, mock_synapse):
        """Test getting all evaluations with no optional params."""
        # GIVEN a mock client that returns evaluations
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = [{"id": EVALUATION_ID}]
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_all_evaluations with no optional params
        result = await evaluation_services.get_all_evaluations(synapse_client=None)

        # THEN I expect a GET to /evaluation
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with("/evaluation", params={})

    @patch("synapseclient.Synapse")
    async def test_get_all_evaluations_with_params(self, mock_synapse):
        """Test getting all evaluations with all optional params."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = [{"id": EVALUATION_ID}]
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_all_evaluations with optional params
        result = await evaluation_services.get_all_evaluations(
            access_type="SUBMIT",
            active_only=False,
            evaluation_ids=["333"],
            offset=0,
            limit=10,
            synapse_client=None,
        )

        # THEN I expect params to be correctly built
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            "/evaluation",
            params={
                "accessType": "SUBMIT",
                "activeOnly": "false",
                "evaluationIds": "333",
                "offset": 0,
                "limit": 10,
            },
        )


class TestGetAvailableEvaluations:
    """Tests for get_available_evaluations function."""

    @patch("synapseclient.Synapse")
    async def test_get_available_evaluations_minimal(self, mock_synapse):
        """Test getting available evaluations with no optional params."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = [{"id": EVALUATION_ID}]
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_available_evaluations
        result = await evaluation_services.get_available_evaluations(
            synapse_client=None
        )

        # THEN I expect a GET to /evaluation/available
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            "/evaluation/available", params={}
        )

    @patch("synapseclient.Synapse")
    async def test_get_available_evaluations_with_params(self, mock_synapse):
        """Test getting available evaluations with all optional params."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = []
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_available_evaluations with params
        result = await evaluation_services.get_available_evaluations(
            active_only=True,
            evaluation_ids=["444", "555"],
            offset=10,
            limit=5,
            synapse_client=None,
        )

        # THEN I expect all params in the request
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            "/evaluation/available",
            params={
                "activeOnly": "true",
                "evaluationIds": "444,555",
                "offset": 10,
                "limit": 5,
            },
        )


class TestDeleteEvaluation:
    """Tests for delete_evaluation function."""

    @patch("synapseclient.Synapse")
    async def test_delete_evaluation(self, mock_synapse):
        """Test deleting an evaluation."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call delete_evaluation
        await evaluation_services.delete_evaluation(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a DELETE to /evaluation/{id}
        mock_client.rest_delete_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}"
        )


class TestGetEvaluationAcl:
    """Tests for get_evaluation_acl function."""

    @patch("synapseclient.Synapse")
    async def test_get_evaluation_acl(self, mock_synapse):
        """Test getting evaluation ACL."""
        # GIVEN a mock client that returns an ACL
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_acl = {
            "id": EVALUATION_ID,
            "resourceAccess": [{"principalId": 123, "accessType": ["READ"]}],
        }
        mock_client.rest_get_async.return_value = expected_acl

        # WHEN I call get_evaluation_acl
        result = await evaluation_services.get_evaluation_acl(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/{id}/acl
        assert result == expected_acl
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/acl"
        )


class TestUpdateEvaluationAcl:
    """Tests for update_evaluation_acl function."""

    @patch("synapseclient.Synapse")
    async def test_update_evaluation_acl(self, mock_synapse):
        """Test updating evaluation ACL."""
        # GIVEN a mock client that returns the updated ACL
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        acl = {
            "id": EVALUATION_ID,
            "resourceAccess": [{"principalId": 123, "accessType": ["READ", "UPDATE"]}],
        }
        mock_client.rest_put_async.return_value = acl

        # WHEN I call update_evaluation_acl
        result = await evaluation_services.update_evaluation_acl(
            acl=acl, synapse_client=None
        )

        # THEN I expect a PUT to /evaluation/acl
        assert result == acl
        mock_client.rest_put_async.assert_awaited_once_with(
            "/evaluation/acl", body=json.dumps(acl)
        )


class TestGetEvaluationPermissions:
    """Tests for get_evaluation_permissions function."""

    @patch("synapseclient.Synapse")
    async def test_get_evaluation_permissions(self, mock_synapse):
        """Test getting evaluation permissions."""
        # GIVEN a mock client that returns permissions
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_permissions = {"canRead": True, "canUpdate": False}
        mock_client.rest_get_async.return_value = expected_permissions

        # WHEN I call get_evaluation_permissions
        result = await evaluation_services.get_evaluation_permissions(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/{id}/permissions
        assert result == expected_permissions
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/permissions"
        )


class TestCreateSubmission:
    """Tests for create_submission function."""

    @patch("synapseclient.Synapse")
    async def test_create_submission(self, mock_synapse):
        """Test creating a submission."""
        # GIVEN a mock client that returns a created submission
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        request_body = {
            "evaluationId": EVALUATION_ID,
            "entityId": PROJECT_ID,
            "versionNumber": 1,
        }
        expected_response = {"id": SUBMISSION_ID, **request_body}
        mock_client.rest_post_async.return_value = expected_response

        # WHEN I call create_submission
        result = await evaluation_services.create_submission(
            request_body=request_body, etag=ETAG, synapse_client=None
        )

        # THEN I expect a POST to /evaluation/submission with etag as query param
        assert result == expected_response
        mock_client.rest_post_async.assert_awaited_once_with(
            "/evaluation/submission",
            body=json.dumps(request_body),
            params={"etag": ETAG},
        )


class TestGetSubmission:
    """Tests for get_submission function."""

    @patch("synapseclient.Synapse")
    async def test_get_submission(self, mock_synapse):
        """Test getting a submission by ID."""
        # GIVEN a mock client that returns a submission
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": SUBMISSION_ID, "evaluationId": EVALUATION_ID}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_submission
        result = await evaluation_services.get_submission(
            submission_id=SUBMISSION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/submission/{id}
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/submission/{SUBMISSION_ID}"
        )


class TestDeleteSubmission:
    """Tests for delete_submission function."""

    @patch("synapseclient.Synapse")
    async def test_delete_submission(self, mock_synapse):
        """Test deleting a submission."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client

        # WHEN I call delete_submission
        await evaluation_services.delete_submission(
            submission_id=SUBMISSION_ID, synapse_client=None
        )

        # THEN I expect a DELETE to /evaluation/submission/{id}
        mock_client.rest_delete_async.assert_awaited_once_with(
            f"/evaluation/submission/{SUBMISSION_ID}"
        )


class TestCancelSubmission:
    """Tests for cancel_submission function."""

    @patch("synapseclient.Synapse")
    async def test_cancel_submission(self, mock_synapse):
        """Test cancelling a submission."""
        # GIVEN a mock client that returns the cancelled submission
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"id": SUBMISSION_ID, "cancelRequested": True}
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call cancel_submission
        result = await evaluation_services.cancel_submission(
            submission_id=SUBMISSION_ID, synapse_client=None
        )

        # THEN I expect a PUT to /evaluation/submission/{id}/cancellation
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            f"/evaluation/submission/{SUBMISSION_ID}/cancellation"
        )


class TestGetSubmissionStatus:
    """Tests for get_submission_status function."""

    @patch("synapseclient.Synapse")
    async def test_get_submission_status(self, mock_synapse):
        """Test getting submission status."""
        # GIVEN a mock client that returns a submission status
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "id": SUBMISSION_ID,
            "status": "RECEIVED",
            "etag": ETAG,
        }
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_submission_status
        result = await evaluation_services.get_submission_status(
            submission_id=SUBMISSION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/submission/{id}/status
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/submission/{SUBMISSION_ID}/status"
        )


class TestUpdateSubmissionStatus:
    """Tests for update_submission_status function."""

    @patch("synapseclient.Synapse")
    async def test_update_submission_status(self, mock_synapse):
        """Test updating submission status."""
        # GIVEN a mock client that returns the updated status
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        request_body = {
            "id": SUBMISSION_ID,
            "status": "SCORED",
            "etag": ETAG,
            "score": 0.95,
        }
        expected_response = dict(request_body)
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call update_submission_status
        result = await evaluation_services.update_submission_status(
            submission_id=SUBMISSION_ID,
            request_body=request_body,
            synapse_client=None,
        )

        # THEN I expect a PUT to /evaluation/submission/{id}/status
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            f"/evaluation/submission/{SUBMISSION_ID}/status",
            body=json.dumps(request_body),
        )


class TestBatchUpdateSubmissionStatuses:
    """Tests for batch_update_submission_statuses function."""

    @patch("synapseclient.Synapse")
    async def test_batch_update_submission_statuses(self, mock_synapse):
        """Test batch updating submission statuses."""
        # GIVEN a mock client that returns a batch response
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        request_body = {
            "statuses": [
                {"id": "111", "status": "SCORED", "etag": "etag1"},
                {"id": "222", "status": "SCORED", "etag": "etag2"},
            ],
            "isFirstBatch": True,
            "isLastBatch": True,
        }
        expected_response = {"nextUploadToken": BATCH_TOKEN}
        mock_client.rest_put_async.return_value = expected_response

        # WHEN I call batch_update_submission_statuses
        result = await evaluation_services.batch_update_submission_statuses(
            evaluation_id=EVALUATION_ID,
            request_body=request_body,
            synapse_client=None,
        )

        # THEN I expect a PUT to /evaluation/{id}/statusBatch
        assert result == expected_response
        mock_client.rest_put_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/statusBatch",
            body=json.dumps(request_body),
        )


class TestGetAllSubmissionStatuses:
    """Tests for get_all_submission_statuses function."""

    @patch("synapseclient.Synapse")
    async def test_get_all_submission_statuses_defaults(self, mock_synapse):
        """Test getting all submission statuses with default params."""
        # GIVEN a mock client that returns paginated statuses
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {
            "results": [{"id": SUBMISSION_ID, "status": "RECEIVED"}],
            "totalNumberOfResults": 1,
        }
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_all_submission_statuses with defaults
        result = await evaluation_services.get_all_submission_statuses(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/{id}/submission/status/all
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/submission/status/all",
            params={"limit": 10, "offset": 0},
        )

    @patch("synapseclient.Synapse")
    async def test_get_all_submission_statuses_with_status_filter(self, mock_synapse):
        """Test getting all submission statuses with status filter."""
        # GIVEN a mock client
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"results": [], "totalNumberOfResults": 0}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call with a status filter
        result = await evaluation_services.get_all_submission_statuses(
            evaluation_id=EVALUATION_ID,
            status="SCORED",
            limit=50,
            offset=10,
            synapse_client=None,
        )

        # THEN I expect status included in the params
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/submission/status/all",
            params={"limit": 50, "offset": 10, "status": "SCORED"},
        )


class TestGetSubmissionCount:
    """Tests for get_submission_count function."""

    @patch("synapseclient.Synapse")
    async def test_get_submission_count(self, mock_synapse):
        """Test getting submission count for an evaluation."""
        # GIVEN a mock client that returns a count
        mock_client = AsyncMock()
        mock_synapse.get_client.return_value = mock_client
        expected_response = {"count": 42}
        mock_client.rest_get_async.return_value = expected_response

        # WHEN I call get_submission_count
        result = await evaluation_services.get_submission_count(
            evaluation_id=EVALUATION_ID, synapse_client=None
        )

        # THEN I expect a GET to /evaluation/{id}/submission/count
        assert result == expected_response
        mock_client.rest_get_async.assert_awaited_once_with(
            f"/evaluation/{EVALUATION_ID}/submission/count"
        )
