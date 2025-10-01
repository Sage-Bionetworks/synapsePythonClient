"""Unit tests for the synapseclient.models.Evaluation class."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Evaluation


class TestEvaluation:
    """Unit tests for basic Evaluation model functionality."""

    def test_has_changed_property(self):
        """Test the has_changed property."""
        # GIVEN a new evaluation
        evaluation = Evaluation(
            name="Test Evaluation",
            description="Description",
            content_source="syn123456",
            submission_instructions_message="Instructions",
            submission_receipt_message="Receipt",
        )

        # THEN it should report as changed
        assert evaluation.has_changed is True

        # WHEN we set the last persistent instance
        evaluation._set_last_persistent_instance()

        # THEN it should no longer report as changed
        assert evaluation.has_changed is False

        # WHEN we modify a property
        evaluation.description = "New description"

        # THEN it should report as changed again
        assert evaluation.has_changed is True

    def test_to_synapse_request_create(self):
        """Test generating a request body for creating an evaluation."""
        # GIVEN a complete evaluation
        evaluation = Evaluation(
            name="Test Evaluation",
            description="This is a test evaluation",
            content_source="syn123456",
            submission_instructions_message="Submit your results",
            submission_receipt_message="Thank you for submitting",
        )

        # WHEN we generate a request body for create
        request_body = evaluation.to_synapse_request("create")

        # THEN it should have the correct structure
        assert request_body == {
            "name": "Test Evaluation",
            "description": "This is a test evaluation",
            "contentSource": "syn123456",
            "submissionInstructionsMessage": "Submit your results",
            "submissionReceiptMessage": "Thank you for submitting",
        }

    def test_to_synapse_request_update(self):
        """Test generating a request body for updating an evaluation."""
        # GIVEN a complete evaluation with id and etag
        evaluation = Evaluation(
            id="9614112",
            etag="abc-123-xyz",
            name="Test Evaluation",
            description="This is a test evaluation",
            content_source="syn123456",
            submission_instructions_message="Submit your results",
            submission_receipt_message="Thank you for submitting",
        )

        # WHEN we generate a request body for update
        request_body = evaluation.to_synapse_request("update")

        # THEN it should have the correct structure including id and etag
        assert request_body == {
            "id": "9614112",
            "etag": "abc-123-xyz",
            "name": "Test Evaluation",
            "description": "This is a test evaluation",
            "contentSource": "syn123456",
            "submissionInstructionsMessage": "Submit your results",
            "submissionReceiptMessage": "Thank you for submitting",
        }

    def test_to_synapse_request_missing_required_fields(self):
        """Test generating a request body with missing required fields."""
        # GIVEN evaluations with various missing fields
        cases = [
            (
                Evaluation(
                    description="Test",
                    content_source="syn123",
                    submission_instructions_message="Inst",
                    submission_receipt_message="Rec",
                ),
                "name",
            ),
            (
                Evaluation(
                    name="Test",
                    content_source="syn123",
                    submission_instructions_message="Inst",
                    submission_receipt_message="Rec",
                ),
                "description",
            ),
            (
                Evaluation(
                    name="Test",
                    description="Test",
                    submission_instructions_message="Inst",
                    submission_receipt_message="Rec",
                ),
                "content_source",
            ),
            (
                Evaluation(
                    name="Test",
                    description="Test",
                    content_source="syn123",
                    submission_receipt_message="Rec",
                ),
                "submission_instructions_message",
            ),
            (
                Evaluation(
                    name="Test",
                    description="Test",
                    content_source="syn123",
                    submission_instructions_message="Inst",
                ),
                "submission_receipt_message",
            ),
        ]

        # Test each case
        for evaluation, missing_field in cases:
            with pytest.raises(
                ValueError, match=f"missing the '{missing_field}' attribute"
            ):
                evaluation.to_synapse_request("create")

    def test_set_last_persistent_instance(self):
        """Test setting the last persistent instance of an evaluation."""
        # GIVEN a new evaluation
        evaluation = Evaluation(
            name="Test Evaluation",
            description="Description",
            content_source="syn123456",
            submission_instructions_message="Instructions",
            submission_receipt_message="Receipt",
        )

        # Initially, the last persistent instance should be None
        assert evaluation._last_persistent_instance is None

        # WHEN we set the last persistent instance
        evaluation._set_last_persistent_instance()

        # THEN it should not be None
        assert evaluation._last_persistent_instance is not None

        # AND it should be a copy of the evaluation
        assert evaluation._last_persistent_instance.name == "Test Evaluation"
        assert evaluation._last_persistent_instance.description == "Description"
        assert evaluation._last_persistent_instance.content_source == "syn123456"
        assert (
            evaluation._last_persistent_instance.submission_instructions_message
            == "Instructions"
        )
        assert (
            evaluation._last_persistent_instance.submission_receipt_message == "Receipt"
        )

        # AND it should be a different object (a copy, not a reference)
        assert evaluation._last_persistent_instance is not evaluation

    def test_last_persistent_instance_tracks_changes(self):
        """Test that the last persistent instance correctly tracks changes."""
        # GIVEN an evaluation with a set last persistent instance
        evaluation = Evaluation(
            name="Test Evaluation",
            description="Original Description",
            content_source="syn123456",
            submission_instructions_message="Instructions",
            submission_receipt_message="Receipt",
        )
        evaluation._set_last_persistent_instance()

        # WHEN we make changes to the evaluation
        evaluation.name = "Updated Evaluation"
        evaluation.description = "Updated Description"

        # THEN the last persistent instance should still have the original values
        assert evaluation._last_persistent_instance.name == "Test Evaluation"
        assert (
            evaluation._last_persistent_instance.description == "Original Description"
        )

        # AND the evaluation should have the updated values
        assert evaluation.name == "Updated Evaluation"
        assert evaluation.description == "Updated Description"

        # AND has_changed should be True
        assert evaluation.has_changed is True

        # WHEN we set the last persistent instance again
        evaluation._set_last_persistent_instance()

        # THEN it should be updated with the new values
        assert evaluation._last_persistent_instance.name == "Updated Evaluation"
        assert evaluation._last_persistent_instance.description == "Updated Description"

        # AND has_changed should be False
        assert evaluation.has_changed is False

    def test_has_changed_with_different_attributes(self):
        """Test has_changed with different attributes."""
        # GIVEN an evaluation with a set last persistent instance
        evaluation = Evaluation(
            id="9614112",
            etag="abc-123-xyz",
            name="Test Evaluation",
            description="Description",
            content_source="syn123456",
            submission_instructions_message="Instructions",
            submission_receipt_message="Receipt",
        )
        evaluation._set_last_persistent_instance()

        # Initially, has_changed should be False
        assert evaluation.has_changed is False

        # WHEN we change the attribute
        evaluation.name = "New Evaluation"
        # THEN has_changed should be True
        assert evaluation.has_changed is True
        # WHEN we set the last persistent instance
        evaluation._set_last_persistent_instance()
        # THEN has_changed should be False
        assert evaluation.has_changed is False

        # WHEN we change another attribute
        evaluation.description = "New Description"
        # THEN has_changed should be True
        assert evaluation.has_changed is True
        # WHEN we set the last persistent instance
        evaluation._set_last_persistent_instance()
        # THEN has_changed should be False
        assert evaluation.has_changed is False

        # WHEN we change the ID attribute
        evaluation.id = "9614113"
        # THEN has_changed should be True
        assert evaluation.has_changed is True
        # WHEN we set the last persistent instance
        evaluation._set_last_persistent_instance()
        # THEN has_changed should be False
        assert evaluation.has_changed is False

        # WHEN we change the etag property
        evaluation.etag = "def-456-uvw"
        # THEN has_changed should be True
        assert evaluation.has_changed is True

    def test_update_acl_permissions_update_existing(self):
        """Test updating permissions for an existing principal."""
        # GIVEN an evaluation instance
        evaluation = Evaluation()

        # AND an ACL dictionary with existing permissions
        acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "CREATE"]},
                {"principalId": 67890, "accessType": ["READ"]},
            ],
        }

        # WHEN we update permissions for an existing principal
        result = evaluation._update_acl_permissions(
            principal_id="12345",  # Test string conversion
            access_type=["READ", "SUBMIT"],
            acl=acl,
        )

        # THEN the ACL should be updated for that principal
        assert result == {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]},
                {"principalId": 67890, "accessType": ["READ"]},
            ],
        }

    def test_update_acl_permissions_add_new(self):
        """Test adding permissions for a new principal."""
        # GIVEN an evaluation instance
        evaluation = Evaluation()

        # AND an ACL dictionary without the principal
        acl = {
            "id": "9614112",
            "resourceAccess": [{"principalId": 67890, "accessType": ["READ"]}],
        }

        # WHEN we update permissions for a new principal
        result = evaluation._update_acl_permissions(
            principal_id=12345,  # Test integer input
            access_type=["READ", "SUBMIT"],
            acl=acl,
        )

        # THEN the principal should be added to the ACL
        assert result == {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 67890, "accessType": ["READ"]},
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]},
            ],
        }

    def test_update_acl_permissions_empty_acl(self):
        """Test updating permissions for an empty ACL."""
        # GIVEN an evaluation instance
        evaluation = Evaluation()

        # AND an empty ACL dictionary
        acl = {"id": "9614112", "resourceAccess": []}

        # WHEN we update permissions for a principal
        result = evaluation._update_acl_permissions(
            principal_id="12345", access_type=["READ", "SUBMIT"], acl=acl
        )

        # THEN the principal should be added to the ACL
        assert result == {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]}
            ],
        }

    def test_update_acl_permissions_remove_principal(self):
        """Test removing a principal from the ACL by providing an empty access_type list."""
        # GIVEN an evaluation instance
        evaluation = Evaluation()

        # AND an ACL dictionary with existing permissions
        acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "CREATE"]},
                {"principalId": 67890, "accessType": ["READ"]},
            ],
        }

        # AND a mocked Synapse client
        mock_client = MagicMock()
        mock_logger = MagicMock()
        mock_client.logger = mock_logger

        # WHEN we update permissions for a principal with an empty access_type list
        with patch.object(Synapse, "get_client", return_value=mock_client):
            result = evaluation._update_acl_permissions(
                principal_id="12345", access_type=[], acl=acl
            )

        # THEN the principal should be removed from the ACL
        assert result == {
            "id": "9614112",
            "resourceAccess": [{"principalId": 67890, "accessType": ["READ"]}],
        }

    @patch("synapseclient.api.evaluation_services.update_evaluation_acl")
    @patch("synapseclient.models.Evaluation.get_acl_async")
    async def test_update_acl_async_with_principal_id(
        self, mock_get_acl_async, mock_update_evaluation_acl
    ):
        """Test updating ACL permissions with principal_id and access_type."""
        # GIVEN an evaluation with an ID
        evaluation = Evaluation(id="9614112")

        # AND mocked ACL response
        mock_acl = {
            "id": "9614112",
            "resourceAccess": [{"principalId": 67890, "accessType": ["READ"]}],
        }
        mock_get_acl_async.return_value = mock_acl

        # AND a mocked update response
        updated_acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 67890, "accessType": ["READ"]},
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]},
            ],
        }
        mock_update_evaluation_acl.return_value = updated_acl

        # WHEN we update permissions for a principal
        result = await evaluation.update_acl_async(
            principal_id=12345, access_type=["READ", "SUBMIT"]
        )

        # THEN get_acl_async should be called
        mock_get_acl_async.assert_called_once_with(synapse_client=None)

        # AND the ACL should be updated with the principal's permissions
        expected_acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 67890, "accessType": ["READ"]},
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]},
            ],
        }
        mock_update_evaluation_acl.assert_called_once_with(
            acl=expected_acl, synapse_client=None
        )

        # AND the result should be the updated ACL
        assert result == updated_acl

    @patch("synapseclient.api.evaluation_services.update_evaluation_acl")
    async def test_update_acl_async_with_full_acl(self, mock_update_evaluation_acl):
        """Test updating ACL permissions with a complete ACL dictionary."""
        # GIVEN an evaluation with an ID
        evaluation = Evaluation(id="9614112")

        # AND a complete ACL dictionary
        acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]}
            ],
        }

        # AND a mocked update response
        mock_update_evaluation_acl.return_value = acl

        # WHEN we update the ACL with the complete dictionary
        result = await evaluation.update_acl_async(acl=acl)

        # THEN update_evaluation_acl should be called with the ACL
        mock_update_evaluation_acl.assert_called_once_with(acl=acl, synapse_client=None)

        # AND the result should be the updated ACL
        assert result == acl

    async def test_update_acl_async_missing_id(self):
        """Test that update_acl_async raises ValueError when ID is missing."""
        # GIVEN an evaluation without an ID
        evaluation = Evaluation()

        # WHEN we try to update the ACL
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="id must be set to update evaluation ACL"):
            await evaluation.update_acl_async(
                principal_id=12345, access_type=["READ", "SUBMIT"]
            )

        # WHEN we try to update with a complete ACL
        # THEN it should still raise a ValueError
        with pytest.raises(ValueError, match="id must be set to update evaluation ACL"):
            await evaluation.update_acl_async(
                acl={
                    "id": "9614112",
                    "resourceAccess": [
                        {"principalId": 12345, "accessType": ["READ", "SUBMIT"]}
                    ],
                }
            )

    async def test_update_acl_async_missing_parameters(self):
        """Test that update_acl_async raises ValueError when parameters are missing."""
        # GIVEN an evaluation with an ID
        evaluation = Evaluation(id="9614112")

        # WHEN we call update_acl_async without required parameters
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="Either .* or acl must be provided"):
            await evaluation.update_acl_async()

        # WHEN we provide only principal_id but no access_type
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="Either .* or acl must be provided"):
            await evaluation.update_acl_async(principal_id=12345)

    @patch("synapseclient.api.evaluation_services.update_evaluation_acl")
    @patch("synapseclient.models.Evaluation.get_acl_async")
    async def test_update_acl_async_remove_principal(
        self, mock_get_acl_async, mock_update_evaluation_acl
    ):
        """Test removing a principal from ACL by providing an empty access_type list."""
        # GIVEN an evaluation with an ID
        evaluation = Evaluation(id="9614112")

        # AND mocked ACL response with two principals
        mock_acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 67890, "accessType": ["READ"]},
                {"principalId": 12345, "accessType": ["READ", "SUBMIT"]},
            ],
        }
        mock_get_acl_async.return_value = mock_acl

        # AND a mocked update response with one principal removed
        updated_acl = {
            "id": "9614112",
            "resourceAccess": [{"principalId": 67890, "accessType": ["READ"]}],
        }
        mock_update_evaluation_acl.return_value = updated_acl

        # AND a mocked Synapse client
        mock_client = MagicMock()
        mock_logger = MagicMock()
        mock_client.logger = mock_logger

        # WHEN we update permissions for a principal with an empty access_type list
        with patch.object(Synapse, "get_client", return_value=mock_client):
            result = await evaluation.update_acl_async(
                principal_id=12345, access_type=[]
            )

        # THEN get_acl_async should be called
        mock_get_acl_async.assert_called_once_with(synapse_client=None)

        # AND the ACL should be updated with the principal removed
        expected_acl = {
            "id": "9614112",
            "resourceAccess": [{"principalId": 67890, "accessType": ["READ"]}],
        }
        mock_update_evaluation_acl.assert_called_once_with(
            acl=expected_acl, synapse_client=None
        )

        # AND the result should be the updated ACL
        assert result == updated_acl

    def test_log_message_when_removing_principal(self):
        """Test that a log message is generated when removing a principal from ACL."""
        # GIVEN an evaluation instance
        evaluation = Evaluation()

        # AND an ACL dictionary with existing permissions
        acl = {
            "id": "9614112",
            "resourceAccess": [
                {"principalId": 12345, "accessType": ["READ", "CREATE"]},
                {"principalId": 67890, "accessType": ["READ"]},
            ],
        }

        # AND a mocked Synapse client
        mock_client = MagicMock()
        mock_logger = MagicMock()
        mock_client.logger = mock_logger

        # AND a patched Synapse.get_client that returns our mock
        with patch.object(Synapse, "get_client", return_value=mock_client):
            # WHEN we update permissions for a principal with an empty access_type list
            evaluation._update_acl_permissions(
                principal_id="12345", access_type=[], acl=acl
            )

            # THEN the logger should have been called with the correct message
            mock_logger.info.assert_called_once_with(
                "Principal ID 12345 will be removed from ACL due to empty access_type"
            )
