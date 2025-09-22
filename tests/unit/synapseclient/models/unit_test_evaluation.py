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
