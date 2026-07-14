"""
Unit tests for synapseclient.extensions.curator.get_curator_grid.
"""

import sys
from unittest.mock import Mock, patch

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator.get_curator_grid import get_curator_grid
from synapseclient.models.curation import (
    CurationTask,
    CurationTaskStatus,
    GridExecutionDetails,
)

# The `get_curator_grid` function is re-exported from the curator package, which
# shadows the `get_curator_grid` submodule attribute. Grab the module object from
# sys.modules so patch.object targets the module's names, not the function.
grid_module = sys.modules["synapseclient.extensions.curator.get_curator_grid"]


def _build_mock_client():
    """A Synapse-spec mock with a usable logger."""
    client = Mock(spec=Synapse)
    client.logger = Mock()
    return client


def _build_mock_task(status):
    """Build a mock CurationTask whose accessors are wired up."""
    task = Mock(spec=CurationTask)
    task.get_status.return_value = status
    return task


class TestGetCuratorGrid:
    """Test cases for the get_curator_grid function."""

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    def test_creates_grid_when_no_session_attached(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls
    ):
        """When the task has grid execution details but no active session id,
        a new grid session is created (with the given owner and timeout) and
        returned, and the existing-grid lookup is not attempted."""
        # GIVEN: A task whose status has grid execution details but no active
        # session id set
        client = _build_mock_client()
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id=None)
        )
        task = _build_mock_task(status)
        created_grid = Mock(session_id="new-session")
        task.create_grid_session.return_value = created_grid
        mock_curation_task_cls.return_value.get.return_value = task

        # WHEN: Getting the curator grid for the task
        result = get_curator_grid(
            task_id=123,
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )

        # THEN: A new grid session is created with the given owner and timeout
        # and returned
        assert result is created_grid
        task.create_grid_session.assert_called_once_with(
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )
        # AND: The existing-grid lookup is not attempted
        mock_grid_cls.return_value.get.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    def test_gets_existing_grid_by_session_id(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls
    ):
        """When the task already has an active grid session, that grid is
        fetched directly by its session id and returned, and no new grid
        session is created."""
        # GIVEN: A task whose status has an active grid session id
        client = _build_mock_client()
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id="abc-123")
        )
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get.return_value = task

        fetched_grid = Mock(session_id="abc-123")
        mock_grid_cls.return_value.get.return_value = fetched_grid

        # WHEN: Getting the curator grid for the task
        result = get_curator_grid(task_id=123, synapse_client=client)

        # THEN: The existing grid is fetched directly by its session id and
        # returned, and no new grid session is created
        assert result is fetched_grid
        mock_grid_cls.assert_called_once_with(session_id="abc-123")
        mock_grid_cls.return_value.get.assert_called_once_with(synapse_client=client)
        task.create_grid_session.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    def test_error_propagates_to_caller(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls
    ):
        """When a Synapse call fails, the exception propagates to the caller
        with its original traceback."""
        # GIVEN: A task whose get call fails with an error
        client = _build_mock_client()
        mock_synapse.get_client.return_value = client
        boom = RuntimeError("boom")
        mock_curation_task_cls.return_value.get.side_effect = boom

        # WHEN: Getting the curator grid for the task
        # THEN: The error propagates unchanged to the caller
        with pytest.raises(RuntimeError, match="boom") as exc_info:
            get_curator_grid(task_id=123, synapse_client=client)

        # AND: The propagated exception is the original one
        assert exc_info.value is boom

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    def test_raises_when_status_has_no_grid_execution_details(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls
    ):
        """When the task status has no grid execution details, a ValueError is
        raised and neither the grid lookup nor grid creation is attempted."""
        # GIVEN: A task whose status has no grid execution details
        client = _build_mock_client()
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(execution_details=None)
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get.return_value = task

        # WHEN: Getting the curator grid for the task
        # THEN: A ValueError is raised
        with pytest.raises(ValueError, match="grid execution details"):
            get_curator_grid(task_id=123, synapse_client=client)

        # AND: Neither the grid lookup nor grid creation is attempted
        mock_grid_cls.return_value.get.assert_not_called()
        task.create_grid_session.assert_not_called()
