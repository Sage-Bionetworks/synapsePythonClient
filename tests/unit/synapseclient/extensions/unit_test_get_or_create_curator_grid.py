"""
Unit tests for synapseclient.extensions.curator.get_or_create_grid.
"""

import sys
from unittest.mock import AsyncMock, Mock, patch

import pytest

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.extensions.curator.get_or_create_grid import (
    get_or_create_curator_grid_async,
)
from synapseclient.models.curation import (
    CurationTask,
    CurationTaskStatus,
    GridExecutionDetails,
)

grid_module = sys.modules["synapseclient.extensions.curator.get_or_create_grid"]


def _build_mock_task(status):
    """Build a mock CurationTask whose async accessors are wired up."""
    task = Mock(spec=CurationTask)
    task.get_status_async = AsyncMock(return_value=status)
    task.create_grid_session_async = AsyncMock()
    return task


class TestGetOrCreateCuratorGridAsync:
    """Test cases for the get_or_create_curator_grid_async function."""

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_creates_grid_when_no_session_attached(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When the task has grid execution details but no active session id,
        a new grid session is created (with the given owner and timeout) and
        returned, and the existing-grid lookup is not attempted."""
        # GIVEN: A task whose status has grid execution details but no active
        # session id set
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id=None)
        )
        task = _build_mock_task(status)
        created_grid = Mock(session_id="new-session")
        task.create_grid_session_async.return_value = created_grid
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        # WHEN: Getting the curator grid for the task
        result = await get_or_create_curator_grid_async(
            task_id=123,
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )

        # THEN: A new grid session is created with the given owner and timeout
        # and returned
        assert result is created_grid
        task.create_grid_session_async.assert_called_once_with(
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )
        # AND: The existing-grid lookup is not attempted
        mock_grid_cls.return_value.get_async.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_gets_existing_grid_by_session_id(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When the task already has an active grid session, that grid is
        fetched directly by its session id and returned, and no new grid
        session is created."""
        # GIVEN: A task whose status has an active grid session id
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id="abc-123")
        )
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        fetched_grid = Mock(session_id="abc-123")
        mock_grid_cls.return_value.get_async = AsyncMock(return_value=fetched_grid)

        # WHEN: Getting the curator grid for the task
        result = await get_or_create_curator_grid_async(
            task_id=123, synapse_client=client
        )

        # THEN: The existing grid is fetched directly by its session id and
        # returned, and no new grid session is created
        assert result is fetched_grid
        mock_grid_cls.assert_called_once_with(session_id="abc-123")
        mock_grid_cls.return_value.get_async.assert_called_once_with(
            synapse_client=client
        )
        task.create_grid_session_async.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_recreates_grid_when_attached_session_is_gone(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When the task points at a session that no longer exists (fetching it
        returns a 404), a new grid session is created, re-linked, and returned."""
        # GIVEN: A task pointing at a stale session whose fetch returns 404
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id="stale-session")
        )
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        not_found = SynapseHTTPError("not found", response=Mock(status_code=404))
        mock_grid_cls.return_value.get_async = AsyncMock(side_effect=not_found)

        created_grid = Mock(session_id="fresh-session")
        task.create_grid_session_async.return_value = created_grid

        # WHEN: Getting the curator grid for the task
        result = await get_or_create_curator_grid_async(
            task_id=123,
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )

        # THEN: The stale session is fetched (and 404s), then a new grid session
        # is created with the given owner and timeout and returned
        mock_grid_cls.assert_called_once_with(session_id="stale-session")
        assert result is created_grid
        task.create_grid_session_async.assert_called_once_with(
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_non_404_error_fetching_attached_session_propagates(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When fetching the attached session fails with a non-404 error, the
        error propagates and no new grid session is created."""
        # GIVEN: A task pointing at a session whose fetch fails with a 403
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(
            execution_details=GridExecutionDetails(active_session_id="abc-123")
        )
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        forbidden = SynapseHTTPError("forbidden", response=Mock(status_code=403))
        mock_grid_cls.return_value.get_async = AsyncMock(side_effect=forbidden)

        # WHEN: Getting the curator grid for the task
        # THEN: The error propagates unchanged
        with pytest.raises(SynapseHTTPError, match="forbidden"):
            await get_or_create_curator_grid_async(task_id=123, synapse_client=client)

        # AND: No new grid session is created
        task.create_grid_session_async.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_error_propagates_to_caller(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When a Synapse call fails, the exception propagates to the caller
        with its original traceback."""
        # GIVEN: A task whose get call fails with an error
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        boom = RuntimeError("boom")
        mock_curation_task_cls.return_value.get_async = AsyncMock(side_effect=boom)

        # WHEN: Getting the curator grid for the task
        # THEN: The error propagates unchanged to the caller
        with pytest.raises(RuntimeError, match="boom") as exc_info:
            await get_or_create_curator_grid_async(task_id=123, synapse_client=client)

        # AND: The propagated exception is the original one
        assert exc_info.value is boom

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_creates_grid_when_execution_details_is_none(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When the task status has no execution details at all (a task that has
        never had a grid), a new grid session is created and returned, and the
        existing-grid lookup is not attempted."""
        # GIVEN: A task whose status has no execution details
        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(execution_details=None)
        task = _build_mock_task(status)
        created_grid = Mock(session_id="new-session")
        task.create_grid_session_async.return_value = created_grid
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        # WHEN: Getting the curator grid for the task
        result = await get_or_create_curator_grid_async(
            task_id=123,
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )

        # THEN: A new grid session is created with the given owner and timeout
        # and returned
        assert result is created_grid
        task.create_grid_session_async.assert_called_once_with(
            owner_principal_id=42,
            timeout=30,
            synapse_client=client,
        )
        # AND: The existing-grid lookup is not attempted
        mock_grid_cls.return_value.get_async.assert_not_called()

    @patch.object(grid_module, "Grid")
    @patch.object(grid_module, "CurationTask")
    @patch.object(grid_module, "Synapse")
    async def test_raises_when_status_has_non_grid_execution_details(
        self, mock_synapse, mock_curation_task_cls, mock_grid_cls, mock_synapse_client
    ):
        """When the task status carries a non-grid execution details type, a
        ValueError is raised and neither the grid lookup nor grid creation is
        attempted."""

        # GIVEN: A task whose status has a non-grid execution details type
        class _NonGridExecutionDetails:
            pass

        client = mock_synapse_client
        mock_synapse.get_client.return_value = client
        status = CurationTaskStatus(execution_details=_NonGridExecutionDetails())
        task = _build_mock_task(status)
        mock_curation_task_cls.return_value.get_async = AsyncMock(return_value=task)

        # WHEN: Getting the curator grid for the task
        # THEN: A ValueError is raised
        with pytest.raises(ValueError, match="non-grid execution details"):
            await get_or_create_curator_grid_async(task_id=123, synapse_client=client)

        # AND: Neither the grid lookup nor grid creation is attempted
        mock_grid_cls.return_value.get_async.assert_not_called()
        task.create_grid_session_async.assert_not_called()
