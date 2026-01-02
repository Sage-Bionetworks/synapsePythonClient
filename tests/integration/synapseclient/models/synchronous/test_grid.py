"""Integration tests for the synapseclient.models.Grid class."""

import os
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.models import Grid, Project, RecordSet


class TestGrid:
    """Tests for the Grid methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def record_set_fixture(self, project_model: Project) -> RecordSet:
        """Create a RecordSet fixture for Grid testing."""
        # Create test data as a pandas DataFrame
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"],
                "value": [10.5, 20.3, 30.7, 40.1, 50.9],
                "category": ["A", "B", "A", "C", "B"],
                "active": [True, False, True, True, False],
            }
        )

        # Create a temporary CSV file
        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(temp_fd)  # Close the file descriptor
            test_data.to_csv(filename, index=False)
            self.schedule_for_cleanup(filename)

            record_set = RecordSet(
                path=filename,
                name=str(uuid.uuid4()),
                description="Test RecordSet for Grid testing",
                version_comment="Grid test version",
                version_label=str(uuid.uuid4()),
                upsert_keys=["id", "name"],
            )

            stored_record_set = record_set.store(
                parent=project_model, synapse_client=self.syn
            )
            self.schedule_for_cleanup(stored_record_set.id)
            return stored_record_set
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(filename):
                os.unlink(filename)
            raise

    def test_create_and_list_grid_sessions(self, record_set_fixture: RecordSet) -> None:
        # GIVEN: A Grid instance with a record_set_id
        grid = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating a grid session
        created_grid = grid.create(synapse_client=self.syn)

        # THEN: The grid should be created successfully
        assert created_grid is grid  # Should return the same instance
        assert created_grid.session_id is not None
        assert created_grid.started_by is not None
        assert created_grid.started_on is not None
        assert created_grid.etag is not None
        assert created_grid.source_entity_id == record_set_fixture.id

        # WHEN: Listing grid sessions
        sessions = list(
            Grid.list(source_id=record_set_fixture.id, synapse_client=self.syn)
        )

        # THEN: The created session should appear in the list
        assert len(sessions) >= 1
        session_ids = [session.session_id for session in sessions]
        assert created_grid.session_id in session_ids

        # Find our specific session
        our_session = next(
            session
            for session in sessions
            if session.session_id == created_grid.session_id
        )
        assert our_session.started_by == created_grid.started_by
        assert our_session.source_entity_id == record_set_fixture.id

    def test_create_grid_session_and_reuse_session(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: Create the first Grid instance with a record_set_id
        grid1 = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating the first grid session
        created_grid1 = grid1.create(synapse_client=self.syn)

        # THEN: A session should be created
        assert created_grid1.session_id is not None
        first_session_id = created_grid1.session_id

        # GIVEN: Create a second Grid instance with the same record_set_id
        grid2 = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating a second grid session (should reuse the existing one)
        created_grid2 = grid2.create(
            synapse_client=self.syn, attach_to_previous_session=True
        )

        # THEN: The same session should be reused
        assert created_grid2.session_id == first_session_id
        assert created_grid2.started_by == created_grid1.started_by
        assert created_grid2.started_on == created_grid1.started_on
        assert created_grid2.source_entity_id == record_set_fixture.id

    def test_create_grid_session_validation_error(self) -> None:
        # GIVEN: A Grid instance with no record_set_id or initial_query
        grid = Grid()

        # WHEN/THEN: Creating a grid session should raise ValueError
        with pytest.raises(
            ValueError,
            match="record_set_id or initial_query is required to create a GridSession",
        ):
            grid.create(synapse_client=self.syn)

    def test_delete_grid_session(self, record_set_fixture: RecordSet) -> None:
        # GIVEN: Create a grid session first
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = grid.create(synapse_client=self.syn)

        # Ensure we have a session_id
        assert created_grid.session_id is not None
        session_id = created_grid.session_id

        # WHEN: Deleting the grid session
        created_grid.delete(synapse_client=self.syn)

        # THEN: The session should no longer exist in the list
        sessions = list(
            Grid.list(source_id=record_set_fixture.id, synapse_client=self.syn)
        )

        # The deleted session should not appear in the list
        session_ids = [session.session_id for session in sessions]
        assert session_id not in session_ids

    def test_delete_grid_session_validation_error(self) -> None:
        # GIVEN: A Grid instance with no session_id
        grid = Grid()

        # WHEN/THEN: Deleting a grid session should raise ValueError
        with pytest.raises(
            ValueError,
            match="session_id is required to delete a GridSession",
        ):
            grid.delete(synapse_client=self.syn)
