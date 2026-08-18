"""Integration tests for the synapseclient.models.Grid class (async)."""

import asyncio
import os
import tempfile
import uuid
from typing import AsyncGenerator, Callable, Generator, Tuple

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core.utils import make_bogus_data_file
from synapseclient.models import (
    AuthorizationMode,
    EntityView,
    File,
    Folder,
    Grid,
    Project,
    RecordSet,
    ViewTypeMask,
    query_async,
)
from synapseclient.models.curation import (
    CellValueFilter,
    CellValueOperator,
    CountStar,
    GridQuery,
    GridReplica,
    QueryRequest,
    RowIdFilter,
    RowIsValidFilter,
    RowSelectionFilter,
    RowValidationResultFilter,
    SelectAll,
    SelectByName,
    ValidationOperator,
)
from synapseclient.models.table_components import Query
from synapseclient.services.json_schema import JsonSchemaOrganization
from tests.integration import ASYNC_JOB_TIMEOUT_SEC, QUERY_TIMEOUT_SEC
from tests.integration.helpers import wait_for_condition


class TestGridAsync:
    """Tests for the Grid async methods."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def entity_view(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> tuple[Folder, EntityView]:
        """Create a folder with an associated EntityView for file-based testing."""
        # Create a folder
        folder = await Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(folder.id)

        entity_view = await EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE.value,
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(entity_view.id)

        return folder, entity_view

    @pytest.fixture(scope="function")
    async def record_set_fixture(self, project_model: Project) -> RecordSet:
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

            stored_record_set = await record_set.store_async(
                parent=project_model, synapse_client=self.syn
            )
            self.schedule_for_cleanup(stored_record_set.id)
            return stored_record_set
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(filename):
                os.unlink(filename)
            raise

    async def test_create_and_list_grid_sessions_async(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: A Grid instance with a record_set_id
        grid = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating a grid session
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)

        # THEN: The grid should be created successfully
        assert created_grid is grid  # Should return the same instance
        assert created_grid.session_id is not None
        assert created_grid.started_by is not None
        assert created_grid.started_on is not None
        assert created_grid.etag is not None
        assert created_grid.source_entity_id == record_set_fixture.id

        # WHEN: Listing grid sessions
        sessions = []
        async for session in Grid.list_async(
            source_id=record_set_fixture.id, synapse_client=self.syn
        ):
            sessions.append(session)

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

    async def test_get_grid_session_async(self, record_set_fixture: RecordSet) -> None:
        # GIVEN: A grid session created from a record set
        created_grid = await Grid(record_set_id=record_set_fixture.id).create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)
        assert created_grid.session_id is not None

        # WHEN: Getting the session from Synapse via a fresh Grid instance
        fetched_grid = await Grid(session_id=created_grid.session_id).get_async(
            synapse_client=self.syn
        )

        # THEN: The same instance is returned, populated from the server
        assert fetched_grid.session_id == created_grid.session_id
        assert fetched_grid.started_by == created_grid.started_by
        assert fetched_grid.started_on == created_grid.started_on
        assert fetched_grid.etag == created_grid.etag
        assert fetched_grid.source_entity_id == record_set_fixture.id

    async def test_create_grid_session_with_authorization_mode_async(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: A Grid instance with a record_set_id and an explicit authorization mode
        grid = Grid(
            record_set_id=record_set_fixture.id,
            authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
        )

        # WHEN: Creating a grid session
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )

        # AND: The grid session is scheduled for cleanup
        self.schedule_for_cleanup(created_grid)

        # THEN: The server accepts the request and creates the session successfully
        assert created_grid is grid
        assert created_grid.session_id is not None
        assert created_grid.source_entity_id == record_set_fixture.id
        assert created_grid.authorization_mode == AuthorizationMode.SOURCE_BENEFACTOR

    async def test_create_grid_session_and_reuse_session_async(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: Create the first Grid instance with a record_set_id
        grid1 = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating the first grid session
        created_grid1 = await grid1.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid1)

        # THEN: A session should be created
        assert created_grid1.session_id is not None
        first_session_id = created_grid1.session_id

        # GIVEN: Create a second Grid instance with the same record_set_id
        grid2 = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Creating a second grid session (should reuse the existing one)
        created_grid2 = await grid2.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            synapse_client=self.syn,
            attach_to_previous_session=True,
        )

        # THEN: The same session should be reused
        assert created_grid2.session_id == first_session_id
        assert created_grid2.started_by == created_grid1.started_by
        assert created_grid2.started_on == created_grid1.started_on
        assert created_grid2.source_entity_id == record_set_fixture.id

    async def test_create_grid_session_validation_error_async(self) -> None:
        # GIVEN: A Grid instance with no record_set_id or initial_query
        grid = Grid()

        # WHEN/THEN: Creating a grid session should raise ValueError
        with pytest.raises(
            ValueError,
            match="record_set_id or initial_query is required to create a GridSession",
        ):
            await grid.create_async(
                timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
            )

    async def test_delete_grid_session_async(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: Create a grid session first
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )

        # Ensure we have a session_id
        assert created_grid.session_id is not None
        session_id = created_grid.session_id

        # WHEN: Deleting the grid session
        await created_grid.delete_async(synapse_client=self.syn)

        # THEN: The session should no longer exist in the list
        sessions = []
        async for session in Grid.list_async(
            source_id=record_set_fixture.id, synapse_client=self.syn
        ):
            sessions.append(session)

        # The deleted session should not appear in the list
        session_ids = [session.session_id for session in sessions]
        assert session_id not in session_ids

    async def test_delete_grid_session_validation_error_async(self) -> None:
        # GIVEN: A Grid instance with no session_id
        grid = Grid()

        # WHEN/THEN: Deleting a grid session should raise ValueError
        with pytest.raises(
            ValueError,
            match="session_id is required to delete a GridSession",
        ):
            await grid.delete_async(synapse_client=self.syn)

    async def test_synchronize_grid_entity_view_async(
        self,
        entity_view: tuple[Folder, EntityView],
    ) -> None:
        folder, ev = entity_view

        # GIVEN: A Grid session created at T0 from an empty EntityView
        query = Query(sql=f"SELECT * FROM {ev.id}")
        grid = Grid(initial_query=query)
        created_grid = await grid.create_async(synapse_client=self.syn)
        self.schedule_for_cleanup(created_grid)

        # AND: A file uploaded into the scoped folder
        bogus_file = make_bogus_data_file()
        self.schedule_for_cleanup(bogus_file)
        uploaded_file = await File(
            path=bogus_file,
            parent_id=folder.id,
        ).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(uploaded_file.id)

        # Wait for the EntityView to index the new file
        async def file_indexed() -> bool:
            df = await query_async(
                query=f"SELECT id FROM {ev.id} WHERE id = '{uploaded_file.id}'",
                include_row_id_and_row_version=False,
                synapse_client=self.syn,
            )
            return not df.empty

        await wait_for_condition(
            condition_fn=file_indexed,
            timeout_seconds=QUERY_TIMEOUT_SEC,
        )

        # WHEN: Synchronizing the same session
        synced_grid = await created_grid.synchronize_async(
            synapse_client=self.syn, sync_type="PULL_PUSH"
        )

        # THEN: The session ID is unchanged
        assert synced_grid.session_id == created_grid.session_id
        assert synced_grid.source_entity_id == ev.id

        # AND: The downloaded CSV reflects the newly uploaded file
        dest = tempfile.mkdtemp()
        self.schedule_for_cleanup(dest)
        csv_path = await synced_grid.download_csv_async(
            destination=dest,
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            synapse_client=self.syn,
        )
        df = pd.read_csv(csv_path)
        assert uploaded_file.id in df["id"].tolist()

    async def test_synchronize_grid_recordset_async(
        self,
        record_set_fixture: RecordSet,
    ) -> None:
        # GIVEN: A Grid session created at T0 from a RecordSet
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)

        # WHEN: Synchronizing the same session
        synced_grid = await created_grid.synchronize_async(
            synapse_client=self.syn, sync_type="PULL_PUSH"
        )

        # THEN: The session ID is unchanged and the source entity is still the RecordSet
        assert synced_grid.session_id == created_grid.session_id
        assert synced_grid.source_entity_id == record_set_fixture.id

    async def test_import_csv_to_grid_session_async(
        self,
        record_set_fixture: RecordSet,
    ) -> None:
        """Test importing a CSV file into a grid session."""

        # GIVEN: Create a grid session first
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)

        assert created_grid.session_id is not None

        # AND a CSV file uploaded to Synapse
        test_data = pd.DataFrame(
            {
                "id": [6, 7, 8, 9, 10],
                "name": ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"],
                "value": [10.5, 20.3, 30.7, 40.1, 50.9],
                "category": ["A", "B", "A", "C", "B"],
                "active": [True, False, False, True, True],
            }
        )

        # Create a temporary CSV file.
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as temp_csv:
            temp_csv_path = temp_csv.name

        test_data.to_csv(temp_csv_path, index=False)
        self.schedule_for_cleanup(temp_csv_path)

        # WHEN: Importing the CSV into the grid session
        imported_grid = await created_grid.import_csv_async(
            path=temp_csv_path,
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            synapse_client=self.syn,
        )

        # THEN: The import should complete and return the Grid with the same session
        assert imported_grid.session_id == created_grid.session_id

        # WHEN: Exporting the grid back to the record set
        exported_grid = await imported_grid.export_to_record_set_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )

        # THEN: The export should contain 10 total rows
        # (5 from the original record set + 5 imported)
        assert exported_grid.validation_summary_statistics is not None
        assert (
            exported_grid.validation_summary_statistics.total_number_of_children == 10
        )

    async def test_download_csv_async(self, record_set_fixture: RecordSet) -> None:
        # GIVEN: Create a grid session first
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)

        # WHEN: Downloading the grid results as CSV
        temp_dir = tempfile.mkdtemp()
        self.schedule_for_cleanup(temp_dir)
        csv_path = await created_grid.download_csv_async(
            synapse_client=self.syn,
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            destination=temp_dir,
        )

        # THEN: The CSV content should be returned and match the original data
        assert os.path.exists(csv_path)
        df = pd.read_csv(csv_path)
        expected_df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"],
                "value": [10.5, 20.3, 30.7, 40.1, 50.9],
                "category": ["A", "B", "A", "C", "B"],
                "active": [True, False, True, True, False],
            }
        )
        pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)

    async def test_create_replica_async(self, record_set_fixture: RecordSet) -> None:
        # GIVEN: A grid session created first
        grid = Grid(record_set_id=record_set_fixture.id)
        created_grid = await grid.create_async(
            timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
        )
        self.schedule_for_cleanup(created_grid)

        # WHEN: Creating a replica for the grid session
        replica = await created_grid._create_replica_async(synapse_client=self.syn)

        # THEN: A populated GridReplica should be returned
        assert isinstance(replica, GridReplica)
        assert replica.replica_id is not None
        assert replica.grid_session_id == created_grid.session_id
        assert replica.created_by == created_grid.started_by

    async def test_connect_async_creates_session_and_binds_replica(
        self, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN: A Grid instance with no existing session
        grid = Grid(record_set_id=record_set_fixture.id)

        # WHEN: Connecting to the grid
        async with grid.connect_async(synapse_client=self.syn) as connected_grid:
            self.schedule_for_cleanup(connected_grid)

            # THEN: A session should have been created and a replica bound
            assert connected_grid is grid
            assert connected_grid.session_id is not None
            assert connected_grid._replica_id is not None

        # AND: The replica should be unbound once the block exits
        assert grid._replica_id is None


CELL_VALUE_FILTER_CASES = [
    pytest.param("category", CellValueOperator.EQUALS, "A", {1, 3}, id="equals"),
    pytest.param(
        "category", CellValueOperator.NOT_EQUALS, "A", {2, 4, 5}, id="not_equals"
    ),
    pytest.param("category", CellValueOperator.IN, ["A", "B"], {1, 2, 3, 5}, id="in"),
    pytest.param("category", CellValueOperator.NOT_IN, ["A", "B"], {4}, id="not_in"),
    pytest.param("value", CellValueOperator.GREATER_THAN, 100, {4}, id="greater_than"),
    pytest.param(
        "value", CellValueOperator.LESS_THAN, 100, {1, 2, 3, 5}, id="less_than"
    ),
    pytest.param(
        "value",
        CellValueOperator.GREATER_THAN_OR_EQUALS,
        50.9,
        {4, 5},
        id="greater_than_or_equals",
    ),
    pytest.param(
        "value",
        CellValueOperator.LESS_THAN_OR_EQUALS,
        20.3,
        {1, 2},
        id="less_than_or_equals",
    ),
    pytest.param("name", CellValueOperator.LIKE, "%lpha", {1}, id="like"),
    pytest.param(
        "name", CellValueOperator.NOT_LIKE, "%lpha", {2, 3, 4, 5}, id="not_like"
    ),
    pytest.param("category", CellValueOperator.IS_NULL, None, set(), id="is_null"),
    pytest.param(
        "category",
        CellValueOperator.IS_NOT_NULL,
        None,
        {1, 2, 3, 4, 5},
        id="is_not_null",
    ),
    pytest.param(
        "category", CellValueOperator.IS_UNDEFINED, None, set(), id="is_undefined"
    ),
    pytest.param(
        "category",
        CellValueOperator.IS_DEFINED,
        None,
        {1, 2, 3, 4, 5},
        id="is_defined",
    ),
]


class TestGridValidateRowsAsync:
    """Tests for Grid.validate_rows_async, covering every SelectItem and
    Filter option, using one shared bound-schema grid session for the whole
    class. Every query in this class is read-only, so
    sharing the same session across tests is safe."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="class")
    def create_test_schema(
        self, syn: Synapse
    ) -> Generator[Tuple[JsonSchemaOrganization, str, list], None, None]:
        """Create a test JSON schema matching the RecordSet's columns, shared
        for the lifetime of this class."""
        org_name = "gridtest" + uuid.uuid4().hex[:6]
        schema_name = "grid.validation.schema"

        js = syn.service("json_schema")
        created_org = js.create_organization(org_name)
        record_set_ids = []  # Track RecordSets that need schema unbinding

        try:
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"https://example.com/schema/{schema_name}.json",
                "title": "Grid ValidateRows Schema",
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {
                        "description": "Name of the record (min 3 characters)",
                        "type": "string",
                        "minLength": 3,
                    },
                    "value": {
                        "description": "Numeric value (must be >= 0 and <= 1000)",
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1000,
                    },
                    "category": {
                        "description": "Category classification (A, B, C, or D only)",
                        "type": "string",
                        "enum": ["A", "B", "C", "D"],
                    },
                    "active": {"type": "boolean"},
                },
                "required": ["id", "name"],
            }

            test_org = js.JsonSchemaOrganization(org_name)
            created_schema = test_org.create_json_schema(schema, schema_name, "0.0.1")
            yield test_org, created_schema.uri, record_set_ids
        finally:
            for record_set_id in record_set_ids:
                try:
                    RecordSet(id=record_set_id).unbind_schema(synapse_client=syn)
                except Exception:
                    pass  # Ignore errors if already unbound or deleted

            try:
                js.delete_json_schema(created_schema.uri)
            except Exception:
                pass  # Ignore if schema can't be deleted

            try:
                js.delete_organization(created_org["id"])
            except Exception:
                pass  # Ignore if org can't be deleted

    @pytest.fixture(scope="class")
    async def record_set_with_schema_fixture(
        self,
        project_model: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
        create_test_schema: Tuple[JsonSchemaOrganization, str, list],
    ) -> RecordSet:
        """Create one RecordSet with a bound JSON schema and a mix of
        valid/invalid rows (ids 3 and 4 invalid), shared by every test in
        this class."""
        _, schema_uri, record_set_ids = create_test_schema

        # Row 3: INVALID - "name" below the schema's minLength of 3
        # Row 4: INVALID - "value" above the schema's maximum of 1000, and
        #   "category" not in the schema's enum
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alpha", "Beta", "AB", "Delta", "Epsilon"],
                "value": [10.5, 20.3, 30.7, 1500.0, 50.9],
                "category": ["A", "B", "A", "X", "B"],
                "active": [True, False, True, True, False],
            }
        )

        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(temp_fd)  # Close the file descriptor
            test_data.to_csv(filename, index=False)
            schedule_for_cleanup(filename)

            record_set = RecordSet(
                path=filename,
                name=str(uuid.uuid4()),
                description="Test RecordSet with bound schema for Grid testing",
                version_comment="Grid schema test version",
                version_label=str(uuid.uuid4()),
                upsert_keys=["id", "name"],
            )

            stored_record_set = await record_set.store_async(
                parent=project_model, synapse_client=syn
            )
            schedule_for_cleanup(stored_record_set.id)
            record_set_ids.append(stored_record_set.id)  # Track for schema cleanup

            await asyncio.sleep(3)

            await stored_record_set.bind_schema_async(
                json_schema_uri=schema_uri,
                enable_derived_annotations=False,
                synapse_client=syn,
            )

            # Wait for schema binding to be fully processed by backend
            await asyncio.sleep(5)

            return stored_record_set
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(filename):
                os.unlink(filename)
            raise

    @pytest.fixture(scope="class")
    async def connected_grid(
        self,
        record_set_with_schema_fixture: RecordSet,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> AsyncGenerator[Grid, None]:
        """Connect once, binding a single replica that every test method in
        this class reuses for its (read-only) validate_rows_async calls."""
        grid = Grid(record_set_id=record_set_with_schema_fixture.id)
        async with grid.connect_async(synapse_client=syn) as connected:
            schedule_for_cleanup(connected)
            yield connected

    async def test_select_all_returns_full_data_and_validation_results(
        self, connected_grid: Grid
    ) -> None:
        # WHEN: Selecting every column for every row
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(query=GridQuery(column_selection=[SelectAll()])),
            synapse_client=self.syn,
        )

        # THEN: Every row has full data and real per-row validation results
        assert len(result.rows) == 5
        for row in result.rows:
            assert {"id", "name", "value", "category", "active"} <= set(row.data.keys())
            assert row.validation_results is not None

        results_by_id = {row.data["id"]: row.validation_results for row in result.rows}
        assert results_by_id[1].is_valid is True
        assert results_by_id[2].is_valid is True
        assert results_by_id[3].is_valid is False
        assert results_by_id[4].is_valid is False
        assert results_by_id[5].is_valid is True

    async def test_select_by_name_returns_only_requested_columns(
        self, connected_grid: Grid
    ) -> None:
        # WHEN: Selecting only "id" and "category" by name
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[
                        SelectByName(column_name="id"),
                        SelectByName(column_name="category"),
                    ]
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: Every row's data contains exactly those two columns
        assert len(result.rows) == 5
        for row in result.rows:
            assert set(row.data.keys()) == {"id", "category"}

    async def test_row_is_valid_filter_returns_only_invalid_rows(
        self, connected_grid: Grid
    ) -> None:
        # WHEN: Filtering to only invalid rows
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[SelectAll()],
                    filters=[RowIsValidFilter(value=False)],
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: Only the two rows that violate the schema are returned
        assert {row.data["id"] for row in result.rows} == {3, 4}

    @pytest.mark.parametrize(
        "operator,validation_result_value,expected_ids",
        [
            # LIKE '%' matches any non-empty message, so only the invalid
            # rows (which have a message) match.
            pytest.param(ValidationOperator.LIKE, "%", {3, 4}, id="like"),
            # LIKE on this specific message should only match row 4.
            pytest.param(
                ValidationOperator.LIKE,
                "%1500.0 is not less or equal to 1000%",
                {4},
                id="like_specific_message",
            ),
            # NOT_LIKE '%' matches rows with no message at all (valid rows):
            pytest.param(ValidationOperator.NOT_LIKE, "%", {1, 2, 5}, id="not_like"),
        ],
    )
    async def test_row_validation_result_filter_operators(
        self,
        connected_grid: Grid,
        operator: ValidationOperator,
        validation_result_value: str,
        expected_ids: set,
    ) -> None:
        # WHEN: Filtering rows by their validation message
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[SelectAll()],
                    filters=[
                        RowValidationResultFilter(
                            operator=operator,
                            validation_result_value=validation_result_value,
                        )
                    ],
                    include_validation_messages=True,
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: Only the rows matching that operator/value are returned
        assert {row.data["id"] for row in result.rows} == expected_ids

    async def test_row_id_filter_returns_only_specified_rows(
        self, connected_grid: Grid
    ) -> None:
        # GIVEN: row_ids captured from an initial unfiltered query
        all_rows_result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(query=GridQuery(column_selection=[SelectAll()])),
            synapse_client=self.syn,
        )
        row_ids_by_data_id = {
            row.data["id"]: row.row_id for row in all_rows_result.rows
        }

        # WHEN: Filtering by two explicit row_ids (ids 1 and 5)
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[SelectAll()],
                    filters=[
                        RowIdFilter(
                            row_ids_in=[
                                row_ids_by_data_id[1],
                                row_ids_by_data_id[5],
                            ]
                        )
                    ],
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: Only those two rows are returned
        assert {row.data["id"] for row in result.rows} == {1, 5}

    async def test_row_selection_filter_excludes_unselected_rows(
        self, connected_grid: Grid
    ) -> None:
        # WHEN: Filtering to rows the user has actively selected in the
        # interface. Nothing has been marked as selected through this API
        # path, so no rows should match.
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[SelectAll()],
                    filters=[RowSelectionFilter(is_selected=True)],
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: No rows are selected
        assert len(result.rows) == 0

    async def test_count_star_returns_aggregate_count(
        self, connected_grid: Grid
    ) -> None:
        # WHEN: Counting only the valid rows instead of selecting them
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[CountStar(alias="total")],
                    filters=[RowIsValidFilter(value=True)],
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: A single aggregate row with the count is returned
        assert len(result.rows) == 1
        assert result.rows[0].data["count"] == 3

    @pytest.mark.parametrize(
        "column_name,operator,value,expected_ids", CELL_VALUE_FILTER_CASES
    )
    async def test_cell_value_filter_operators(
        self,
        connected_grid: Grid,
        column_name: str,
        operator: CellValueOperator,
        value,
        expected_ids: set,
    ) -> None:
        # WHEN: Filtering by a single column/operator/value combination
        result = await connected_grid.validate_rows_async(
            query_request=QueryRequest(
                query=GridQuery(
                    column_selection=[SelectAll()],
                    filters=[
                        CellValueFilter(
                            column_name=column_name, operator=operator, value=value
                        )
                    ],
                )
            ),
            synapse_client=self.syn,
        )

        # THEN: Only the rows matching that operator/value are returned
        assert {row.data["id"] for row in result.rows} == expected_ids
