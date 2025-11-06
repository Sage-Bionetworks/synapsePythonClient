import asyncio
import tempfile
import uuid
from typing import Callable, List

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import synapseclient.models.mixins.table_components as table_module
from synapseclient import Synapse
from synapseclient.api import get_default_columns
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    EntityView,
    File,
    Folder,
    Project,
    UsedURL,
    ViewTypeMask,
    query,
    query_part_mask,
)


class TestEntityView:
    """Integration tests for Entity View functionality."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def setup_files_in_folder(
        self, project_model: Project, num_files: int = 4
    ) -> tuple[Folder, List[File]]:
        """Helper to create a folder with files for testing"""
        # Create a folder
        folder = Folder(name=str(uuid.uuid4()), parent_id=project_model.id).store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(folder.id)

        # Create files
        files = []
        filename = utils.make_bogus_uuid_file()

        # First file has a real path
        file1 = File(
            parent_id=folder.id,
            name="file1",
            path=filename,
            description="file1_description",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(file1.id)
        files.append(file1)

        # Other files reuse the file handle
        for i in range(2, num_files + 1):
            file = File(
                parent_id=folder.id,
                name=f"file{i}",
                data_file_handle_id=file1.data_file_handle_id,
                description=f"file{i}_description",
            ).store(synapse_client=self.syn)
            self.schedule_for_cleanup(file.id)
            files.append(file)

        return folder, files

    def test_entityview_creation_with_columns(self, project_model: Project) -> None:
        """Test creating entity views with different column configurations"""
        # GIVEN parameters for three different entity view configurations
        test_cases = [
            {
                "name": "default_columns",
                "description": "EntityView with default columns",
                "columns": None,
                "include_default_columns": True,
                "expected_column_count": None,  # Will be set after getting default columns
            },
            {
                "name": "single_column",
                "description": "EntityView with a single column",
                "columns": [Column(name="test_column", column_type=ColumnType.STRING)],
                "include_default_columns": False,
                "expected_column_count": 1,
            },
            {
                "name": "multiple_columns",
                "description": "EntityView with multiple columns",
                "columns": [
                    Column(name="test_column", column_type=ColumnType.STRING),
                    Column(name="test_column2", column_type=ColumnType.INTEGER),
                ],
                "include_default_columns": False,
                "expected_column_count": 2,
            },
        ]

        # Get default column count to set expectation
        default_columns = asyncio.run(
            get_default_columns(
                view_type_mask=ViewTypeMask.FILE.value, synapse_client=self.syn
            )
        )
        test_cases[0]["expected_column_count"] = len(default_columns)

        # Test each case
        for case in test_cases:
            # WHEN I create and store an entity view with the specified configuration
            entityview = EntityView(
                name=f"{case['name']}_{str(uuid.uuid4())}",
                parent_id=project_model.id,
                description=case["description"],
                columns=case["columns"],
                view_type_mask=ViewTypeMask.FILE,
                include_default_columns=case["include_default_columns"],
            )
            entityview = entityview.store(synapse_client=self.syn)
            self.schedule_for_cleanup(entityview.id)

            # THEN the entity view should be created with correct properties
            retrieved_view = EntityView(id=entityview.id).get(
                synapse_client=self.syn, include_columns=True
            )

            # Verify basic properties
            assert retrieved_view.id == entityview.id
            assert retrieved_view.name == entityview.name
            assert retrieved_view.description == entityview.description

            # Verify columns
            assert len(retrieved_view.columns) == case["expected_column_count"]

            if case["name"] == "default_columns":
                # Verify default columns
                for column in default_columns:
                    assert column.name in retrieved_view.columns
                    assert column == retrieved_view.columns[column.name]
            elif case["name"] == "single_column":
                # Verify single column
                assert "test_column" in retrieved_view.columns
                assert (
                    retrieved_view.columns["test_column"].column_type
                    == ColumnType.STRING
                )
            elif case["name"] == "multiple_columns":
                # Verify multiple columns
                assert "test_column" in retrieved_view.columns
                assert "test_column2" in retrieved_view.columns
                assert (
                    retrieved_view.columns["test_column"].column_type
                    == ColumnType.STRING
                )
                assert (
                    retrieved_view.columns["test_column2"].column_type
                    == ColumnType.INTEGER
                )

    def test_entityview_invalid_column(self, project_model: Project) -> None:
        """Test creating an entity view with an invalid column"""
        # GIVEN an entity view with an invalid column
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test entityview with invalid column",
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,  # Invalid: too large
                )
            ],
            view_type_mask=ViewTypeMask.FILE,
        )

        # WHEN I try to store the entity view
        # THEN an exception should be raised
        with pytest.raises(SynapseHTTPError) as e:
            entityview.store(synapse_client=self.syn)

        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

    def test_entityview_with_files_in_scope(self, project_model: Project) -> None:
        """Test creating entity view with files in scope and querying it"""
        # GIVEN a folder with files
        folder, files = self.setup_files_in_folder(project_model)

        # WHEN I create an entity view with that folder in its scope
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE,
            scope_ids=[folder.id],
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # AND I query the data in the entity view
        results = query(f"SELECT * FROM {entityview.id}", synapse_client=self.syn)

        # THEN the data for all files should be present in the view
        assert len(results) == len(files)

        # AND the file properties should match
        for i, file in enumerate(files):
            assert results["name"][i] == file.name
            assert results["description"][i] == file.description

    def test_update_rows_and_annotations(
        self, mocker: MockerFixture, project_model: Project
    ) -> None:
        """Test updating rows in an entity view from different sources and verifying annotations"""
        # GIVEN a folder with files
        folder, files = self.setup_files_in_folder(project_model)

        # AND an entity view with columns and files in scope
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
            columns=[
                Column(name="column_string", column_type=ColumnType.STRING),
                Column(name="integer_column", column_type=ColumnType.INTEGER),
                Column(name="float_column", column_type=ColumnType.DOUBLE),
            ],
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # Custom wrapper to capture call stack
        original_csv_to_pandas_df = table_module.csv_to_pandas_df
        call_info = []

        def csv_wrapper(*args, **kwargs):
            import traceback

            stack = traceback.extract_stack()

            # Find the calling function (skip the wrapper itself)
            calling_function = None
            for frame in reversed(stack[:-1]):  # Skip current frame
                if "_upsert_rows_async" in frame.name:
                    calling_function = "_upsert_rows_async"
                    break
                elif "query_async" in frame.name:
                    calling_function = "query_async"
                    break
                else:
                    pass

            call_info.append(
                {
                    "caller": calling_function,
                    "args": args,
                    "kwargs": kwargs,
                    "filepath": kwargs.get("filepath", args[0] if args else None),
                }
            )

            return original_csv_to_pandas_df(*args, **kwargs)

        # Patch the csv_to_pandas_df function to use the wrapper
        mock_csv_to_pandas_df = mocker.patch.object(
            table_module, "csv_to_pandas_df", side_effect=csv_wrapper
        )

        # Create test data for all files
        test_data = {
            "id": [file.id for file in files],
            "column_string": [f"value{i+1}" for i in range(len(files))],
            "integer_column": [
                i + 1 if i < len(files) - 1 else None for i in range(len(files))
            ],
            "float_column": [
                float(i + 1.1) if i < len(files) - 1 else None
                for i in range(len(files))
            ],
        }

        # Test three update methods: CSV, DataFrame, and dictionary
        update_methods = ["csv", "dataframe", "dict"]

        for method in update_methods:
            # Reset the spy for each method
            call_info.clear()

            # WHEN I update rows using different input types
            if method == "csv":
                # Use CSV file
                filepath = f"{tempfile.mkdtemp()}/upload_{uuid.uuid4()}.csv"
                self.schedule_for_cleanup(filepath)
                pd.DataFrame(test_data).to_csv(
                    filepath, index=False, float_format="%.12g"
                )

                entityview.update_rows(
                    values=filepath,
                    primary_keys=["id"],
                    synapse_client=self.syn,
                    wait_for_eventually_consistent_view=True,
                )

                # THEN the CSV conversion function should be called
                _upsert_rows_async_calls = [
                    call for call in call_info if call["caller"] == "_upsert_rows_async"
                ]
                assert len(_upsert_rows_async_calls) == 1

            elif method == "dataframe":
                # Use DataFrame
                entityview.update_rows(
                    values=pd.DataFrame(test_data),
                    primary_keys=["id"],
                    synapse_client=self.syn,
                    wait_for_eventually_consistent_view=True,
                )

                # THEN the CSV conversion function should NOT be called
                _upsert_rows_async_calls = [
                    call for call in call_info if call["caller"] == "_upsert_rows_async"
                ]
                assert len(_upsert_rows_async_calls) == 0

            else:  # dict
                # Use dictionary
                entityview.update_rows(
                    values=test_data,
                    primary_keys=["id"],
                    synapse_client=self.syn,
                    wait_for_eventually_consistent_view=True,
                )

                # THEN the CSV conversion function should NOT be called
                _upsert_rows_async_calls = [
                    call for call in call_info if call["caller"] == "_upsert_rows_async"
                ]
                assert len(_upsert_rows_async_calls) == 0

            # THEN the columns should exist in the entity view
            assert "column_string" in entityview.columns
            assert "integer_column" in entityview.columns
            assert "float_column" in entityview.columns

            # AND the data should be queryable
            query_results = query(
                f"SELECT * FROM {entityview.id}", synapse_client=self.syn
            )

            # AND the values should match what we set
            # Create series with matching names or ignore name attribute in comparison
            pd.testing.assert_series_equal(
                query_results["column_string"],
                pd.Series(test_data["column_string"], name="column_string"),
                check_names=True,
            )
            pd.testing.assert_series_equal(
                query_results["integer_column"],
                pd.Series(test_data["integer_column"], name="integer_column"),
                check_names=True,
            )
            pd.testing.assert_series_equal(
                query_results["float_column"],
                pd.Series(test_data["float_column"], name="float_column"),
                check_names=True,
            )

            # AND the annotations should be updated on the files
            for i, file in enumerate(files):
                file_copy = File(id=file.id, download_file=False).get(
                    synapse_client=self.syn
                )
                assert file_copy.annotations["column_string"] == [
                    test_data["column_string"][i]
                ]

                if test_data["integer_column"][i] is not None:
                    assert file_copy.annotations["integer_column"] == [
                        test_data["integer_column"][i]
                    ]
                else:
                    assert "integer_column" not in file_copy.annotations.keys()

                if test_data["float_column"][i] is not None:
                    assert file_copy.annotations["float_column"] == [
                        test_data["float_column"][i]
                    ]
                else:
                    assert "float_column" not in file_copy.annotations.keys()

    def test_update_rows_without_id_column(self, project_model: Project) -> None:
        """Test that updating rows requires the id column"""
        # GIVEN a folder with files and an entity view
        folder, _ = self.setup_files_in_folder(project_model, num_files=1)

        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I delete the id column and try to update rows
        entityview.delete_column(name="id")
        entityview.store(synapse_client=self.syn)

        # THEN it should raise an exception
        with pytest.raises(ValueError) as e:
            entityview.update_rows(
                values={},
                primary_keys=["id"],
                synapse_client=self.syn,
                wait_for_eventually_consistent_view=True,
            )

        assert (
            "The 'id' column is required to wait for eventually consistent views."
            in str(e.value)
        )

    def test_column_modifications(self, project_model: Project) -> None:
        """Test renaming and deleting columns in an entity view"""
        # GIVEN an entity view with multiple columns
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"

        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
            view_type_mask=ViewTypeMask.FILE,
            include_default_columns=False,
        )

        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I rename a column
        new_column_name = "new_column_string"
        entityview.columns[old_column_name].name = new_column_name
        entityview.store(synapse_client=self.syn)

        # THEN the column should be renamed
        # Both in the local instance
        assert new_column_name in entityview.columns
        assert old_column_name not in entityview.columns

        # And on the server
        retrieved_view = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_column_name in retrieved_view.columns
        assert old_column_name not in retrieved_view.columns

        # WHEN I delete a column
        entityview.delete_column(name=new_column_name)
        entityview.store(synapse_client=self.syn)

        # THEN the column should be deleted
        # Both in the local instance
        assert new_column_name not in entityview.columns
        assert column_to_keep in entityview.columns

        # And on the server
        retrieved_view = EntityView(id=entityview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_column_name not in retrieved_view.columns
        assert column_to_keep in retrieved_view.columns

    def test_query_with_part_mask(self, project_model: Project) -> None:
        """Test querying an entity view with different part masks"""
        # GIVEN a folder with files
        folder, files = self.setup_files_in_folder(project_model, num_files=2)

        # AND an entity view with the folder in scope
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            view_type_mask=ViewTypeMask.FILE.value,
            scope_ids=[folder.id],
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I query with a full part mask
        query_results = 0x1
        query_count = 0x2
        sum_file_size_bytes = 0x40
        last_updated_on = 0x80
        full_part_mask = (
            query_results | query_count | sum_file_size_bytes | last_updated_on
        )

        full_results = query_part_mask(
            query=f"SELECT * FROM {entityview.id} ORDER BY id ASC",
            synapse_client=self.syn,
            part_mask=full_part_mask,
        )

        # THEN all parts should be present in the results
        assert full_results.count == len(files)
        assert full_results.sum_file_sizes is not None
        assert full_results.sum_file_sizes.greater_than is not None
        assert full_results.sum_file_sizes.sum_file_size_bytes is not None
        assert full_results.last_updated_on is not None
        assert full_results.result["name"].tolist() == [file.name for file in files]

        # WHEN I query with only the results part mask
        results_only = query_part_mask(
            query=f"SELECT * FROM {entityview.id} ORDER BY id ASC",
            synapse_client=self.syn,
            part_mask=query_results,
        )

        # THEN only the results should be present
        assert results_only.count is None
        assert results_only.sum_file_sizes is None
        assert results_only.last_updated_on is None
        assert results_only.result["name"].tolist() == [file.name for file in files]

    def test_snapshot_functionality(self, project_model: Project) -> None:
        """Test creating snapshots of entity views with different activity configurations"""
        # GIVEN a folder with a file
        folder, [file] = self.setup_files_in_folder(project_model, num_files=1)

        # AND an entity view with an activity
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test entityview for snapshots",
            scope_ids=[folder.id],
            view_type_mask=ViewTypeMask.FILE,
            activity=Activity(
                name="Original activity",
                used=[UsedURL(name="Synapse", url="https://synapse.org")],
            ),
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # Test different snapshot configurations
        snapshot_configs = [
            {
                "name": "with_activity_pulled_forward",
                "include_activity": True,
                "associate_activity_to_new_version": True,
                "expect_activity_in_snapshot": True,
                "expect_activity_in_new_version": True,
            },
            {
                "name": "with_activity_not_pulled_forward",
                "include_activity": True,
                "associate_activity_to_new_version": False,
                "expect_activity_in_snapshot": True,
                "expect_activity_in_new_version": False,
            },
            {
                "name": "without_activity",
                "include_activity": False,
                "associate_activity_to_new_version": False,
                "expect_activity_in_snapshot": False,
                "expect_activity_in_new_version": False,
            },
        ]

        # Test each configuration in succession
        for i, config in enumerate(snapshot_configs):
            # WHEN I create a snapshot with this configuration
            snapshot = entityview.snapshot(
                comment=f"Snapshot {i+1}",
                label=f"Label {i+1}",
                include_activity=config["include_activity"],
                associate_activity_to_new_version=config[
                    "associate_activity_to_new_version"
                ],
                synapse_client=self.syn,
            )

            # THEN the snapshot should be created
            assert snapshot.results is not None

            # AND the snapshot should have the expected properties
            snapshot_version = i + 1
            snapshot_instance = EntityView(
                id=entityview.id, version_number=snapshot_version
            ).get(synapse_client=self.syn, include_activity=True)

            assert snapshot_instance.version_number == snapshot_version
            assert snapshot_instance.version_comment == f"Snapshot {snapshot_version}"
            assert snapshot_instance.version_label == f"Label {snapshot_version}"

            # Check activity in snapshot
            if config["expect_activity_in_snapshot"]:
                assert snapshot_instance.activity is not None
                assert snapshot_instance.activity.name == "Original activity"
                assert snapshot_instance.activity.used[0].name == "Synapse"
                assert snapshot_instance.activity.used[0].url == "https://synapse.org"
            else:
                assert snapshot_instance.activity is None

            # Check activity in new version
            newest_instance = EntityView(id=entityview.id).get(
                synapse_client=self.syn, include_activity=True
            )
            assert newest_instance.version_number == snapshot_version + 1

            if config["expect_activity_in_new_version"]:
                assert newest_instance.activity is not None
                assert newest_instance.activity.name == "Original activity"
            else:
                assert newest_instance.activity is None

    def test_snapshot_with_no_scope(self, project_model: Project) -> None:
        """Test that creating a snapshot of an entity view with no scope raises an error"""
        # GIVEN an entity view with no scope
        entityview = EntityView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test entityview with no scope",
            view_type_mask=ViewTypeMask.FILE,
        )
        entityview = entityview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(entityview.id)

        # WHEN I try to create a snapshot
        # THEN it should raise an error
        with pytest.raises(SynapseHTTPError) as e:
            entityview.snapshot(
                comment="My snapshot",
                label="My snapshot label",
                synapse_client=self.syn,
            )

        assert (
            "400 Client Error: You cannot create a version of a view that has no scope."
            in str(e.value)
        )
