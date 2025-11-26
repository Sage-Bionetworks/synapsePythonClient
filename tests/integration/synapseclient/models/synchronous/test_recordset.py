"""Integration tests for the synapseclient.models.RecordSet class."""

import os
import tempfile
import time
import uuid
from typing import Callable, Generator, Tuple

import pandas as pd
import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Folder,
    Project,
    RecordSet,
    UsedEntity,
    UsedURL,
)
from synapseclient.models.curation import Grid
from synapseclient.services.json_schema import JsonSchemaOrganization


class TestRecordSetStore:
    """Tests for the RecordSet.store method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(autouse=True, scope="function")
    def record_set_fixture(
        self, schedule_for_cleanup: Callable[..., None]
    ) -> RecordSet:
        """Create a RecordSet fixture for testing."""
        filename = utils.make_bogus_uuid_file()
        schedule_for_cleanup(filename)
        return RecordSet(
            path=filename,
            description="This is a test RecordSet.",
            version_comment="My version comment",
            version_label=str(uuid.uuid4()),
            upsert_keys=["id", "name"],
        )

    def test_store_in_project(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet
        record_set_fixture.name = str(uuid.uuid4())

        # WHEN I store the RecordSet in a project
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored successfully
        assert stored_record_set.id is not None
        assert stored_record_set.name == record_set_fixture.name
        assert stored_record_set.description == "This is a test RecordSet."
        assert stored_record_set.version_comment == "My version comment"
        assert stored_record_set.upsert_keys == ["id", "name"]
        assert stored_record_set.parent_id == project_model.id
        assert stored_record_set.etag is not None
        assert stored_record_set.created_on is not None
        assert stored_record_set.created_by is not None

    def test_store_in_folder(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a folder within a project
        folder = Folder(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # AND a RecordSet
        record_set_fixture.name = str(uuid.uuid4())

        # WHEN I store the RecordSet in the folder
        stored_record_set = record_set_fixture.store(
            parent=folder, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored successfully in the folder
        assert stored_record_set.id is not None
        assert stored_record_set.name == record_set_fixture.name
        assert stored_record_set.parent_id == folder.id
        assert stored_record_set.etag is not None

    def test_store_with_activity(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with activity
        record_set_fixture.name = str(uuid.uuid4())
        activity = Activity(
            name="Test Activity",
            description="Test activity for RecordSet",
            used=[
                UsedURL(name="Example URL", url="https://example.com"),
                UsedEntity(target_id="syn123456"),
            ],
        )
        record_set_fixture.activity = activity

        # WHEN I store the RecordSet
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet and activity should be stored successfully
        assert stored_record_set.id is not None
        assert stored_record_set.activity is not None
        assert stored_record_set.activity.name == "Test Activity"
        assert stored_record_set.activity.description == "Test activity for RecordSet"
        assert len(stored_record_set.activity.used) == 2

    def test_store_with_annotations(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with annotations
        record_set_fixture.name = str(uuid.uuid4())
        record_set_fixture.annotations = {
            "test_annotation": ["test_value"],
            "numeric_annotation": [42],
            "boolean_annotation": [True],
        }

        # WHEN I store the RecordSet
        stored_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(stored_record_set.id)

        # THEN the RecordSet should be stored with annotations
        assert stored_record_set.id is not None
        assert stored_record_set.annotations is not None
        assert "test_annotation" in stored_record_set.annotations
        assert "numeric_annotation" in stored_record_set.annotations
        assert "boolean_annotation" in stored_record_set.annotations
        assert stored_record_set.annotations["test_annotation"] == ["test_value"]
        assert stored_record_set.annotations["numeric_annotation"] == [42]
        assert stored_record_set.annotations["boolean_annotation"] == [True]

    def test_store_update_existing_record_set(
        self, project_model: Project, record_set_fixture: RecordSet
    ) -> None:
        # GIVEN an existing RecordSet
        record_set_fixture.name = str(uuid.uuid4())
        original_record_set = record_set_fixture.store(
            parent=project_model, synapse_client=self.syn
        )
        self.schedule_for_cleanup(original_record_set.id)

        # WHEN I update the RecordSet with new metadata
        original_record_set.description = "Updated description"
        original_record_set.version_comment = "Updated version comment"

        updated_record_set = original_record_set.store(synapse_client=self.syn)

        # THEN the RecordSet should be updated successfully
        assert updated_record_set.id == original_record_set.id
        assert updated_record_set.description == "Updated description"
        assert updated_record_set.version_comment == "Updated version comment"
        assert updated_record_set.version_number >= original_record_set.version_number

    def test_store_validation_errors(self) -> None:
        # GIVEN a RecordSet without required fields
        record_set = RecordSet()

        # WHEN I try to store it without proper configuration
        # THEN it should raise a ValueError for missing required fields
        with pytest.raises(ValueError):
            record_set.store(synapse_client=self.syn)


class TestRecordSetGet:
    """Tests for the RecordSet.get method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def stored_record_set(self, project_model: Project) -> RecordSet:
        """Create and store a RecordSet for testing get operations."""
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            description="Test RecordSet for get operations",
            parent_id=project_model.id,
            upsert_keys=["id", "name"],
            version_comment="Initial version",
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)
        return record_set

    def test_get_record_set_by_id(self, stored_record_set: RecordSet) -> None:
        # GIVEN an existing RecordSet
        original_id = stored_record_set.id

        # WHEN I get the RecordSet by ID
        retrieved_record_set = RecordSet(id=original_id).get(synapse_client=self.syn)

        # THEN the retrieved RecordSet should match the original
        assert retrieved_record_set.id == original_id
        assert retrieved_record_set.name == stored_record_set.name
        assert retrieved_record_set.description == stored_record_set.description
        assert retrieved_record_set.parent_id == stored_record_set.parent_id
        assert retrieved_record_set.upsert_keys == stored_record_set.upsert_keys
        assert retrieved_record_set.version_comment == stored_record_set.version_comment
        assert retrieved_record_set.etag == stored_record_set.etag
        assert retrieved_record_set.version_number == stored_record_set.version_number

    def test_get_record_set_with_activity(self, project_model: Project) -> None:
        # GIVEN a RecordSet with activity
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        activity = Activity(
            name="Test Activity",
            description="Test activity for RecordSet",
            used=[UsedURL(name="Test URL", url="https://example.com")],
        )

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            parent_id=project_model.id,
            upsert_keys=["id"],
            activity=activity,
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)

        # WHEN I get the RecordSet with activity
        retrieved_record_set = RecordSet(id=record_set.id).get(
            include_activity=True, synapse_client=self.syn
        )

        # THEN the RecordSet should include the activity
        assert retrieved_record_set.activity is not None
        assert retrieved_record_set.activity.name == "Test Activity"
        assert (
            retrieved_record_set.activity.description == "Test activity for RecordSet"
        )
        assert len(retrieved_record_set.activity.used) == 1
        assert retrieved_record_set.path is not None

    def test_get_validation_error(self) -> None:
        # GIVEN a RecordSet without an ID
        record_set = RecordSet()

        # WHEN I try to get it
        # THEN it should raise a ValueError
        with pytest.raises(ValueError):
            record_set.get(synapse_client=self.syn)

    def test_get_non_existent_record_set(self) -> None:
        # GIVEN a non-existent RecordSet ID
        record_set = RecordSet(id="syn999999999")

        # WHEN I try to get it
        # THEN it should raise a SynapseHTTPError
        with pytest.raises(SynapseHTTPError):
            record_set.get(synapse_client=self.syn)


class TestRecordSetDelete:
    """Tests for the RecordSet.delete method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_delete_entire_record_set(self, project_model: Project) -> None:
        # GIVEN an existing RecordSet
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            description="RecordSet to be deleted",
            parent_id=project_model.id,
            upsert_keys=["id"],
        ).store(synapse_client=self.syn)

        record_set_id = record_set.id

        # WHEN I delete the entire RecordSet
        record_set.delete(synapse_client=self.syn)

        # THEN the RecordSet should be deleted and no longer accessible
        with pytest.raises(SynapseHTTPError):
            RecordSet(id=record_set_id).get(synapse_client=self.syn)

    def test_delete_specific_version(self, project_model: Project) -> None:
        # GIVEN an existing RecordSet with multiple versions
        filename1 = utils.make_bogus_uuid_file()
        filename2 = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename1)
        self.schedule_for_cleanup(filename2)

        # Create initial version
        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename1,
            description="RecordSet version 1",
            parent_id=project_model.id,
            upsert_keys=["id"],
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)

        # Create second version
        record_set.path = filename2
        record_set.description = "RecordSet version 2"
        v2_record_set = record_set.store(synapse_client=self.syn)

        # WHEN I delete only version 2
        v2_record_set.delete(version_only=True, synapse_client=self.syn)

        # THEN the RecordSet should still exist but version 2 should be gone
        current_record_set = RecordSet(id=record_set.id).get(synapse_client=self.syn)
        assert current_record_set.id == record_set.id
        assert current_record_set.version_number == 1  # Should be back to version 1
        assert current_record_set.description == "RecordSet version 1"

    def test_delete_validation_errors(self) -> None:
        # GIVEN a RecordSet without an ID
        record_set = RecordSet()

        # WHEN I try to delete it
        # THEN it should raise a ValueError
        with pytest.raises(ValueError):
            record_set.delete(synapse_client=self.syn)

        # AND WHEN I try to delete a specific version without version number
        record_set_with_id = RecordSet(id="syn123456")
        with pytest.raises(ValueError):
            record_set_with_id.delete(version_only=True, synapse_client=self.syn)


class TestRecordSetGetDetailedValidationResults:
    """Tests for the RecordSet.get_detailed_validation_results method."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def create_test_schema(
        self, syn: Synapse
    ) -> Generator[Tuple[JsonSchemaOrganization, str, list], None, None]:
        """Create a test JSON schema for RecordSet validation."""
        org_name = "recordsettest" + uuid.uuid4().hex[:6]
        schema_name = "recordset.validation.schema"

        js = syn.service("json_schema")
        created_org = js.create_organization(org_name)
        record_set_ids = []  # Track RecordSets that need schema unbinding

        try:
            # Define a schema with comprehensive validation rules to test different error types:
            # 1. Required fields (id, name)
            # 2. Type constraints (integer, string, number, boolean)
            # 3. String constraints (minLength)
            # 4. Numeric constraints (minimum, maximum)
            # 5. Enum constraints (category must be A, B, C, or D)
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"https://example.com/schema/{schema_name}.json",
                "title": "RecordSet Validation Schema",
                "type": "object",
                "properties": {
                    "id": {
                        "description": "The unique identifier",
                        "type": "integer",
                    },
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
                    "active": {
                        "description": "Active status flag",
                        "type": "boolean",
                    },
                },
                "required": ["id", "name"],
            }

            test_org = js.JsonSchemaOrganization(org_name)
            created_schema = test_org.create_json_schema(schema, schema_name, "0.0.1")
            yield test_org, created_schema.uri, record_set_ids
        finally:
            # Unbind schema from any RecordSets before deleting
            for record_set_id in record_set_ids:
                try:
                    record_set = RecordSet(id=record_set_id)
                    record_set.unbind_schema(synapse_client=syn)
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

    @pytest.fixture(scope="function")
    def record_set_with_validation_fixture(
        self,
        project_model: Project,
        create_test_schema: Tuple[JsonSchemaOrganization, str, list],
    ) -> RecordSet:
        """Create and store a RecordSet with schema bound, then export via Grid to generate validation results."""
        from tests.integration import ASYNC_JOB_TIMEOUT_SEC

        _, schema_uri, record_set_ids = create_test_schema

        # Create test data with multiple types of validation errors:
        # Row 1: VALID - all fields correct
        # Row 2: VALID - all fields correct
        # Row 3: INVALID - missing required 'name' field (None value)
        # Row 4: INVALID - multiple violations:
        #        - name too short ("AB" < minLength of 3)
        #        - value exceeds maximum (1500 > 1000)
        #        - category not in enum ("X" not in [A, B, C, D])
        # Row 5: INVALID - value below minimum (-50 < 0)
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": [
                    "Alpha",
                    "Beta",
                    None,
                    "AB",
                    "Epsilon",
                ],  # Row 3: None, Row 4: too short
                "value": [
                    10.5,
                    150.3,
                    30.7,
                    1500.0,
                    -50.0,
                ],  # Row 4: too high, Row 5: negative
                "category": ["A", "B", "A", "X", "B"],  # Row 4: invalid enum value
                "active": [True, False, True, True, False],
            }
        )

        # Create a temporary CSV file
        temp_fd, filename = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(temp_fd)  # Close the file descriptor
            test_data.to_csv(filename, index=False)
            self.schedule_for_cleanup(filename)

            # Create and store the RecordSet
            record_set = RecordSet(
                path=filename,
                name=str(uuid.uuid4()),
                description="Test RecordSet for validation testing",
                version_comment="Validation test version",
                version_label=str(uuid.uuid4()),
                upsert_keys=["id", "name"],
            )

            stored_record_set = record_set.store(
                parent=project_model, synapse_client=self.syn
            )
            self.schedule_for_cleanup(stored_record_set.id)
            record_set_ids.append(stored_record_set.id)  # Track for schema cleanup

            time.sleep(10)

            # Bind the JSON schema to the RecordSet
            stored_record_set.bind_schema(
                json_schema_uri=schema_uri,
                enable_derived_annotations=False,
                synapse_client=self.syn,
            )

            time.sleep(10)

            # Verify the schema is bound by getting the schema from the entity
            stored_record_set.get_schema(synapse_client=self.syn)

            # Create a Grid session from the RecordSet
            grid = Grid(record_set_id=stored_record_set.id)
            created_grid = grid.create(
                timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
            )

            time.sleep(10)

            # Export the Grid back to RecordSet to generate validation results
            exported_grid = created_grid.export_to_record_set(
                timeout=ASYNC_JOB_TIMEOUT_SEC, synapse_client=self.syn
            )

            # Clean up the Grid session
            exported_grid.delete(synapse_client=self.syn)

            # Re-fetch the RecordSet to get the updated validation_file_handle_id
            updated_record_set = RecordSet(id=stored_record_set.id).get(
                synapse_client=self.syn
            )

            return updated_record_set
        except Exception:
            # Clean up the temp file if something goes wrong
            if os.path.exists(filename):
                os.unlink(filename)
            raise

    def test_get_validation_results_no_file_handle_id(
        self, project_model: Project
    ) -> None:
        # GIVEN a RecordSet without a validation_file_handle_id
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)

        record_set = RecordSet(
            name=str(uuid.uuid4()),
            path=filename,
            description="RecordSet without validation",
            parent_id=project_model.id,
            upsert_keys=["id"],
        ).store(synapse_client=self.syn)
        self.schedule_for_cleanup(record_set.id)

        # WHEN I try to get detailed validation results
        result = record_set.get_detailed_validation_results(synapse_client=self.syn)

        # THEN it should return None and log a warning
        assert result is None
        assert record_set.validation_file_handle_id is None

    def test_get_validation_results_with_default_location(
        self, record_set_with_validation_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with validation results
        record_set = record_set_with_validation_fixture

        # WHEN I get detailed validation results without specifying a location
        results_df = record_set.get_detailed_validation_results(synapse_client=self.syn)

        # THEN it should return a pandas DataFrame
        assert results_df is not None
        assert isinstance(results_df, pd.DataFrame)
        # The validation results file should be downloaded to the cache
        assert record_set.validation_file_handle_id is not None

        # AND the DataFrame should contain expected columns for validation results
        expected_columns = [
            "row_index",
            "is_valid",
            "validation_error_message",
            "all_validation_messages",
        ]
        for col in expected_columns:
            assert (
                col in results_df.columns
            ), f"Expected column '{col}' not found in validation results"

        # AND there should be 5 rows (one per data row)
        assert (
            len(results_df) == 5
        ), f"Expected 5 rows in validation results, got {len(results_df)}"

        # Debug: Print actual validation results to diagnose the issue
        print("\n=== Debug: Validation Results ===")
        print(f"DataFrame shape: {results_df.shape}")
        print(f"DataFrame dtypes:\n{results_df.dtypes}")
        print("\nValidation results:")
        print(results_df.to_string())
        print(f"\nis_valid column unique values: {results_df['is_valid'].unique()}")
        print(f"is_valid column dtype: {results_df['is_valid'].dtype}")
        print("=== End Debug ===")

        # AND rows 0 and 1 should be valid (is_valid == True)
        assert (
            results_df.loc[0, "is_valid"] == True
        ), "Row 0 should be valid"  # noqa: E712
        assert (
            results_df.loc[1, "is_valid"] == True
        ), "Row 1 should be valid"  # noqa: E712
        assert pd.isna(
            results_df.loc[0, "validation_error_message"]
        ), "Row 0 should have no error message"
        assert pd.isna(
            results_df.loc[1, "validation_error_message"]
        ), "Row 1 should have no error message"

        # AND row 2 should be invalid (missing required 'name' field)
        assert (
            results_df.loc[2, "is_valid"] == False
        ), "Row 2 should be invalid (missing required name)"  # noqa: E712
        assert (
            "expected type: String, found: Null"
            in results_df.loc[2, "validation_error_message"]
        ), f"Row 2 should have null type error, got: {results_df.loc[2, 'validation_error_message']}"
        assert "#/name: expected type: String, found: Null" in str(
            results_df.loc[2, "all_validation_messages"]
        ), f"Row 2 all_validation_messages incorrect: {results_df.loc[2, 'all_validation_messages']}"

        # AND row 3 should be invalid (multiple violations: minLength, maximum, enum)
        assert (
            results_df.loc[3, "is_valid"] == False
        ), "Row 3 should be invalid (multiple violations)"  # noqa: E712
        assert (
            "3 schema violations found" in results_df.loc[3, "validation_error_message"]
        ), f"Row 3 should have 3 violations, got: {results_df.loc[3, 'validation_error_message']}"
        all_msgs_3 = str(results_df.loc[3, "all_validation_messages"])
        assert (
            "#/name: expected minLength: 3, actual: 2" in all_msgs_3
        ), f"Row 3 should have minLength violation: {all_msgs_3}"
        assert (
            "#/value: 1500 is not less or equal to 1000" in all_msgs_3
            or "1500" in all_msgs_3
        ), f"Row 3 should have maximum violation: {all_msgs_3}"
        assert (
            "#/category: X is not a valid enum value" in all_msgs_3
            or "enum" in all_msgs_3.lower()
        ), f"Row 3 should have enum violation: {all_msgs_3}"

        # AND row 4 should be invalid (value below minimum)
        assert (
            results_df.loc[4, "is_valid"] == False
        ), "Row 4 should be invalid (value below minimum)"  # noqa: E712
        assert (
            "-50 is not greater or equal to 0"
            in results_df.loc[4, "validation_error_message"]
        ), f"Row 4 should have minimum violation, got: {results_df.loc[4, 'validation_error_message']}"
        assert "#/value: -50 is not greater or equal to 0" in str(
            results_df.loc[4, "all_validation_messages"]
        ), f"Row 4 all_validation_messages incorrect: {results_df.loc[4, 'all_validation_messages']}"

    def test_get_validation_results_with_custom_location(
        self, record_set_with_validation_fixture: RecordSet
    ) -> None:
        # GIVEN a RecordSet with validation results
        record_set = record_set_with_validation_fixture

        # AND a custom download location
        custom_location = tempfile.mkdtemp()
        self.schedule_for_cleanup(custom_location)

        # WHEN I get detailed validation results with a custom location
        results_df = record_set.get_detailed_validation_results(
            download_location=custom_location, synapse_client=self.syn
        )

        # THEN it should return a pandas DataFrame
        assert results_df is not None
        assert isinstance(results_df, pd.DataFrame)

        # AND the file should be downloaded to the custom location
        expected_filename = (
            f"SYNAPSE_RECORDSET_VALIDATION_{record_set.validation_file_handle_id}.csv"
        )
        expected_path = os.path.join(custom_location, expected_filename)
        assert os.path.exists(expected_path)

        # AND the DataFrame should contain validation result columns
        assert "row_index" in results_df.columns
        assert "is_valid" in results_df.columns
        assert "validation_error_message" in results_df.columns
        assert "all_validation_messages" in results_df.columns

        # Expected behavior: 3 invalid rows (rows 2, 3, 4) and 2 valid rows (rows 0, 1)
        invalid_rows = results_df[results_df["is_valid"] == False]  # noqa: E712
        assert (
            len(invalid_rows) == 3
        ), f"Expected 3 invalid rows, got {len(invalid_rows)}"

        valid_rows = results_df[results_df["is_valid"] == True]  # noqa: E712
        assert len(valid_rows) == 2, f"Expected 2 valid rows, got {len(valid_rows)}"

        # All invalid rows should have validation error messages
        for idx, row in invalid_rows.iterrows():
            assert pd.notna(
                row["validation_error_message"]
            ), f"Row {idx} is marked invalid but has no validation_error_message"
            assert pd.notna(
                row["all_validation_messages"]
            ), f"Row {idx} is marked invalid but has no all_validation_messages"

    def test_get_validation_results_validation_error(self) -> None:
        # GIVEN a RecordSet without an ID
        record_set = RecordSet()

        # Note: The method doesn't have explicit validation for missing ID,
        # but it will fail when trying to download without a valid entity
        # This test documents the expected behavior
        # The method requires validation_file_handle_id to be set to work properly
        result = record_set.get_detailed_validation_results(synapse_client=self.syn)

        # THEN it should return None since there's no validation_file_handle_id
        assert result is None
