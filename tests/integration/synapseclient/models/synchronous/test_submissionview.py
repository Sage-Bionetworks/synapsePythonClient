import asyncio
import tempfile
import uuid
from typing import Callable

import pandas as pd
import pytest

from synapseclient import Evaluation, Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    File,
    Project,
    SubmissionView,
    UsedURL,
    query_part_mask,
)


class TestSubmissionViewCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_submissionview_with_default_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with default columns
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # THEN the submissionview should be created
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn
        )
        assert new_submissionview_instance is not None
        assert new_submissionview_instance.name == submissionview_name
        assert new_submissionview_instance.id == submissionview.id
        assert new_submissionview_instance.description == submissionview_description

        # AND the submissionview has columns
        assert len(new_submissionview_instance.columns) > 0

    async def test_create_submissionview_with_single_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with a single column
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            columns=[Column(name="test_column", column_type=ColumnType.STRING)],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # THEN the submissionview should be created
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_submissionview_instance is not None
        assert new_submissionview_instance.name == submissionview_name
        assert new_submissionview_instance.id == submissionview.id
        assert new_submissionview_instance.description == submissionview_description
        assert "test_column" in new_submissionview_instance.columns
        assert new_submissionview_instance.columns["test_column"].name == "test_column"
        assert (
            new_submissionview_instance.columns["test_column"].column_type
            == ColumnType.STRING
        )

    async def test_create_submissionview_with_multiple_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with multiple columns
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            columns=[
                Column(name="test_column", column_type=ColumnType.STRING),
                Column(name="test_column2", column_type=ColumnType.INTEGER),
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # THEN the submissionview should be created
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_submissionview_instance is not None
        assert new_submissionview_instance.name == submissionview_name
        assert new_submissionview_instance.id == submissionview.id
        assert new_submissionview_instance.description == submissionview_description
        assert "test_column" in new_submissionview_instance.columns
        assert new_submissionview_instance.columns["test_column"].name == "test_column"
        assert (
            new_submissionview_instance.columns["test_column"].column_type
            == ColumnType.STRING
        )
        assert "test_column2" in new_submissionview_instance.columns
        assert (
            new_submissionview_instance.columns["test_column2"].name == "test_column2"
        )
        assert (
            new_submissionview_instance.columns["test_column2"].column_type
            == ColumnType.INTEGER
        )

    async def test_create_submissionview_with_invalid_column(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with an invalid column
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,
                )
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        with pytest.raises(SynapseHTTPError) as e:
            submissionview.store(synapse_client=self.syn)

        # THEN the submissionview should not be created
        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

    async def test_create_submissionview_with_empty_scope(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND a submissionview with no scope
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            scope_ids=[],
        )

        # WHEN I store the submissionview
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # THEN the submissionview should be created but with empty scope
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn
        )
        assert new_submissionview_instance is not None
        assert len(new_submissionview_instance.scope_ids) == 0

    async def test_create_submissionview_without_default_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with custom columns but no default columns
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview without default columns"
        custom_column1 = "custom_column1"
        custom_column2 = "custom_column2"

        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            include_default_columns=False,
            columns=[
                Column(name=custom_column1, column_type=ColumnType.STRING),
                Column(name=custom_column2, column_type=ColumnType.INTEGER),
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # THEN the submissionview should be created
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn, include_columns=True
        )
        assert new_submissionview_instance is not None
        assert new_submissionview_instance.name == submissionview_name
        assert new_submissionview_instance.id == submissionview.id
        assert new_submissionview_instance.description == submissionview_description

        # AND the submissionview should only contain our custom columns
        assert len(new_submissionview_instance.columns) == 2
        assert custom_column1 in new_submissionview_instance.columns
        assert custom_column2 in new_submissionview_instance.columns

        # AND default columns like "id" should not be present
        assert "id" not in new_submissionview_instance.columns
        assert "name" not in new_submissionview_instance.columns
        assert "createdOn" not in new_submissionview_instance.columns


class TestColumnModifications:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_column_rename(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview in Synapse
        submissionview_name = str(uuid.uuid4())
        old_column_name = "column_string"
        old_submissionview_instance = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            columns=[Column(name=old_column_name, column_type=ColumnType.STRING)],
            scope_ids=[evaluation.id],
        )
        old_submissionview_instance = old_submissionview_instance.store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_submissionview_instance.id)

        # WHEN I rename the column
        new_column_name = "new_column_string"
        old_submissionview_instance.columns[old_column_name].name = new_column_name

        # AND I store the submissionview
        old_submissionview_instance.store(synapse_client=self.syn)

        # THEN the column name should be updated on the existing submissionview instance
        assert old_submissionview_instance.columns[new_column_name] is not None
        assert old_column_name not in old_submissionview_instance.columns

        # AND the new column name should be reflected in the Synapse submissionview
        new_submissionview_instance = SubmissionView(
            id=old_submissionview_instance.id
        ).get(synapse_client=self.syn)
        assert new_submissionview_instance.columns[new_column_name] is not None
        assert old_column_name not in new_submissionview_instance.columns

    async def test_delete_column(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview in Synapse
        submissionview_name = str(uuid.uuid4())
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"
        old_submissionview_instance = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
            scope_ids=[evaluation.id],
        )
        old_submissionview_instance = old_submissionview_instance.store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(old_submissionview_instance.id)

        # WHEN I delete the column
        old_submissionview_instance.delete_column(name=old_column_name)

        # AND I store the submissionview
        old_submissionview_instance.store(synapse_client=self.syn)

        # THEN the column should be removed from the submissionview instance
        assert old_column_name not in old_submissionview_instance.columns

        # AND the column to keep should still be in the submissionview instance
        assert column_to_keep in old_submissionview_instance.columns

        # AND the column should be removed from the Synapse submissionview
        new_submissionview_instance = SubmissionView(
            id=old_submissionview_instance.id
        ).get(synapse_client=self.syn)
        assert old_column_name not in new_submissionview_instance.columns

        # AND the column to keep should still be in the Synapse submissionview
        assert column_to_keep in new_submissionview_instance.columns


class TestQuerying:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_query_submission_view(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # WHEN I query the submissionview
        results = submissionview.query(
            f"SELECT * FROM {submissionview.id}",
            synapse_client=self.syn,
        )

        # THEN results should be returned (even if empty)
        assert results is not None
        assert isinstance(results, pd.DataFrame)

    async def test_part_mask_query(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # WHEN I query the submissionview with a part mask
        query_results = 0x1
        query_count = 0x2
        last_updated_on = 0x80
        part_mask = query_results | query_count | last_updated_on

        results = query_part_mask(
            query=f"SELECT * FROM {submissionview.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
        )

        # THEN the part mask should be reflected in the results
        assert results is not None
        assert results.result is not None
        assert results.count is not None
        assert results.last_updated_on is not None


class TestSubmissionViewSnapshot:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_snapshot_with_activity(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with activity
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            scope_ids=[evaluation.id],
            activity=Activity(
                name="Activity for snapshot",
                used=[UsedURL(name="Synapse", url="https://synapse.org")],
            ),
        )

        # AND the submissionview is stored in Synapse
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)
        assert submissionview.id is not None

        # WHEN I snapshot the submissionview
        snapshot = submissionview.snapshot(
            comment="My snapshot",
            label="My snapshot label",
            include_activity=True,
            associate_activity_to_new_version=True,
            synapse_client=self.syn,
        )

        # THEN the view should be snapshotted
        assert snapshot.results is not None

        # AND getting the first version of the submissionview should return the snapshot instance
        snapshot_instance = SubmissionView(id=submissionview.id, version_number=1).get(
            synapse_client=self.syn, include_activity=True
        )
        assert snapshot_instance is not None
        assert snapshot_instance.version_number == 1
        assert snapshot_instance.id == submissionview.id
        assert snapshot_instance.name == submissionview_name
        assert snapshot_instance.description == submissionview_description
        assert snapshot_instance.version_comment == "My snapshot"
        assert snapshot_instance.version_label == "My snapshot label"
        assert snapshot_instance.activity.name == "Activity for snapshot"
        assert snapshot_instance.activity.used[0].name == "Synapse"
        assert snapshot_instance.activity.used[0].url == "https://synapse.org"

        # AND The activity should be associated with the new version
        newest_instance = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn, include_activity=True
        )
        assert newest_instance.version_number == 2
        assert newest_instance.activity is not None
        assert newest_instance.activity.name == "Activity for snapshot"

    async def test_snapshot_with_no_scope(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND a submissionview with no scope
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
        )

        # AND the submissionview is stored in Synapse
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)
        assert submissionview.id is not None

        # WHEN I snapshot the submissionview
        with pytest.raises(SynapseHTTPError) as e:
            submissionview.snapshot(
                comment="My snapshot",
                label="My snapshot label",
                synapse_client=self.syn,
            )

        # THEN the submissionview should not be snapshot
        assert (
            "400 Client Error: You cannot create a version of a view that has no scope."
            in str(e.value)
        )


class TestUpdateScope:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_update_scope(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND two evaluations
        evaluation1 = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation 1",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation1.id)

        evaluation2 = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation 2",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation2.id)

        # AND a submissionview with one evaluation in scope
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation1.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # WHEN I update the scope to include both evaluations
        submissionview.scope_ids = [evaluation1.id, evaluation2.id]
        updated_submissionview = submissionview.store(synapse_client=self.syn)

        # THEN the submissionview should have both evaluations in its scope
        assert len(updated_submissionview.scope_ids) == 2
        assert evaluation1.id in updated_submissionview.scope_ids
        assert evaluation2.id in updated_submissionview.scope_ids

        # AND when I retrieve the submissionview from Synapse
        retrieved_submissionview = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn
        )

        # THEN it should have both evaluations in its scope
        assert len(retrieved_submissionview.scope_ids) == 2
        assert evaluation1.id in retrieved_submissionview.scope_ids
        assert evaluation2.id in retrieved_submissionview.scope_ids

    async def test_clear_scope(self, project_model: Project) -> None:
        # GIVEN a project to work with

        # AND an evaluation to use in the scope
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview with the evaluation in scope
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # WHEN I clear the scope
        submissionview.scope_ids = []
        updated_submissionview = submissionview.store(synapse_client=self.syn)

        # THEN the submissionview should have an empty scope
        assert len(updated_submissionview.scope_ids) == 0

        # AND when I retrieve the submissionview from Synapse
        retrieved_submissionview = SubmissionView(id=submissionview.id).get(
            synapse_client=self.syn
        )

        # THEN it should have an empty scope
        assert len(retrieved_submissionview.scope_ids) == 0


class TestSubmissionViewWithSubmissions:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_submission_view_with_submissions(
        self, project_model: Project
    ) -> None:
        """Test that submissions to an evaluation appear in a submission view."""
        # GIVEN an evaluation
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview that includes the evaluation
        submissionview = SubmissionView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test submissionview with submissions",
            scope_ids=[evaluation.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # WHEN I create and upload test files for submission
        files = []
        for i in range(3):
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False
            ) as f:
                filename = f.name
                f.write(f"Test content for submission {i}")

                self.schedule_for_cleanup(filename)

            file_entity = File(
                path=filename, parent_id=project_model.id, name=f"Test file {i}"
            )
            file_entity = file_entity.store(synapse_client=self.syn)
            self.schedule_for_cleanup(file_entity.id)
            files.append(file_entity)

        # AND submit these files to the evaluation
        submissions = []
        for i, file_entity in enumerate(files):
            submission = self.syn.submit(
                evaluation,
                file_entity.id,  # Use the file ID for submission
                name=f"Submission {i}",
                submitterAlias=f"Test submitter {i}",
            )
            submissions.append(submission)

        # THEN eventually the submissions should appear in the submission view
        # (retry because of eventual consistency)
        max_attempts = 10
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            # Query the view to see if submissions are available
            results = submissionview.query(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
            )

            if len(results) == len(submissions):
                success = True
                break

            # Wait before retrying
            asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5  # Exponential backoff

        assert (
            success
        ), f"Submissions did not appear in the view after {max_attempts} attempts"

        # Verify that we have the expected number of submissions
        assert len(results) == len(submissions)

        # Verify that each submission name exists in the results
        for i in range(len(submissions)):
            submission_name = f"Submission {i}"
            matching_rows = results[results["name"] == submission_name]
            assert (
                len(matching_rows) > 0
            ), f"Expected submission with name {submission_name} not found"

    async def test_submission_removal_from_evaluation(
        self, project_model: Project
    ) -> None:
        """Test that when a submission is deleted, it is removed from the submission view."""
        # GIVEN an evaluation
        evaluation = self.syn.store(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission removal",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation.id)

        # AND a submissionview that includes the evaluation
        submissionview = SubmissionView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test submissionview for deletion",
            scope_ids=[evaluation.id],
        )
        submissionview = submissionview.store(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview.id)

        # AND a file is submitted to the evaluation
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            filename = f.name
            f.write("Test content for submission that will be deleted")
            self.schedule_for_cleanup(filename)

        file_entity = File(
            path=filename, parent_id=project_model.id, name="Test file for deletion"
        )
        file_entity = file_entity.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file_entity.id)

        # Submit the file to the evaluation
        submission = self.syn.submit(
            evaluation,
            file_entity.id,  # Use the file ID for submission
            name="Submission to be deleted",
            submitterAlias="Test submitter",
        )

        # Wait for the submission to appear in the view (handle eventual consistency)
        max_attempts = 10
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            results = submissionview.query(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
            )

            if len(results) == 1:
                success = True
                break

            asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5

        assert success, "Submission did not appear in the view"

        # WHEN I delete the submission
        self.syn.restDELETE(f"/evaluation/submission/{submission.id}")

        # THEN eventually the submission should be removed from the view
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            results = submissionview.query(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
            )

            if len(results) == 0:
                success = True
                break

            asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5

        assert success, "Deleted submission still appears in the view"
