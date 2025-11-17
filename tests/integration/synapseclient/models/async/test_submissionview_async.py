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
    query_part_mask_async,
)
from tests.integration import ASYNC_JOB_TIMEOUT_SEC, QUERY_TIMEOUT_SEC


class TestSubmissionViewCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_submissionview_with_columns(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with
        # AND an evaluation to use in the scope
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # Test Case 1: Submissionview with default columns
        # GIVEN a submissionview with default columns
        submissionview_name = str(uuid.uuid4())
        submissionview_description = "Test submissionview"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description=submissionview_description,
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # THEN the submissionview should be created
        assert submissionview.id is not None

        # AND I can retrieve that submissionview from Synapse
        new_submissionview_instance = await SubmissionView(
            id=submissionview.id
        ).get_async(synapse_client=self.syn)
        assert new_submissionview_instance is not None
        assert new_submissionview_instance.name == submissionview_name
        assert new_submissionview_instance.id == submissionview.id
        assert new_submissionview_instance.description == submissionview_description

        # AND the submissionview has columns
        assert len(new_submissionview_instance.columns) > 0

        # Test Case 2: Submissionview with a single custom column
        # GIVEN a submissionview with a single column
        submissionview_name2 = str(uuid.uuid4())
        submissionview_description2 = "Test submissionview with single column"
        custom_column = "test_column"
        submissionview2 = SubmissionView(
            name=submissionview_name2,
            parent_id=project_model.id,
            description=submissionview_description2,
            columns=[Column(name=custom_column, column_type=ColumnType.STRING)],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview2 = await submissionview2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview2)

        # THEN the submissionview should be created
        assert submissionview2.id is not None

        # AND I can retrieve that submissionview from Synapse with the custom column
        new_submissionview_instance2 = await SubmissionView(
            id=submissionview2.id
        ).get_async(synapse_client=self.syn, include_columns=True)
        assert new_submissionview_instance2 is not None
        assert custom_column in new_submissionview_instance2.columns
        assert new_submissionview_instance2.columns[custom_column].name == custom_column
        assert (
            new_submissionview_instance2.columns[custom_column].column_type
            == ColumnType.STRING
        )

        # Test Case 3: Submissionview with multiple custom columns
        # GIVEN a submissionview with multiple columns
        submissionview_name3 = str(uuid.uuid4())
        submissionview_description3 = "Test submissionview with multiple columns"
        custom_column1 = "test_column1"
        custom_column2 = "test_column2"
        submissionview3 = SubmissionView(
            name=submissionview_name3,
            parent_id=project_model.id,
            description=submissionview_description3,
            columns=[
                Column(name=custom_column1, column_type=ColumnType.STRING),
                Column(name=custom_column2, column_type=ColumnType.INTEGER),
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview3 = await submissionview3.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview3)

        # THEN I can retrieve that submissionview with both columns
        new_submissionview_instance3 = await SubmissionView(
            id=submissionview3.id
        ).get_async(synapse_client=self.syn, include_columns=True)
        assert custom_column1 in new_submissionview_instance3.columns
        assert custom_column2 in new_submissionview_instance3.columns
        assert (
            new_submissionview_instance3.columns[custom_column1].column_type
            == ColumnType.STRING
        )
        assert (
            new_submissionview_instance3.columns[custom_column2].column_type
            == ColumnType.INTEGER
        )

    async def test_create_submissionview_special_cases(
        self, project_model: Project
    ) -> None:
        # GIVEN a project to work with
        # AND an evaluation to use in the scope
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # Test Case 1: Creating a submissionview with an invalid column
        # GIVEN a submissionview with an invalid column
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            description="Test submissionview with invalid column",
            columns=[
                Column(
                    name="test_column",
                    column_type=ColumnType.STRING,
                    maximum_size=999999999,  # Too large
                )
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I try to store the submissionview
        # THEN it should fail with a specific error
        with pytest.raises(SynapseHTTPError) as e:
            await submissionview.store_async(synapse_client=self.syn)
        assert (
            "400 Client Error: ColumnModel.maxSize for a STRING cannot exceed:"
            in str(e.value)
        )

        # Test Case 2: Creating a submissionview with empty scope
        # GIVEN a submissionview with no scope
        submissionview_name2 = str(uuid.uuid4())
        submissionview2 = SubmissionView(
            name=submissionview_name2,
            parent_id=project_model.id,
            description="Test submissionview with empty scope",
            scope_ids=[],
        )

        # WHEN I store the submissionview
        submissionview2 = await submissionview2.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview2)

        # THEN the submissionview should be created but with empty scope
        retrieved_view = await SubmissionView(id=submissionview2.id).get_async(
            synapse_client=self.syn
        )
        assert len(retrieved_view.scope_ids) == 0

        # Test Case 3: Creating a submissionview without default columns
        # GIVEN a submissionview with custom columns but no default columns
        submissionview_name3 = str(uuid.uuid4())
        custom_column1 = "custom_column1"
        custom_column2 = "custom_column2"
        submissionview3 = SubmissionView(
            name=submissionview_name3,
            parent_id=project_model.id,
            description="Test submissionview without default columns",
            include_default_columns=False,
            columns=[
                Column(name=custom_column1, column_type=ColumnType.STRING),
                Column(name=custom_column2, column_type=ColumnType.INTEGER),
            ],
            scope_ids=[evaluation.id],
        )

        # WHEN I store the submissionview
        submissionview3 = await submissionview3.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview3)

        # THEN the submissionview should only contain our custom columns
        retrieved_view3 = await SubmissionView(id=submissionview3.id).get_async(
            synapse_client=self.syn, include_columns=True
        )
        assert len(retrieved_view3.columns) == 2
        assert custom_column1 in retrieved_view3.columns
        assert custom_column2 in retrieved_view3.columns
        assert "id" not in retrieved_view3.columns
        assert "name" not in retrieved_view3.columns
        assert "createdOn" not in retrieved_view3.columns


class TestColumnAndScopeModifications:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_column_modifications(self, project_model: Project) -> None:
        # GIVEN a project to work with
        # AND an evaluation to use in the scope
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # AND a submissionview in Synapse with two columns
        submissionview_name = str(uuid.uuid4())
        old_column_name = "column_string"
        column_to_keep = "column_to_keep"
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            columns=[
                Column(name=old_column_name, column_type=ColumnType.STRING),
                Column(name=column_to_keep, column_type=ColumnType.STRING),
            ],
            scope_ids=[evaluation.id],
        )
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # Test Case 1: Rename column
        # WHEN I rename the column
        new_column_name = "new_column_string"
        submissionview.columns[old_column_name].name = new_column_name

        # AND I store the submissionview
        await submissionview.store_async(synapse_client=self.syn)

        # THEN the column name should be updated on the existing submissionview instance
        assert submissionview.columns[new_column_name] is not None
        assert old_column_name not in submissionview.columns

        # AND the new column name should be reflected in the Synapse submissionview
        updated_view = await SubmissionView(id=submissionview.id).get_async(
            synapse_client=self.syn
        )
        assert new_column_name in updated_view.columns
        assert old_column_name not in updated_view.columns

        # Test Case 2: Delete column
        # WHEN I delete the renamed column
        submissionview.delete_column(name=new_column_name)

        # AND I store the submissionview
        await submissionview.store_async(synapse_client=self.syn)

        # THEN the column should be removed from the submissionview instance
        assert new_column_name not in submissionview.columns
        assert column_to_keep in submissionview.columns

        # AND the column should be removed from the Synapse submissionview
        updated_view2 = await SubmissionView(id=submissionview.id).get_async(
            synapse_client=self.syn
        )
        assert new_column_name not in updated_view2.columns
        assert column_to_keep in updated_view2.columns

    async def test_scope_modifications(self, project_model: Project) -> None:
        # GIVEN a project to work with
        # AND two evaluations for testing scope changes
        evaluation1 = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation 1",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation1)

        evaluation2 = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation 2",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation2)

        # AND a submissionview with one evaluation in scope
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation1.id],
        )
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # Test Case 1: Update scope to include multiple evaluations
        # WHEN I update the scope to include both evaluations
        submissionview.scope_ids = [evaluation1.id, evaluation2.id]
        updated_submissionview = await submissionview.store_async(
            synapse_client=self.syn
        )

        # THEN the submissionview should have both evaluations in its scope
        assert len(updated_submissionview.scope_ids) == 2
        assert evaluation1.id in updated_submissionview.scope_ids
        assert evaluation2.id in updated_submissionview.scope_ids

        # AND when I retrieve the submissionview from Synapse it should have both evaluations
        retrieved_submissionview = await SubmissionView(id=submissionview.id).get_async(
            synapse_client=self.syn
        )
        assert len(retrieved_submissionview.scope_ids) == 2
        assert evaluation1.id in retrieved_submissionview.scope_ids
        assert evaluation2.id in retrieved_submissionview.scope_ids

        # Test Case 2: Clear scope completely
        # WHEN I clear the scope
        submissionview.scope_ids = []
        cleared_submissionview = await submissionview.store_async(
            synapse_client=self.syn
        )

        # THEN the submissionview should have an empty scope
        assert len(cleared_submissionview.scope_ids) == 0

        # AND when I retrieve the submissionview from Synapse it should have empty scope
        retrieved_submissionview2 = await SubmissionView(
            id=submissionview.id
        ).get_async(synapse_client=self.syn)
        assert len(retrieved_submissionview2.scope_ids) == 0


class TestQuerying:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_query_submissionview(self, project_model: Project) -> None:
        # GIVEN a project to work with
        # AND an evaluation to use in the scope
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # AND a submissionview with the evaluation in scope
        submissionview_name = str(uuid.uuid4())
        submissionview = SubmissionView(
            name=submissionview_name,
            parent_id=project_model.id,
            scope_ids=[evaluation.id],
        )
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # Test Case 1: Simple query
        # WHEN I query the submissionview with a standard query
        results = await submissionview.query_async(
            f"SELECT * FROM {submissionview.id}",
            synapse_client=self.syn,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN results should be returned (even if empty)
        assert results is not None
        assert isinstance(results, pd.DataFrame)

        # Test Case 2: Query with part mask
        # WHEN I query the submissionview with a part mask
        query_results = 0x1
        query_count = 0x2
        last_updated_on = 0x80
        part_mask = query_results | query_count | last_updated_on

        mask_results = await query_part_mask_async(
            query=f"SELECT * FROM {submissionview.id}",
            synapse_client=self.syn,
            part_mask=part_mask,
            timeout=QUERY_TIMEOUT_SEC,
        )

        # THEN the part mask should be reflected in the results
        assert mask_results is not None
        assert mask_results.result is not None
        assert mask_results.count is not None
        assert mask_results.last_updated_on is not None


class TestSnapshotting:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_submissionview_snapshots(self, project_model: Project) -> None:
        # GIVEN a project to work with
        # AND an evaluation to use in the scope
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # Test Case 1: Snapshot with Activity
        # GIVEN a submissionview with activity
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
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # WHEN I snapshot the submissionview
        snapshot = await submissionview.snapshot_async(
            comment="My snapshot",
            label="My snapshot label",
            include_activity=True,
            associate_activity_to_new_version=True,
            timeout=ASYNC_JOB_TIMEOUT_SEC,
            synapse_client=self.syn,
        )

        # THEN the view should be snapshotted
        assert snapshot.results is not None

        # AND getting the first version should return the snapshot instance
        snapshot_instance = await SubmissionView(
            id=submissionview.id, version_number=1
        ).get_async(synapse_client=self.syn, include_activity=True)
        assert snapshot_instance is not None
        assert snapshot_instance.version_number == 1
        assert snapshot_instance.id == submissionview.id
        assert snapshot_instance.version_comment == "My snapshot"
        assert snapshot_instance.version_label == "My snapshot label"
        assert snapshot_instance.activity.name == "Activity for snapshot"
        assert snapshot_instance.activity.used[0].name == "Synapse"

        # AND The activity should be associated with the new version
        newest_instance = await SubmissionView(id=submissionview.id).get_async(
            synapse_client=self.syn, include_activity=True
        )
        assert newest_instance.version_number == 2
        assert newest_instance.activity is not None
        assert newest_instance.activity.name == "Activity for snapshot"

        # Test Case 2: Snapshot with no scope
        # GIVEN a submissionview with no scope
        empty_view_name = str(uuid.uuid4())
        empty_view = SubmissionView(
            name=empty_view_name,
            parent_id=project_model.id,
            description="Test submissionview with no scope",
        )

        # AND the submissionview is stored in Synapse
        empty_view = await empty_view.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(empty_view.id)

        # WHEN I try to snapshot the submissionview
        # THEN it should fail with a specific error
        with pytest.raises(SynapseHTTPError) as e:
            await empty_view.snapshot_async(
                comment="My snapshot",
                label="My snapshot label",
                timeout=ASYNC_JOB_TIMEOUT_SEC,
                synapse_client=self.syn,
            )
        assert (
            "400 Client Error: You cannot create a version of a view that has no scope."
            in str(e.value)
        )


class TestSubmissionViewWithSubmissions:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_submission_lifecycle(self, project_model: Project) -> None:
        """Test submission lifecycle in a submission view: adding and removing submissions."""
        # GIVEN an evaluation
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for submission view",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # AND a submissionview that includes the evaluation
        submissionview = SubmissionView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test submissionview for submissions",
            scope_ids=[evaluation.id],
        )
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # AND a file for submission
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            filename = f.name
            f.write("Test content for submission")
            self.schedule_for_cleanup(filename)

        file_entity = File(path=filename, parent_id=project_model.id, name="Test file")
        file_entity = await file_entity.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(file_entity.id)

        # WHEN I submit the file to the evaluation
        submission = await self.syn.submit_async(
            evaluation,
            file_entity.id,
            name="Test submission",
            submitterAlias="Test submitter",
        )

        # THEN eventually the submission should appear in the submission view
        max_attempts = 10
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            results = await submissionview.query_async(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
                timeout=QUERY_TIMEOUT_SEC,
            )

            if len(results) == 1:
                success = True
                break

            await asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5

        assert success, "Submission did not appear in the view"
        assert len(results) == 1
        assert results["name"].iloc[0] == "Test submission"

        # WHEN I delete the submission
        self.syn.restDELETE(f"/evaluation/submission/{submission.id}")

        # THEN eventually the submission should be removed from the view
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            results = await submissionview.query_async(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
                timeout=QUERY_TIMEOUT_SEC,
            )

            if len(results) == 0:
                success = True
                break

            await asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5

        assert success, "Deleted submission still appears in the view"

    async def test_multiple_submissions(self, project_model: Project) -> None:
        """Test that multiple submissions to an evaluation appear in a submission view."""
        # GIVEN an evaluation
        evaluation = await self.syn.store_async(
            Evaluation(
                name=str(uuid.uuid4()),
                description="Test evaluation for multiple submissions",
                contentSource=project_model.id,
            )
        )
        self.schedule_for_cleanup(evaluation)

        # AND a submissionview that includes the evaluation
        submissionview = SubmissionView(
            name=str(uuid.uuid4()),
            parent_id=project_model.id,
            description="Test submissionview for multiple submissions",
            scope_ids=[evaluation.id],
        )
        submissionview = await submissionview.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(submissionview)

        # WHEN I create and upload multiple test files for submission
        files = []
        submissions = []

        for i in range(3):
            # Create test file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False
            ) as f:
                filename = f.name
                f.write(f"Test content for submission {i}")
                self.schedule_for_cleanup(filename)

            # Store file in Synapse
            file_entity = File(
                path=filename, parent_id=project_model.id, name=f"Test file {i}"
            )
            file_entity = await file_entity.store_async(synapse_client=self.syn)
            self.schedule_for_cleanup(file_entity.id)
            files.append(file_entity)

            # Submit to evaluation
            submission = await self.syn.submit_async(
                evaluation,
                file_entity.id,
                name=f"Submission {i}",
                submitterAlias=f"Test submitter {i}",
            )
            submissions.append(submission)

        # THEN eventually all submissions should appear in the submission view
        max_attempts = 10
        wait_seconds = 3
        success = False

        for attempt in range(max_attempts):
            results = await submissionview.query_async(
                f"SELECT * FROM {submissionview.id}",
                synapse_client=self.syn,
                timeout=QUERY_TIMEOUT_SEC,
            )

            if len(results) == len(submissions):
                success = True
                break

            await asyncio.sleep(wait_seconds)
            wait_seconds *= 1.5

        assert (
            success
        ), f"Submissions did not appear in the view after {max_attempts} attempts"
        assert len(results) == len(submissions)

        # Verify each submission is present
        for i in range(len(submissions)):
            submission_name = f"Submission {i}"
            matching_rows = results[results["name"] == submission_name]
            assert (
                len(matching_rows) > 0
            ), f"Expected submission {submission_name} not found"
