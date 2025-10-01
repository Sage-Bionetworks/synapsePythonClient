"""Integration tests for the synapseclient.models.Evaluation class."""

import logging
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import Evaluation, Project


class TestEvaluationCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_evaluation(self):
        # GIVEN a project to work with
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(project.id)

        # WHEN I create an evaluation using the dataclass method
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for testing purposes",
            content_source=project.id,
            submission_instructions_message="Please submit your results in CSV format",
            submission_receipt_message="Thank you for your submission!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(created_evaluation.id)

        # THEN the evaluation should be created
        assert created_evaluation.id is not None
        assert created_evaluation.etag is not None  # Check that etag is set
        assert created_evaluation.name == evaluation.name
        assert (
            created_evaluation.description == "A test evaluation for testing purposes"
        )
        assert created_evaluation.content_source == project.id
        assert created_evaluation.owner_id is not None  # Check that owner_id is set
        assert created_evaluation.created_on is not None  # Check that created_on is set


class TestGetEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=self.syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for get tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for get tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    @pytest.fixture(scope="function")
    async def multiple_evaluations(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> list[Evaluation]:
        """Create multiple test evaluations for bulk tests."""
        evaluations = []
        for i in range(3):
            evaluation = Evaluation(
                name=f"test_evaluation_{i}_{uuid.uuid4()}",
                description=f"Test evaluation {i}",
                content_source=test_project.id,
                submission_instructions_message="Please submit your results",
                submission_receipt_message="Thank you!",
            )
            created_evaluation = await evaluation.store_async(synapse_client=syn)
            schedule_for_cleanup(created_evaluation.id)
            evaluations.append(created_evaluation)
        return evaluations

    async def test_get_evaluation_by_id(
        self, test_evaluation: Evaluation, test_project: Project
    ):
        # WHEN I get an evaluation by id using the dataclass method
        retrieved_evaluation = await Evaluation(id=test_evaluation.id).get_async(
            synapse_client=self.syn
        )

        # THEN the evaluation should be retrieved
        assert retrieved_evaluation.id == test_evaluation.id
        assert retrieved_evaluation.etag is not None  # Check that etag is set
        assert retrieved_evaluation.name == test_evaluation.name
        assert retrieved_evaluation.description == test_evaluation.description
        assert retrieved_evaluation.content_source == test_project.id
        assert retrieved_evaluation.owner_id is not None  # Check that owner_id is set
        assert (
            retrieved_evaluation.created_on is not None
        )  # Check that created_on is set

    async def test_get_evaluation_by_name(
        self, test_evaluation: Evaluation, test_project: Project
    ):
        # WHEN I get an evaluation by name using the dataclass method
        retrieved_evaluation = await Evaluation(name=test_evaluation.name).get_async(
            synapse_client=self.syn
        )

        # THEN the evaluation should be retrieved
        assert retrieved_evaluation.id == test_evaluation.id
        assert retrieved_evaluation.etag is not None  # Check that etag is set
        assert retrieved_evaluation.name == test_evaluation.name
        assert retrieved_evaluation.description == test_evaluation.description
        assert retrieved_evaluation.content_source == test_project.id
        assert retrieved_evaluation.owner_id is not None  # Check that owner_id is set
        assert (
            retrieved_evaluation.created_on is not None
        )  # Check that created_on is set

    async def test_get_all_evaluations(
        self, multiple_evaluations: list[Evaluation], limit: int = 1
    ):
        # Test 1: Grab evaluations that the user has access to
        # WHEN a call is made to get all evaluations
        evaluations = await Evaluation.get_all_evaluations_async(
            synapse_client=self.syn
        )

        # THEN the evaluations should be retrieved
        assert evaluations is not None
        assert len(evaluations) >= len(multiple_evaluations)

        # Test 2: Grab evaluations that the user has access to and are active
        # WHEN the active_only parameter is True
        active_evaluations = await Evaluation.get_all_evaluations_async(
            synapse_client=self.syn, active_only=True
        )

        # THEN the active evaluations should be retrieved
        assert active_evaluations is not None

        # Test 3: Grab evaluations based on a limit
        # WHEN the limit parameter is set
        limited_evaluations = await Evaluation.get_all_evaluations_async(
            synapse_client=self.syn, limit=limit
        )

        # THEN the evaluations retrieved should match said limit
        assert len(limited_evaluations) == limit

    async def test_get_available_evaluations(
        self, multiple_evaluations: list[Evaluation]
    ):
        # WHEN a call is made to get available evaluations for a given user
        evaluations = await Evaluation.get_available_evaluations_async(
            synapse_client=self.syn
        )

        # THEN the evaluations should be retrieved
        assert evaluations is not None
        assert len(evaluations) >= len(multiple_evaluations)

    async def test_get_evaluations_by_project(
        self, test_project: Project, multiple_evaluations: list[Evaluation]
    ):
        # WHEN a call is made to get evaluations by project
        evaluations = await Evaluation.get_evaluations_by_project_async(
            project_id=test_project.id, synapse_client=self.syn
        )

        # THEN the evaluations should be retrieved
        assert evaluations is not None
        assert len(evaluations) >= len(multiple_evaluations)

        # AND all returned evaluations belong to the test project
        for evaluation in evaluations:
            assert evaluation.content_source == test_project.id


class TestStoreEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=self.syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for update tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for update tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    async def test_update_evaluation_name(self, test_evaluation: Evaluation):
        # WHEN I update the evaluation name in my evaluation object
        new_name = f"updated_evaluation_{uuid.uuid4()}"
        test_evaluation.name = new_name

        updated_evaluation = await test_evaluation.store_async(synapse_client=self.syn)

        # THEN the evaluation should be updated
        assert updated_evaluation.name == new_name
        assert updated_evaluation.id == test_evaluation.id
        assert updated_evaluation.description == test_evaluation.description
        assert updated_evaluation.etag == test_evaluation.etag

    async def test_update_evaluation_description(self, test_evaluation: Evaluation):
        # WHEN I update the evaluation description
        new_description = f"Updated description {uuid.uuid4()}"
        old_etag = test_evaluation.etag
        test_evaluation.description = new_description

        updated_evaluation = await test_evaluation.store_async(synapse_client=self.syn)

        # THEN the evaluation should be updated
        assert updated_evaluation.description == new_description
        assert updated_evaluation.id == test_evaluation.id
        assert updated_evaluation.name == test_evaluation.name

        # AND the etag is updated after an update operation
        assert updated_evaluation.etag is not None
        assert updated_evaluation.etag != old_etag

    async def test_update_multiple_fields(self, test_evaluation: Evaluation):
        # WHEN I update multiple fields at once
        new_name = f"multi_update_{uuid.uuid4()}"
        new_description = f"Multi-updated description {uuid.uuid4()}"
        new_instructions = "Updated submission instructions"
        old_etag = test_evaluation.etag

        test_evaluation.name = new_name
        test_evaluation.description = new_description
        test_evaluation.submission_instructions_message = new_instructions

        updated_evaluation = await test_evaluation.store_async(
            synapse_client=self.syn,
        )

        # THEN all fields should be updated
        assert updated_evaluation.name == new_name
        assert updated_evaluation.description == new_description
        assert updated_evaluation.submission_instructions_message == new_instructions
        assert updated_evaluation.id == test_evaluation.id

        # AND the etag is updated after an update operation
        assert updated_evaluation.etag is not None
        assert updated_evaluation.etag != old_etag

    async def test_store_unchanged_evaluation(
        self, test_evaluation: Evaluation, monkeypatch
    ):
        warning_messages = []

        def mock_warning(self, msg, *args, **kwargs):
            """
            Using the method interception pattern to mock the implementation of logging.Logger.warning
            to create a mock logger that captures warning messages.

            When the Evaluation.store_async method detects no changes, it logs a warning
            using logger.warning(). This mock replaces the actual logging function to
            capture those warning messages in our warning_messages list instead of sending
            them to the logging system. This allows us to assert that the expected warning
            was generated without depending on the logging configuration.
            """
            warning_messages.append(msg)

        monkeypatch.setattr(logging.Logger, "warning", mock_warning)

        # GIVEN an evaluation that has not been changed
        retrieved_evaluation = await Evaluation(id=test_evaluation.id).get_async(
            synapse_client=self.syn
        )

        # WHEN trying to store the unchanged evaluation
        result = await retrieved_evaluation.store_async(synapse_client=self.syn)

        # THEN it should not be updated and return the same instance
        assert result is retrieved_evaluation

        # AND a warning should be logged indicating no changes were detected
        warning_text = "has not changed since last 'store' or 'get' event"

        # Check if any captured warning contains our expected text
        assert any(
            warning_text in msg for msg in warning_messages
        ), f"Warning message not found in captured warnings: {warning_messages}"


class TestDeleteEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=self.syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for delete tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for delete tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    async def test_delete_evaluation(self, test_evaluation: Evaluation):
        # WHEN I delete the evaluation using the dataclass method
        await test_evaluation.delete_async(synapse_client=self.syn)

        # THEN the evaluation should be deleted (attempting to get it should raise an exception)
        with pytest.raises(SynapseHTTPError):
            await Evaluation(id=test_evaluation.id).get_async(synapse_client=self.syn)


class TestEvaluationAccess:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=self.syn
        )
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(
        self,
        test_project: Project,
        syn: Synapse,
        schedule_for_cleanup: Callable[..., None],
    ) -> Evaluation:
        """Create a test evaluation for access tests."""
        evaluation = Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}",
            description="A test evaluation for access tests",
            content_source=test_project.id,
            submission_instructions_message="Please submit your results",
            submission_receipt_message="Thank you!",
        )
        created_evaluation = await evaluation.store_async(synapse_client=syn)
        schedule_for_cleanup(created_evaluation.id)
        return created_evaluation

    async def test_get_evaluation_acl(self, test_evaluation: Evaluation):
        # GIVEN the current user's ID
        user_profile = self.syn.getUserProfile()
        current_user_id = int(user_profile.get("ownerId"))

        # WHEN we get the evaluation ACL using the dataclass method
        acl = await test_evaluation.get_acl_async(synapse_client=self.syn)

        # THEN the ACL should be retrieved
        assert acl is not None
        assert "id" in acl
        assert "resourceAccess" in acl

        # AND the ACL ID matches the evaluation ID
        assert acl["id"] == test_evaluation.id

        # AND one of the principalIds in the resourceAccess key matches the ID of the user currently accessing Synapse
        assert "resourceAccess" in acl and len(acl["resourceAccess"]) > 0
        principal_ids = [
            int(access.get("principalId")) for access in acl["resourceAccess"]
        ]
        assert (
            current_user_id in principal_ids
        ), f"Current user {current_user_id} not found in resourceAccess principal IDs: {principal_ids}"

    async def test_get_evaluation_permissions(self, test_evaluation: Evaluation):
        # WHEN I get evaluation permissions using the dataclass method
        permissions = await test_evaluation.get_permissions_async(
            synapse_client=self.syn
        )

        # THEN the permissions should be retrieved
        assert permissions is not None

    async def test_update_acl_async_with_principal_id(
        self, test_evaluation: Evaluation
    ):
        """Test updating ACL for an evaluation using principal_id and access_type."""
        # GIVEN the current user's ID
        user_profile = self.syn.getUserProfile()
        current_user_id = int(user_profile.get("ownerId"))

        # WHEN we update the ACL for the current user with specific permissions
        updated_acl = await test_evaluation.update_acl_async(
            principal_id=current_user_id,
            access_type=["READ", "UPDATE", "DELETE", "CHANGE_PERMISSIONS"],
            synapse_client=self.syn,
        )

        # THEN the ACL should be updated
        assert updated_acl is not None
        assert "resourceAccess" in updated_acl

        # AND the user's permissions should be updated
        user_access = None
        for access in updated_acl["resourceAccess"]:
            if int(access.get("principalId")) == current_user_id:
                user_access = access
                break

        assert (
            user_access is not None
        ), f"User {current_user_id} not found in updated ACL"
        assert set(user_access["accessType"]) == set(
            ["READ", "UPDATE", "DELETE", "CHANGE_PERMISSIONS"]
        )

    async def test_update_acl_async_with_full_dictionary(
        self, test_evaluation: Evaluation
    ):
        """Test updating ACL for an evaluation using a complete ACL dictionary."""
        # GIVEN the current ACL
        current_acl = await test_evaluation.get_acl_async(synapse_client=self.syn)

        # AND a modified version of the ACL with a changed permission set
        modified_acl = current_acl.copy()
        user_profile = self.syn.getUserProfile()
        current_user_id = int(user_profile.get("ownerId"))

        # Find the current user in the ACL and update permissions
        for access in modified_acl["resourceAccess"]:
            if int(access.get("principalId")) == current_user_id:
                access["accessType"] = ["READ", "DELETE", "SUBMIT"]
                break

        # WHEN we update the ACL with the complete dictionary
        updated_acl = await test_evaluation.update_acl_async(
            acl=modified_acl, synapse_client=self.syn
        )

        # THEN the ACL should be updated
        assert updated_acl is not None

        # AND the user's permissions should be updated
        user_access = None
        for access in updated_acl["resourceAccess"]:
            if int(access.get("principalId")) == current_user_id:
                user_access = access
                break

        assert (
            user_access is not None
        ), f"User {current_user_id} not found in updated ACL"
        assert set(user_access["accessType"]) == set(["READ", "DELETE", "SUBMIT"])


class TestEvaluationValidation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_evaluation_missing_required_fields(self):
        # WHEN I try to create an evaluation with missing required fields
        evaluation = Evaluation(name="test_evaluation")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="missing the 'description' attribute"):
            await evaluation.store_async(synapse_client=self.syn)

    async def test_get_evaluation_missing_id_and_name(self):
        # WHEN I try to get an evaluation without id or name
        evaluation = Evaluation()

        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="Either id or name must be set to get an evaluation"
        ):
            await evaluation.get_async(synapse_client=self.syn)

    async def test_delete_evaluation_missing_id(self):
        # WHEN I try to delete an evaluation without an id
        evaluation = Evaluation(name="test_evaluation")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="id must be set to delete an evaluation"):
            await evaluation.delete_async(synapse_client=self.syn)

    async def test_get_acl_missing_id(self):
        # WHEN I try to get ACL for an evaluation without an id
        evaluation = Evaluation(name="test_evaluation")

        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="id must be set to get evaluation ACL"):
            await evaluation.get_acl_async(synapse_client=self.syn)

    async def test_get_permissions_missing_id(self):
        # WHEN I try to get permissions for an evaluation without an id
        evaluation = Evaluation(name="test_evaluation")

        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError, match="id must be set to get evaluation permissions"
        ):
            await evaluation.get_permissions_async(synapse_client=self.syn)
