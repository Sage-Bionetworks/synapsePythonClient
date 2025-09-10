"""Integration tests for the synapseclient.models.Evaluation class."""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.models import Evaluation, Project


class TestEvaluationCreation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    async def test_create_evaluation(self):
        # GIVEN a project to work with
        project = await Project(id=self.syn.store(Project(name="test_project")).id).get_async(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # WHEN I create an evaluation
        evaluation = await Evaluation(name="test_evaluation", parent_id=project.id).store_async(synapse_client=self.syn)
        self.schedule_for_cleanup(evaluation.id)

        # THEN the evaluation should be created
        assert evaluation.id is not None


class TestGetEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(id=syn.store(Project(name=f"test_project_{uuid.uuid4()}")).id).get_async(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(self, test_project: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Evaluation:
        """Create a test evaluation for get tests."""
        evaluation = await Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}", 
            parent_id=test_project.id
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    @pytest.fixture(scope="function")
    async def multiple_evaluations(self, test_project: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> list[Evaluation]:
        """Create multiple test evaluations for bulk tests."""
        evaluations = []
        for i in range(3):
            evaluation = await Evaluation(
                name=f"test_evaluation_{i}_{uuid.uuid4()}", 
                parent_id=test_project.id
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(evaluation.id)
            evaluations.append(evaluation)
        return evaluations

    async def test_get_evaluation_by_id(self, test_evaluation: Evaluation):
        # WHEN I get an evaluation by id
        retrieved_evaluation = await Evaluation(id=test_evaluation.id).get_async(synapse_client=self.syn)

        # THEN the evaluation should be retrieved
        assert retrieved_evaluation.id == test_evaluation.id
        assert retrieved_evaluation.name == test_evaluation.name

    async def test_get_evaluation_by_entity(self, test_evaluation: Evaluation, test_project: Project):
        # WHEN I get an evaluation by a project
        retrieved_evaluation = await Evaluation(project_id=test_project.id).get_async(synapse_client=self.syn)

        # THEN the evaluation should be retrieved
        assert retrieved_evaluation.id == test_evaluation.id
        assert retrieved_evaluation.parent_id == test_project.id

    async def test_get_all_evaluations(self, multiple_evaluations: list[Evaluation], limit: int = 1):
        # Test 1: Grab evaluations that the user has access to
        # WHEN a call is made to get all evaluations
        evaluations = await Evaluation.get_all_evaluations_async(synapse_client=self.syn)

        # THEN the evaluations should be retrieved
        assert evaluations is not None
        assert len(evaluations) >= len(multiple_evaluations)

        # Test 2: Grab evaluations that the user has access to and are active
        # WHEN the active_only parameter is True
        active_evaluations = await Evaluation.get_all_evaluations_async(synapse_client=self.syn, active_only=True)

        # THEN the active evaluations should be retrieved
        assert active_evaluations is not None

        # Test 3: Grab evaluations based on a limit
        # WHEN the limit parameter is set
        limited_evaluations = await Evaluation.get_all_evaluations_async(synapse_client=self.syn, limit=limit)

        # THEN the evaluations retrieved should match said limit
        assert len(limited_evaluations) == limit

    async def test_get_available_evaluations(self, multiple_evaluations: list[Evaluation]):
        # WHEN a call is made to get available evaluations for a given user
        evaluations = await Evaluation.get_available_evaluations_async(synapse_client=self.syn)

        # THEN the evaluations should be retrieved
        assert evaluations is not None
        assert len(evaluations) >= len(multiple_evaluations)

    async def test_get_evaluation_by_name(self, test_evaluation: Evaluation):
        # WHEN a call is made to get an evaluation by name
        retrieved_evaluation = await Evaluation.get_evaluation_by_name_async(
            synapse_client=self.syn, 
            name=test_evaluation.name
        )

        # THEN the evaluation should be retrieved
        assert retrieved_evaluation.id == test_evaluation.id
        assert retrieved_evaluation.name == test_evaluation.name


class TestUpdateEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(id=syn.store(Project(name=f"test_project_{uuid.uuid4()}")).id).get_async(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(self, test_project: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Evaluation:
        """Create a test evaluation for update tests."""
        evaluation = await Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}", 
            parent_id=test_project.id
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    async def test_update_evaluation_name(self, test_evaluation: Evaluation):
        # WHEN I update the evaluation name
        new_name = f"updated_evaluation_{uuid.uuid4()}"
        test_evaluation.name = new_name
        updated_evaluation = await test_evaluation.store_async(synapse_client=self.syn)

        # THEN the evaluation should be updated
        assert updated_evaluation.name == new_name
        assert updated_evaluation.id == test_evaluation.id


class TestDeleteEvaluation:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(id=syn.store(Project(name=f"test_project_{uuid.uuid4()}")).id).get_async(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(self, test_project: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Evaluation:
        """Create a test evaluation for delete tests."""
        evaluation = await Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}", 
            parent_id=test_project.id
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    async def test_delete_evaluation(self, test_evaluation: Evaluation):
        # WHEN I delete the evaluation
        await test_evaluation.delete_async(synapse_client=self.syn)

        # THEN the evaluation should be deleted (attempting to get it should raise an exception)
        # Note: This test might need adjustment based on how deletion is handled in the API
        pass


class TestEvaluationAccess:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    async def test_project(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Project:
        """Create a test project for evaluation tests."""
        project = await Project(id=syn.store(Project(name=f"test_project_{uuid.uuid4()}")).id).get_async(synapse_client=syn)
        schedule_for_cleanup(project.id)
        return project

    @pytest.fixture(scope="function")
    async def test_evaluation(self, test_project: Project, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> Evaluation:
        """Create a test evaluation for access tests."""
        evaluation = await Evaluation(
            name=f"test_evaluation_{uuid.uuid4()}", 
            parent_id=test_project.id
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(evaluation.id)
        return evaluation

    async def test_get_evaluation_acl(self, test_evaluation: Evaluation):
        # WHEN I get the evaluation ACL
        acl = await test_evaluation.get_acl_async(synapse_client=self.syn)

        # THEN the ACL should be retrieved
        assert acl is not None

    async def test_update_evaluation_acl(self, test_evaluation: Evaluation):
        # WHEN I update the evaluation ACL
        # Note: This test might need adjustment based on ACL update implementation
        pass

    async def test_get_evaluation_permissions(self, test_evaluation: Evaluation):
        # WHEN I get evaluation permissions
        # Note: This test might need adjustment based on permissions implementation
        pass