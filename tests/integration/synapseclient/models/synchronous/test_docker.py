import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.models import DockerRepository, Project


class TestDockerRepository:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    @pytest.fixture(scope="function")
    def test_docker_repo(
        self, schedule_for_cleanup: Callable[..., None]
    ) -> DockerRepository:
        """Create a test docker repository for testing."""
        # GIVEN a project to work with
        project = Project(name=f"test_project_{uuid.uuid4()}").store(
            synapse_client=self.syn
        )
        self.schedule_for_cleanup(project.id)

        # Create a DockerRepository entity
        docker_repo = DockerRepository(
            parent_id=project.id, repository_name="username/test"
        )
        schedule_for_cleanup(project.id)
        docker_repo.store(synapse_client=self.syn)
        return docker_repo

    def test_get_docker_repo(self, test_docker_repo: DockerRepository) -> None:
        # GIVEN a project to work with
        retrieved_docker_repo = DockerRepository(
            id=test_docker_repo.id,
        ).get(synapse_client=self.syn)

        # THEN the retrieved DockerRepository should match the created one
        assert retrieved_docker_repo.id == test_docker_repo.id
        assert retrieved_docker_repo.repository_name == test_docker_repo.repository_name
        assert retrieved_docker_repo.parent_id == test_docker_repo.parent_id

        # Metadata fields (set by Synapse on creation)
        assert retrieved_docker_repo.etag is not None
        assert retrieved_docker_repo.created_on is not None
        assert retrieved_docker_repo.modified_on is not None
        assert retrieved_docker_repo.created_by is not None
        assert retrieved_docker_repo.modified_by is not None

        # Repository type
        assert retrieved_docker_repo.is_managed == False  # External repo
        assert (
            retrieved_docker_repo.concrete_type
            == "org.sagebionetworks.repo.model.docker.DockerRepository"
        )

    def test_get_docker_repo_missing_id_raises_error(self) -> None:
        """Test that get() raises ValueError when neither id nor repository_name is set."""
        docker_repo = DockerRepository()

        with pytest.raises(
            ValueError, match="must have either an id or repository_name"
        ):
            docker_repo.get(synapse_client=self.syn)

    def test_get_docker_repo_with_optional_fields(
        self, schedule_for_cleanup: Callable[..., None]
    ) -> None:
        """Test retrieving a Docker repository with all optional fields set."""
        # GIVEN a project and DockerRepository with all fields
        project = Project(name=f"test_project_{uuid.uuid4()}").store(
            synapse_client=self.syn
        )
        schedule_for_cleanup(project.id)

        docker_repo = DockerRepository(
            parent_id=project.id,
            repository_name="username/test",
            name="My Test Repo",
            description="A test repository with all fields",
        ).store(synapse_client=self.syn)

        # WHEN we retrieve it
        retrieved = DockerRepository(id=docker_repo.id).get(synapse_client=self.syn)

        # THEN optional fields should be preserved
        assert retrieved.name == "My Test Repo"
        assert retrieved.description == "A test repository with all fields"

    def test_update_docker_repo_description(
        self, test_docker_repo: DockerRepository
    ) -> None:
        """Test updating the description of a Docker repository."""
        # GIVEN an existing Docker repository
        original_description = test_docker_repo.description

        # WHEN we update the description
        new_description = "Updated description for testing"
        test_docker_repo.description = new_description
        updated_repo = test_docker_repo.store(synapse_client=self.syn)

        # THEN the description should be updated, and other fields should remain unchanged
        assert updated_repo.description == new_description
        assert updated_repo.id == test_docker_repo.id
        assert updated_repo.repository_name == test_docker_repo.repository_name
        assert updated_repo.parent_id == test_docker_repo.parent_id
        assert updated_repo.etag == test_docker_repo.etag
        assert updated_repo.created_on == test_docker_repo.created_on
        assert updated_repo.modified_on == test_docker_repo.modified_on
        assert updated_repo.created_by == test_docker_repo.created_by
        assert updated_repo.modified_by == test_docker_repo.modified_by

    def test_create_docker_repo_without_parent_raises_error(self) -> None:
        """Test that creating a Docker repo without parent_id raises error."""
        docker_repo = DockerRepository(repository_name="username/test")

        with pytest.raises(ValueError, match="parent_id"):
            docker_repo.store(synapse_client=self.syn)

    def test_delete_docker_repo(self, test_docker_repo: DockerRepository) -> None:
        """Test deleting a Docker repository."""
        # GIVEN an existing Docker repository
        repo_id = test_docker_repo.id

        # WHEN we delete it
        test_docker_repo.delete(synapse_client=self.syn)

        # THEN it should no longer be retrievable
        with pytest.raises(
            Exception, match=f"404 Client Error: Entity {repo_id} is in trash can"
        ):
            DockerRepository(id=repo_id).get(synapse_client=self.syn)
