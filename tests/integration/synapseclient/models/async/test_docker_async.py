"""Integration tests for the synapseclient.models.DockerRepository class (async)."""

import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.models import DockerRepository, Project


class TestDockerRepositoryAsync:
    """Async integration tests for DockerRepository."""

    @pytest.fixture(scope="class")
    async def readonly_docker_repo(
        self,
        schedule_for_cleanup: Callable[..., None],
        syn: Synapse,
    ) -> DockerRepository:
        """Class-scoped fixture for read-only tests. Do not modify or delete."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)

        docker_repo = DockerRepository(
            parent_id=project.id, repository_name="username/test-async-readonly"
        )
        await docker_repo.store_async(synapse_client=syn)
        schedule_for_cleanup(docker_repo.id)
        return docker_repo

    @pytest.fixture(scope="function")
    async def mutable_docker_repo(
        self,
        schedule_for_cleanup: Callable[..., None],
        syn: Synapse,
    ) -> DockerRepository:
        """Function-scoped fixture for tests that modify or delete the repo."""
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)

        docker_repo = DockerRepository(
            parent_id=project.id, repository_name="username/test-async-mutable"
        )
        await docker_repo.store_async(synapse_client=syn)
        schedule_for_cleanup(docker_repo.id)
        return docker_repo

    async def test_get_docker_repo(
        self, readonly_docker_repo: DockerRepository, syn: Synapse
    ) -> None:
        """Test retrieving a Docker repository by ID (async)."""
        # GIVEN an existing Docker repository

        # WHEN we retrieve it by ID
        retrieved_docker_repo = await DockerRepository(
            id=readonly_docker_repo.id,
        ).get_async(synapse_client=syn)

        # THEN the retrieved DockerRepository should match the created one
        assert retrieved_docker_repo.id == readonly_docker_repo.id
        assert (
            retrieved_docker_repo.repository_name
            == readonly_docker_repo.repository_name
        )
        assert retrieved_docker_repo.parent_id == readonly_docker_repo.parent_id

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

    async def test_get_docker_repo_missing_id_raises_error(self, syn: Synapse) -> None:
        """Test that get_async() raises ValueError when neither id nor repository_name is set."""
        docker_repo = DockerRepository()

        with pytest.raises(
            ValueError, match="must have either an id or repository_name"
        ):
            await docker_repo.get_async(synapse_client=syn)

    async def test_get_docker_repo_with_optional_fields(
        self, schedule_for_cleanup: Callable[..., None], syn: Synapse
    ) -> None:
        """Test retrieving a Docker repository with all optional fields set (async)."""
        # GIVEN a project and DockerRepository with all fields
        project = await Project(name=f"test_project_{uuid.uuid4()}").store_async(
            synapse_client=syn
        )
        schedule_for_cleanup(project.id)

        docker_repo = await DockerRepository(
            parent_id=project.id,
            repository_name="username/test-async-optional",
            name="My Test Repo Async",
            description="A test repository with all fields (async)",
        ).store_async(synapse_client=syn)
        schedule_for_cleanup(docker_repo.id)

        # WHEN we retrieve it
        retrieved = await DockerRepository(id=docker_repo.id).get_async(
            synapse_client=syn
        )

        # THEN optional fields should be preserved
        assert retrieved.name == "My Test Repo Async"
        assert retrieved.description == "A test repository with all fields (async)"

    async def test_update_docker_repo_description(
        self, mutable_docker_repo: DockerRepository, syn: Synapse
    ) -> None:
        """Test updating the description of a Docker repository (async)."""
        # GIVEN an existing Docker repository
        original_description = mutable_docker_repo.description

        # WHEN we update the description
        new_description = "Updated description for testing (async)"
        mutable_docker_repo.description = new_description
        updated_repo = await mutable_docker_repo.store_async(synapse_client=syn)

        # THEN the description should be updated, and other fields should remain unchanged
        assert updated_repo.description == new_description
        assert updated_repo.id == mutable_docker_repo.id
        assert updated_repo.repository_name == mutable_docker_repo.repository_name
        assert updated_repo.parent_id == mutable_docker_repo.parent_id
        # Note: etag, modified_on may change on update
        assert updated_repo.created_on == mutable_docker_repo.created_on
        assert updated_repo.created_by == mutable_docker_repo.created_by

    async def test_create_docker_repo_without_parent_raises_error(
        self, syn: Synapse
    ) -> None:
        """Test that creating a Docker repo without parent_id raises error (async)."""
        docker_repo = DockerRepository(repository_name="username/test-async-no-parent")

        with pytest.raises(ValueError, match="parent_id"):
            await docker_repo.store_async(synapse_client=syn)

    async def test_delete_docker_repo(
        self, mutable_docker_repo: DockerRepository, syn: Synapse
    ) -> None:
        """Test deleting a Docker repository (async)."""
        # GIVEN an existing Docker repository
        repo_id = mutable_docker_repo.id

        # WHEN we delete it
        await mutable_docker_repo.delete_async(synapse_client=syn)

        # THEN it should no longer be retrievable
        with pytest.raises(
            Exception, match=f"404 Client Error: Entity {repo_id} is in trash can"
        ):
            await DockerRepository(id=repo_id).get_async(synapse_client=syn)
