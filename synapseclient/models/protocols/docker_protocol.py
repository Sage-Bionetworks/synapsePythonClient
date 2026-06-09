"""Protocol defining the synchronous interface for DockerRepository operations."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import DockerRepository


class DockerRepositorySynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for DockerRepository operations."""

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "DockerRepository":
        """Get the Docker repository metadata from Synapse.

        You can retrieve a Docker repository by either:

        - `id`: The Synapse ID of the Docker repository (e.g., "syn123"). This works
          for both managed and external Docker repositories.
        - `repository_name`: The name of a **managed** Docker repository
          (e.g., "docker.synapse.org/syn123/my-repo"). This lookup method **only works
          for managed repositories** hosted on Synapse's Docker registry. External
          (unmanaged) Docker repositories must be retrieved using their Synapse `id`.

        If both are provided, `id` takes precedence.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The DockerRepository object.

        Raises:
            ValueError: If neither id nor repository_name is set.
            SynapseHTTPError: If retrieving by repository_name and the repository
                is not found or is not a managed repository.

        Example: Using this function
            Retrieve a Docker repository by Synapse ID (works for both managed and
            external repositories):

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            syn = Synapse()
            syn.login()

            docker_repo = DockerRepository(id="syn123").get()
            print(docker_repo.repository_name)
            ```

            Retrieve a managed Docker repository by repository name (only works for
            managed repositories hosted at docker.synapse.org):

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            syn = Synapse()
            syn.login()

            docker_repo = DockerRepository(
                repository_name="docker.synapse.org/syn123/my-repo"
            ).get()
            print(docker_repo.id)
            ```
        """
        return self

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "DockerRepository":
        """Store an external Docker repository in Synapse.

        This method is used to create or update **external** (unmanaged) Docker
        repositories that reference images hosted on external registries like
        DockerHub or quay.io.

        Note: **Managed** Docker repositories (hosted at docker.synapse.org) cannot
        be created or modified via this method. To create a managed Docker repository
        or to migrate an external repository to be managed, you must push Docker images
        directly to the Synapse Docker registry. See the
        [Synapse Docker Registry documentation](https://docs.synapse.org/synapse-docs/synapse-docker-registry)
        for instructions.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The DockerRepository object.

        Raises:
            ValueError: If the Docker repository does not have a parent_id or
                repository_name set.

        Note: The `parent_id` must be a Project ID. Docker repositories cannot be
        placed inside Folders - they must be direct children of a Project.

        Example: Using this function
            Create an external Docker repository referencing a DockerHub image:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            syn = Synapse()
            syn.login()

            # parent_id must be a Project ID
            docker_repo = DockerRepository(
                parent_id="syn123",
                repository_name="dockerhub_username/my-repo"
            ).store()
            print(docker_repo.id)
            ```
        """
        return self

    def delete(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Delete the Docker repository from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the Docker repository does not have an id set.

        Example: Using this function
            Delete a Docker repository:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            syn = Synapse()
            syn.login()

            DockerRepository(id="syn123").delete()
            ```
        """
        return None
