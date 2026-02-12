"""Docker repository dataclass model for Synapse entities."""

import asyncio
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Optional

from synapseclient import Synapse
from synapseclient.api.docker_services import get_entity_id_by_repository_name
from synapseclient.api.entity_bundle_services_v2 import get_entity_id_bundle2
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants.concrete_types import DOCKER_REPOSITORY
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.protocols.docker_protocol import (
    DockerRepositorySynchronousProtocol,
)
from synapseclient.models.services.storable_entity import store_entity


@dataclass()
@async_to_sync
class DockerRepository(DockerRepositorySynchronousProtocol):
    """A Docker repository entity within Synapse.

    A Docker repository is a lightweight virtual machine image.

    Represents a [Synapse DockerRepository](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/docker/DockerRepository.html).

    There are two types of Docker repositories in Synapse:

    - **Managed repositories** (`is_managed=True`): Hosted on Synapse's Docker registry
      at `docker.synapse.org`. These cannot be created or modified via this API. To create
      a managed Docker repository, you must push a Docker image directly to the Synapse
      Docker registry. See the
      [Synapse Docker Registry documentation](https://docs.synapse.org/synapse-docs/synapse-docker-registry)
      for instructions.

    - **External repositories** (`is_managed=False`): References to Docker images hosted
      on external registries like DockerHub or quay.io. These can be created and updated
      using the `store()` method.

    Attributes:
        id: The unique immutable ID for this entity. A new ID will be generated for new
            Entities. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
            apostrophes, and parentheses.
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: (Read Only) The date this entity was created.
        modified_on: (Read Only) The date this entity was last modified.
        created_by: (Read Only) The ID of the user that created this entity.
        modified_by: (Read Only) The ID of the user that last modified this entity.
        parent_id: The ID of the Project that is the parent of this Docker repository.
            Docker repositories must be direct children of a Project, not a Folder.
        repository_name: The name of the Docker Repository. Usually in the format:
            [host[:port]/]path. If host is not set, it will default to that of DockerHub.
            Port can only be specified if the host is also specified.
        is_managed: (Read Only) Whether this Docker repository is managed by Synapse.
            If True, the repository is hosted on Synapse's Docker registry. If False,
            it references an external Docker registry.
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new
    Entities. Once issued, this ID is guaranteed to never change or be re-issued."""

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses."""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated
    it is used to detect when a client's current representation of an entity is
    out-of-date."""

    created_on: Optional[str] = None
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = None
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Project that is the parent of this Docker repository.
    Docker repositories must be direct children of a Project, not a Folder."""

    repository_name: Optional[str] = None
    """The name of the Docker Repository. Usually in the format: [host[:port]/]path.
    If host is not set, it will default to that of DockerHub. Port can only be
    specified if the host is also specified."""

    is_managed: Optional[bool] = None
    """(Read Only) Whether this Docker repository is managed by Synapse. If True,
    the repository is hosted on Synapse's Docker registry. If False, it references
    an external Docker registry."""

    _last_persistent_instance: Optional["DockerRepository"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = replace(self)

    def fill_from_dict(self, synapse_entity: Dict[str, Any]) -> "DockerRepository":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_entity: The response from the REST API.

        Returns:
            The DockerRepository object.
        """
        self.id = synapse_entity.get("id", None)
        self.name = synapse_entity.get("name", None)
        self.description = synapse_entity.get("description", None)
        self.etag = synapse_entity.get("etag", None)
        self.created_on = synapse_entity.get("createdOn", None)
        self.modified_on = synapse_entity.get("modifiedOn", None)
        self.created_by = synapse_entity.get("createdBy", None)
        self.modified_by = synapse_entity.get("modifiedBy", None)
        self.parent_id = synapse_entity.get("parentId", None)
        self.repository_name = synapse_entity.get("repositoryName", None)
        self.is_managed = synapse_entity.get("isManaged", None)

        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "etag": self.etag,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": DOCKER_REPOSITORY,
            "repositoryName": self.repository_name,
        }
        delete_none_keys(request_dict)
        return request_dict

    async def get_async(
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
            print(docker_repo)
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
            print(docker_repo)
            ```
        """
        if not self.id and not self.repository_name:
            raise ValueError(
                "The Docker repository must have either an id or repository_name set."
            )

        # If we only have repository_name, look up the entity ID first
        if not self.id and self.repository_name:
            self.id = await self._get_entity_id_by_repository_name(
                synapse_client=synapse_client,
            )

        bundle = await get_entity_id_bundle2(
            entity_id=self.id,
            request={"includeEntity": True},
            synapse_client=synapse_client,
        )
        self.fill_from_dict(synapse_entity=bundle["entity"])

        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Got DockerRepository {self.repository_name}, id: {self.id}"
        )
        return self

    async def _get_entity_id_by_repository_name(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Get the Synapse entity ID for a managed Docker repository by its repository name.

        This uses the `GET /entity/dockerRepo/id` endpoint which only works for
        **managed** Docker repositories (those hosted on Synapse's Docker registry).

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Synapse entity ID of the Docker repository.

        Raises:
            SynapseHTTPError: If the repository is not found or is not a managed repository.
        """
        return await get_entity_id_by_repository_name(
            repository_name=self.repository_name,
            synapse_client=synapse_client,
        )

    async def store_async(
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
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            async def create_external_docker_repo():
                syn = Synapse()
                syn.login()

                # parent_id must be a Project ID
                docker_repo = await DockerRepository(
                    parent_id="syn123",
                    repository_name="my-repo"
                ).store_async()
                return docker_repo

            docker_repo = asyncio.run(create_external_docker_repo())
            print(docker_repo.id)
            ```
        """
        if not self.id and not self.parent_id:
            raise ValueError(
                "The Docker repository must have a parent_id set to store."
            )
        if not self.id and not self.repository_name:
            raise ValueError(
                "The Docker repository must have a repository_name set to store."
            )

        if self.has_changed:
            entity = await store_entity(
                resource=self,
                entity=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
            self.fill_from_dict(synapse_entity=entity)

        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Stored DockerRepository {self.repository_name}, id: {self.id}"
        )
        return self

    async def delete_async(
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
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DockerRepository

            async def delete_docker_repo():
                syn = Synapse()
                syn.login()

                await DockerRepository(id="syn123").delete_async()

            # Run the async function
            asyncio.run(delete_docker_repo())
            ```
        """
        if not self.id:
            raise ValueError("The Docker repository must have an id set.")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                obj=self.id,
            ),
        )

        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Deleted DockerRepository {self.id}"
        )
