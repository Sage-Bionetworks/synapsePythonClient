"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.DockerController>
"""

import urllib.parse as urllib_parse
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_entity_id_by_repository_name(
    repository_name: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Get the Synapse entity ID for a managed Docker repository by its repository name.

    <https://rest-docs.synapse.org/rest/GET/entity/dockerRepo/id.html>

    Arguments:
        repository_name: The name of the managed Docker repository
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The Synapse entity ID of the Docker repository.

    Raises:
        SynapseHTTPError: If the repository is not found or is not a managed repository.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    encoded_name = urllib_parse.quote(repository_name, safe="")
    response = await client.rest_get_async(
        uri=f"/entity/dockerRepo/id?repositoryName={encoded_name}",
    )
    return response["id"]
