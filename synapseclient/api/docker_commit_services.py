"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.DockerCommitController>
"""
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_docker_tag(
    entity_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict:
    """
    Arguments:
        entity_id: The ID of the Docker repository entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The Docker tag information for the entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/dockerTag",
    )

