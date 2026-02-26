from synapseclient import Synapse
from synapseclient.models import Project
from synapseclient.operations import get


def project_id_from_entity_id(entity_id: str, synapse_client: Synapse) -> str:
    """
    Retrieves the project ID from a given entity ID by traversing up the folder hierarchy

    Args:
        entity_id: The Synapse ID of the entity (e.g., folder, file) to start from.
        synapse_client: Authenticated Synapse client instance

    Returns:
        The Synapse ID of the project that the entity belongs to.

    Raises:
        ValueError: If the project ID cannot be found within 1000 iterations.
    """

    # Get the project ID from the folder ID
    current_obj = get(entity_id, synapse_client=synapse_client)
    iterations = 0
    while not isinstance(current_obj, Project):
        current_obj = get(current_obj.parent_id, synapse_client=synapse_client)
        iterations += 1
        if iterations > 1000:
            raise ValueError("Could not find project ID in folder hierarchy")
    return current_obj.id
