"""Functional interface for searching for entities in Synapse."""

from typing import TYPE_CHECKING, Optional, Union

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models.services.storable_entity_components import FailureStrategy

if TYPE_CHECKING:
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        Link,
        MaterializedView,
        Project,
        RecordSet,
        SubmissionView,
        Table,
        VirtualTable,
    )


async def get_id(
    entity: Union[
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "Link",
        "MaterializedView",
        "Project",
        "RecordSet",
        "SubmissionView",
        "Table",
        "VirtualTable",
    ],
    failure_strategy: Optional[FailureStrategy] = FailureStrategy.RAISE_EXCEPTION,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Optional[str]:
    """
    Get the ID of the entity from either the ID field or the name/parent of the entity.
    This is a wrapper for the [synapseclient.operations.find_entity_id_async][] function
    that is used in order to search by name/parent.

    Arguments:
        entity: The entity to resolve the ID for. Resolution uses the id field if
            set, otherwise the name and parent_id fields.
        failure_strategy: Determines how to handle failures when getting the entity
            from Synapse and an exception occurs. Only RAISE_EXCEPTION and None are
            supported.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The ID of the entity.

    Raises:
        ValueError: If the entity ID or Name and parent is not set.
        SynapseNotFoundError: If the entity is not found in Synapse.
    """
    can_search = (
        entity.id
        or (
            entity.name and (entity.__class__.__name__ == "Project" or entity.parent_id)
        )
    ) is not None
    if not can_search:
        if failure_strategy is None:
            return None
        raise ValueError("Entity ID or Name/Parent is required")

    from synapseclient.operations import find_entity_id_async

    entity_id = entity.id or await find_entity_id_async(
        name=entity.name,
        parent=entity.parent_id,
        synapse_client=synapse_client,
    )

    if not entity_id:
        if failure_strategy is None:
            return None
        raise SynapseNotFoundError(
            f"{entity.__class__.__name__} [Id: {entity.id}, Name: {entity.name}, "
            f"Parent: {entity.parent_id}] not found in Synapse."
        )
    entity.id = entity_id
    return entity_id
