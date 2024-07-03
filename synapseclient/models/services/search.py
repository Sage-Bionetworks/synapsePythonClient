"""Functional interface for searching for entities in Synapse."""

import asyncio
from typing import TYPE_CHECKING, Optional, Union

from opentelemetry import context

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.core.utils import run_and_attach_otel_context
from synapseclient.models.services.storable_entity_components import FailureStrategy

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project


async def get_id(
    entity: Union["Project", "Folder", "File"],
    failure_strategy: Optional[FailureStrategy] = FailureStrategy.RAISE_EXCEPTION,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Union[str, None]:
    """
    Get the ID of the entity from either the ID field or the name/parent of the entity.
    This is a wrapper for the [synapseclient.Synapse.findEntityId][] method that is
    used in order to search by name/parent.

    Arguments:
        failure_strategy: Determines how to handle failures when getting the entity
            from Synapse and an exception occurs. Only RAISE_EXCEPTION and None are
            supported.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

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

    loop = asyncio.get_event_loop()
    current_context = context.get_current()
    entity_id = entity.id or await loop.run_in_executor(
        None,
        lambda: run_and_attach_otel_context(
            lambda: Synapse.get_client(synapse_client=synapse_client).findEntityId(
                name=entity.name,
                parent=entity.parent_id,
            ),
            current_context,
        ),
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
