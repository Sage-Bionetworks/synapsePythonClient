"""Script used to store an entity to Synapse."""

from typing import TYPE_CHECKING, Dict, Optional, Union

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    create_access_requirements_if_none,
    post_entity,
    put_entity,
)
from synapseclient.core.utils import get_properties

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project


async def store_entity(
    resource: Union["File", "Folder", "Project"],
    entity: Dict[str, Union[str, bool, int, float]],
    *,
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """
    Function to store an entity to synapse.

    TODO: This function is not complete and is a work in progress.


    Arguments:
        resource: The root dataclass instance we are storing data for.
        entity: The entity to store.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        If a read from Synapse is required to retireve the current state of the entity.
    """
    query_params = {}
    increment_version = False
    # Create or update Entity in Synapse
    if resource.id:
        trace.get_current_span().set_attributes({"synapse.id": resource.id})
        if hasattr(resource, "version_number"):
            if (
                resource.version_label
                and resource.version_label
                != resource._last_persistent_instance.version_label
            ):
                # a versionLabel implicitly implies incrementing
                increment_version = True
            elif resource.force_version and resource.version_number:
                increment_version = True
                entity["versionLabel"] = str(resource.version_number + 1)

            if increment_version:
                query_params["newVersion"] = "true"

        updated_entity = await put_entity(
            entity_id=resource.id,
            request=get_properties(entity),
            new_version=increment_version,
            synapse_client=synapse_client,
        )
    else:
        # TODO - When Link is implemented this needs to be completed
        # If Link, get the target name, version number and concrete type and store in link properties
        # if properties["concreteType"] == "org.sagebionetworks.repo.model.Link":
        #     target_properties = self._getEntity(
        #         properties["linksTo"]["targetId"],
        #         version=properties["linksTo"].get("targetVersionNumber"),
        #     )
        #     if target_properties["parentId"] == properties["parentId"]:
        #         raise ValueError(
        #             "Cannot create a Link to an entity under the same parent."
        #         )
        #     properties["linksToClassName"] = target_properties["concreteType"]
        #     if (
        #         target_properties.get("versionNumber") is not None
        #         and properties["linksTo"].get("targetVersionNumber") is not None
        #     ):
        #         properties["linksTo"]["targetVersionNumber"] = target_properties[
        #             "versionNumber"
        #         ]
        #     properties["name"] = target_properties["name"]

        updated_entity = await post_entity(
            request=get_properties(entity),
            synapse_client=synapse_client,
        )

    if hasattr(resource, "is_restricted") and resource.is_restricted:
        await create_access_requirements_if_none(entity_id=updated_entity.get("id"))

    trace.get_current_span().set_attributes(
        {
            "synapse.id": updated_entity.get("id"),
            "synapse.concrete_type": updated_entity.get("concreteType", ""),
        }
    )
    return updated_entity
