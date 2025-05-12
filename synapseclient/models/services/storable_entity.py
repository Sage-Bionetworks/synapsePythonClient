"""Script used to store an entity to Synapse."""

from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import (
    create_access_requirements_if_none,
    get_entity_id_bundle2,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from synapseclient.core.utils import get_properties

if TYPE_CHECKING:
    from synapseclient.models import (
        Annotations,
        Dataset,
        EntityView,
        File,
        Folder,
        Project,
        Table,
    )


async def store_entity(
    resource: Union["File", "Folder", "Project", "Table", "Dataset", "EntityView"],
    entity: Dict[str, Union[str, bool, int, float]],
    parent_id: Optional[str] = None,
    acl: Optional[Dict[str, Any]] = None,
    new_version: bool = False,
    force_version: bool = False,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Dict[str, Any]:
    """
    Function to store an entity to synapse using the bundle2 service.

    This function handles both creation and update of entities in Synapse:
    1. For new entities (without an ID):
       - Creates a new entity with the provided data
       - Pushes the new entity with bundle2 create endpoint

    2. For existing entities (with an ID):
       - Updates the entity with the provided data
       - Pushes the updated entity with bundle2 update endpoint

    Arguments:
        resource: The root dataclass instance we are storing data for.
        entity: The entity to store.
        parent_id: The ID of the parent entity for creation.
        acl: Access control list for the entity.
        new_version: If True, create a new version of the entity.
        force_version: If True, forces a new version of an entity even if nothing has changed.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The entity data from the stored entity bundle.
    """
    from synapseclient.models import Annotations

    # First, handle the activity if it exists and needs to be stored
    activity_id = None
    if hasattr(resource, "activity") and resource.activity is not None:
        # Store the activity first if it doesn't have an ID yet or if it's changed
        last_persistent_instance = getattr(resource, "_last_persistent_instance", None)
        activity_changed = (
            last_persistent_instance is None
            or last_persistent_instance.activity != resource.activity
        )

        if not resource.activity.id or activity_changed:
            resource.activity = await resource.activity.store_async(
                synapse_client=synapse_client
            )

        activity_id = resource.activity.id

    # Prepare annotations if they exist
    annotations = None
    if hasattr(resource, "annotations") and resource.annotations:
        annotations = Annotations(resource.annotations).to_synapse_request()

    # Set trace attributes if ID exists
    if resource.id:
        trace.get_current_span().set_attributes({"synapse.id": resource.id})

    # Handle versioning attributes if not already specified
    # TODO: force_version is not yet supported in the bundle2 API: https://sagebionetworks.jira.com/browse/PLFM-8313
    if (
        not force_version
        and hasattr(resource, "version_number")
        and hasattr(resource, "force_version")
    ):
        if resource.force_version:
            force_version = True

    # Get parent_id from resource if not specified
    if parent_id is None:
        parent_id = getattr(resource, "parent_id", None)

    # Get client
    client = Synapse.get_client(synapse_client=synapse_client)

    # Determine if this is a create or update operation
    entity_id = entity.get("id", None)

    # Construct bundle request based on provided data
    bundle_request = {"entity": entity.to_synapse_request()}

    if annotations:
        bundle_request["annotations"] = annotations

    if acl:
        bundle_request["accessControlList"] = acl

    if activity_id:
        bundle_request["activity"] = activity_id

    # Handle create or update
    if not entity_id:
        # This is a creation
        client.logger.debug("Creating new entity via bundle2 API")

        # Create entity using bundle2 create endpoint
        updated_entity = await post_entity_bundle2_create(
            request=bundle_request,
            generated_by=activity_id,
            synapse_client=synapse_client,
        )
    else:
        # This is an update
        client.logger.debug(f"Updating entity {entity_id} via bundle2 API")

        # If we're creating a new version or forcing one, we need to update
        # the entity directly instead of via bundle2
        updated_entity = await put_entity_id_bundle2(
            entity_id=entity_id,
            request=bundle_request,
            generated_by=activity_id,
            synapse_client=synapse_client,
        )

    # Handle access restrictions if needed
    if hasattr(resource, "is_restricted") and resource.is_restricted:
        await create_access_requirements_if_none(
            entity_id=updated_entity["entity"].get("id")
        )

    # Set trace attributes
    trace.get_current_span().set_attributes(
        {
            "synapse.id": updated_entity["entity"].get("id"),
            "synapse.concrete_type": updated_entity["entity"].get("concreteType", ""),
        }
    )

    return updated_entity["entity"]
