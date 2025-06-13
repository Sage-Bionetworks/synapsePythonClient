"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityBundleV2Controller>
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_entity_id_bundle2(
    entity_id: str,
    request: Optional[Dict[str, bool]] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to which the bundle belongs
        request: The request for the bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundleRequest.html>.
            If not passed in or None, the default request will be used:

            - includeEntity: True
            - includeAnnotations: True
            - includeFileHandles: True
            - includeRestrictionInformation: True
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    from synapseclient import Synapse

    if not request:
        request = {
            "includeEntity": True,
            "includeAnnotations": True,
            "includeFileHandles": True,
            "includeRestrictionInformation": True,
        }

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri=f"/entity/{entity_id}/bundle2",
        body=json.dumps(request),
    )


async def get_entity_id_version_bundle2(
    entity_id: str,
    version: int,
    request: Optional[Dict[str, bool]] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to which the bundle belongs
        version: The version of the entity to which the bundle belongs
        request: The request for the bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundleRequest.html>.
            If not passed in or None, the default request will be used:

            - includeEntity: True
            - includeAnnotations: True
            - includeFileHandles: True
            - includeRestrictionInformation: True
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    from synapseclient import Synapse

    if not request:
        request = {
            "includeEntity": True,
            "includeAnnotations": True,
            "includeFileHandles": True,
            "includeRestrictionInformation": True,
        }
    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri=f"/entity/{entity_id}/version/{version}/bundle2",
        body=json.dumps(request),
    )


async def post_entity_bundle2_create(
    request: Dict[str, Any],
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        request: The request for the bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundleCreate.html>
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/entity/bundle2/create"
        + (f"?generatedBy={generated_by}" if generated_by else ""),
        body=json.dumps(request),
    )


async def put_entity_id_bundle2(
    entity_id: str,
    request: Dict[str, Any],
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to which the bundle belongs.
        request: The request for the bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundleCreate.html>
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/entitybundle/v2/EntityBundle.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri=f"/entity/{entity_id}/bundle2"
        + (f"?generatedBy={generated_by}" if generated_by else ""),
        body=json.dumps(request),
    )


async def store_entity_with_bundle2(
    entity: Dict[str, Any],
    parent_id: Optional[str] = None,
    acl: Optional[Dict[str, Any]] = None,  # TODO: Consider skipping ACL?
    annotations: Optional[Dict[str, Any]] = None,
    activity: Optional[Dict[str, Any]] = None,
    new_version: bool = False,
    force_version: bool = False,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Store an entity in Synapse using the bundle2 API endpoints to reduce HTTP calls.

    This function follows a specific flow:
    1. Determines if the operation is a create or update:
       - If no ID is provided, searches for the ID via /entity/child
       - If no ID is found, treats as a Create
       - If an ID is found, treats as an Update

    2. For Updates:
       - Retrieves entity by ID and merges with existing data
       - Updates desired fields in the retrieved object
       - Pushes modified object with HTTP PUT if there are changes

    3. For Creates:
       - Creates a new object with desired fields
       - Pushes the new object with HTTP POST

    Arguments:
        entity: The entity to store.
        parent_id: The ID of the parent entity for creation.
        acl: Access control list for the entity.
        annotations: Annotations to associate with the entity.
        activity: Activity to associate with the entity.
        new_version: If True, create a new version of the entity.
        force_version: If True, forces a new version of an entity even if nothing has changed.
        synapse_client: Synapse client instance.

    Returns:
        The stored entity bundle.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Determine if this is a create or update operation
    entity_id = entity.get("id", None)

    # Construct bundle request based on provided data
    bundle_request = {"entity": entity}

    if annotations:
        bundle_request["annotations"] = annotations

    if acl:
        bundle_request["accessControlList"] = acl

    if activity:
        bundle_request["activity"] = activity

    # Handle create or update
    if not entity_id:
        # This is a creation
        client.logger.debug("Creating new entity via bundle2 API")

        # For creation, parent ID is required
        # TODO: Projects won't have a parent in this case
        # if parent_id:
        #     # Add parentId to the entity if not already set
        #     if not entity.get("parentId"):
        #         entity["parentId"] = parent_id
        # elif not entity.get("parentId"):
        #     raise ValueError("Parent ID must be provided for entity creation")

        # Create entity using bundle2 create endpoint
        return await post_entity_bundle2_create(
            request=bundle_request,
            generated_by=activity.get("id") if activity else None,
            synapse_client=synapse_client,
        )
    else:
        # This is an update
        client.logger.debug(f"Updating entity {entity_id} via bundle2 API")

        # For updates we might need to retrieve the existing entity to merge data
        # Only retrieve if we need
