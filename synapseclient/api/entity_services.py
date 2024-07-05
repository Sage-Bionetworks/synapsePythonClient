"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityController>
"""

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from async_lru import alru_cache

if TYPE_CHECKING:
    from synapseclient import Synapse


async def post_entity(
    request: Dict[str, Any],
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        request: The request for the entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The requested entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    params = {}
    if generated_by:
        params["generatedBy"] = generated_by
    return await client.rest_post_async(
        uri="/entity", body=json.dumps(request), params=params
    )


async def put_entity(
    entity_id: str,
    request: Dict[str, Any],
    new_version: bool = False,
    generated_by: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to update.
        request: The request for the entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
        new_version: If true, a new version of the entity will be created.
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    params = {}
    if generated_by:
        params["generatedBy"] = generated_by
    if new_version:
        params["newVersion"] = "true"
    return await client.rest_put_async(
        uri=f"/entity/{entity_id}", body=json.dumps(request), params=params
    )


async def get_entity(
    entity_id: str,
    version_number: int = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    if version_number:
        return await client.rest_get_async(
            uri=f"/entity/{entity_id}/version/{version_number}",
        )
    else:
        return await client.rest_get_async(
            uri=f"/entity/{entity_id}",
        )


@alru_cache(ttl=60)
async def get_upload_destination(
    entity_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination",
        endpoint=client.fileHandleEndpoint,
    )


async def get_upload_destination_location(
    entity_id: str, location: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination/storageLocationId.html>

    Arguments:
        entity_id: The ID of the entity.
        location: A storage location ID of the upload destination.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination/{location}",
        endpoint=client.fileHandleEndpoint,
    )


async def create_access_requirements_if_none(
    entity_id: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    Checks to see if the given entity has access requirements. If not, then one is added

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    existing_restrictions = await client.rest_get_async(
        f"/entity/{entity_id}/accessRequirement?offset=0&limit=1"
    )
    if (
        existing_restrictions is None
        or not hasattr(existing_restrictions, "results")
        or len(existing_restrictions["results"]) == 0
    ):
        access_requirements = await client.rest_post_async(
            f"/entity/{entity_id}/lockAccessRequirement"
        )
        client.logger.info(
            "Created an access requirements request for "
            f"{entity_id}: {access_requirements['jiraKey']}. An email will be sent to "
            "the Synapse access control team to start the process of adding "
            "terms-of-use or review board approval for this entity."
        )


async def delete_entity_generated_by(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns: None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_delete_async(
        uri=f"/entity/{entity_id}/generatedBy",
    )


async def get_entity_path(
    entity_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, List[Dict[str, Union[str, int, bool]]]]:
    """
    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/id/path.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        Entity paths matching:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityPath.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/path",
    )


async def get_entities_by_md5(
    md5: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[int, List[Dict[str, Any]]]]:
    """
    Implements:
    <https://rest-docs.synapse.org/rest/GET/entity/md5/md5.html>

    Arguments:
        md5: The MD5 of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        Paginated results of:
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/reflection/model/PaginatedResults.html>
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityHeader.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/md5/{md5}",
    )
