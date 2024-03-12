"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityController>
"""

import json
from typing import Any, Dict, Optional, Union

from synapseclient import Synapse


async def post_entity(
    request: Dict[str, Any],
    generated_by: Optional[str] = None,
    synapse_client: Optional[Synapse] = None,
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
    synapse_client: Optional[Synapse] = None,
) -> Dict[str, Any]:
    """
    Arguments:
        entity_id: The ID of the entity to update.
        request: The request for the entity matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
        generated_by: The ID of the activity to associate with the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The requested entity bundle matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Entity.html>
    """
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
    synapse_client: Optional[Synapse] = None,
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
    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}",
    )


async def get_upload_destination(
    entity_id: str, synapse_client: Optional[Synapse] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination.html>

    Arguments:
        entity_id: The ID of the entity.
        endpoint: Server endpoint to call to.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination",
        endpoint=client.fileHandleEndpoint,
    )


async def get_upload_destination_location(
    entity_id: str, location: str, synapse_client: Optional[Synapse] = None
) -> Dict[str, Union[str, int]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/uploadDestination/storageLocationId.html>

    Arguments:
        entity_id: The ID of the entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The upload destination.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadDestination.html>
    """
    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{entity_id}/uploadDestination/{location}",
        endpoint=client.fileHandleEndpoint,
    )
