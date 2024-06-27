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
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

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
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

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
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

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
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

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
