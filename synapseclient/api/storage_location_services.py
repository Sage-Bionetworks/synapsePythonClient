"""Services for interacting with storage location settings and project settings in Synapse.

This module provides async REST wrappers for creating, retrieving, and managing
storage location settings and their associated project settings.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_storage_location_setting(
    body: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new storage location setting in Synapse.

    Storage location creation is idempotent per user - if the same user creates
    a storage location with identical properties, the existing one is returned.

    Arguments:
        body: The storage location setting request body containing concreteType
            and other type-specific fields.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created or existing storage location setting as a dictionary.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/storageLocation",
        body=json.dumps(body),
    )


async def get_storage_location_setting(
    storage_location_id: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Retrieve a storage location setting by its ID.

    Only the creator of a StorageLocationSetting can retrieve it by its ID.

    Arguments:
        storage_location_id: The ID of the storage location setting to retrieve.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The storage location setting as a dictionary.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/storageLocation/{storage_location_id}",
    )


async def get_project_setting(
    project_id: str,
    setting_type: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[Dict[str, Any]]:
    """Get the project setting for an entity.

    Arguments:
        project_id: The Synapse ID of the project or folder.
        setting_type: The type of setting to retrieve. One of:
            'upload', 'external_sync', 'requester_pays'.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The project setting as a dictionary, or None if no setting exists.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    response = await client.rest_get_async(
        uri=f"/projectSettings/{project_id}/type/{setting_type}",
    )
    # If no project setting, an empty string is returned as the response
    return response if response else None


async def create_project_setting(
    body: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new project setting.

    Arguments:
        body: The project setting request body.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created project setting as a dictionary.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/projectSettings",
        body=json.dumps(body),
    )


async def update_project_setting(
    body: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Update an existing project setting.

    Arguments:
        body: The project setting request body including the id field.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated project setting as a dictionary.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri="/projectSettings",
        body=json.dumps(body),
    )


async def delete_project_setting(
    setting_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Delete a project setting.

    Arguments:
        setting_id: The ID of the project setting to delete.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async(
        uri=f"/projectSettings/{setting_id}",
    )
