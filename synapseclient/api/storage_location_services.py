"""Services for interacting with storage location settings and project settings in Synapse.

This module provides async REST wrappers for creating, retrieving, and managing
storage location settings and their associated project settings.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_storage_location_setting(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a new storage location in Synapse that can be linked to a project,
    allowing users to upload their data to a storage location they own.

    Storage location creation is idempotent per user - if the same user creates
    a storage location with identical properties, the existing one is returned.

    Arguments:
        request: The storage location setting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/StorageLocationSetting.html>.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created storage location setting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/StorageLocationSetting.html>.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/storageLocation",
        body=json.dumps(request),
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
        The created storage location setting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/StorageLocationSetting.html>.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/storageLocation/{storage_location_id}",
    )


async def get_project_setting(
    project_id: str,
    project_setting_type: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[Dict[str, Any]]:
    """Retrieve the project setting of a particular setting type for the project or folder.
    Only users with READ access on a project can retrieve its project settings.

    Arguments:
        project_id: The Synapse ID of the project or folder.
        project_setting_type: The type of project setting to retrieve. Currently supports 'upload' only.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The upload destination list setting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/UploadDestinationListSetting.html>.
        If the storage location is Synapse S3, the response will be an empty string.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    response = await client.rest_get_async(
        uri=f"/projectSettings/{project_id}/type/{project_setting_type}",
    )
    return response


async def create_project_setting(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """Create a project setting for a project or folder.
    Only the users with CREATE access to the project or folder can add a project setting.
    Currently, only the "upload" project setting is supported. This is implemented using UploadDestinationListSetting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/UploadDestinationListSetting.html>.
    A project can have a maximum of 10 storage locations.

    Arguments:
        request: The project setting request body matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/ProjectSetting.html>.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created project setting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/ProjectSetting.html>.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        uri="/projectSettings",
        body=json.dumps(request),
    )


async def update_project_setting(
    request: Dict[str, Any],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Update an existing project setting for a project or folder.
    Only the users with UPDATE access to the project or folder can update a project setting.
    Currently, only the "upload" project setting is supported. This is implemented using UploadDestinationListSetting matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/UploadDestinationListSetting.html>.
    A project can have a maximum of 10 storage locations.

    Arguments:
        request: The project setting request body including the id field matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/ProjectSetting.html>.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        uri="/projectSettings",
        body=json.dumps(request),
    )


async def delete_project_setting(
    project_setting_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Delete a project setting for a project or folder.
    Only the users with DELETE access to the project or folder can delete a project setting.

    Arguments:
        project_setting_id: The ID of the project setting to delete.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async(
        uri=f"/projectSettings/{project_setting_id}",
    )
