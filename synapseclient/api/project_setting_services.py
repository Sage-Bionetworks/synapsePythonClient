"""Services for interacting with project settings in Synapse.

This module provides async REST wrappers for creating, retrieving, updating,
and deleting project settings.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_project_setting(
    project_id: str,
    setting_type: str = "upload",
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[Dict[str, Any]]:
    """Retrieve the project setting of a particular setting type for the project or folder.
    Only users with READ access on a project can retrieve its project settings.

    Arguments:
        project_id: The Synapse ID of the project or folder.
        setting_type: The type of project setting to retrieve. Currently supports 'upload' only.
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
        uri=f"/projectSettings/{project_id}/type/{setting_type}",
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
    setting_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Delete a project setting for a project or folder.
    Only the users with DELETE access to the project or folder can delete a project setting.

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
