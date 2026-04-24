"""Services for interacting with storage location settings in Synapse.

This module provides async REST wrappers for creating and retrieving
storage location settings.
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
