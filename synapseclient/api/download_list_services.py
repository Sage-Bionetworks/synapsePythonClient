"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.DownloadListController>
"""

import json
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.download_list import DownloadListItem


async def clear_download_list_async(
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """Clear all files from the user's Synapse download list.

    <https://rest-docs.synapse.org/rest/DELETE/download/list.html>

    Arguments:
        synapse_client: If not passed in and caching was not disabled by
            Synapse.allow_client_caching(False) this will use the last created
            instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    await client.rest_delete_async("/download/list")


async def add_to_download_list_async(
    files: list["DownloadListItem"],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> int:
    """Add a batch of specific file versions to the user's Synapse download list.

    <https://rest-docs.synapse.org/rest/POST/download/list/add.html>

    Arguments:
        files: List of DownloadListItem objects identifying the file versions to add.
        synapse_client: If not passed in and caching was not disabled by
            Synapse.allow_client_caching(False) this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The number of files added to the download list.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    batch = [
        {"fileEntityId": item.file_entity_id, "versionNumber": item.version_number}
        for item in files
    ]
    request_body = {"batchToAdd": batch}
    response = await client.rest_post_async(
        "/download/list/add", body=json.dumps(request_body)
    )
    return response["numberOfFilesAdded"]


async def remove_from_download_list_async(
    files: list["DownloadListItem"],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> int:
    """Remove a batch of specific file versions from the user's Synapse download list.

    <https://rest-docs.synapse.org/rest/POST/download/list/remove.html>

    Arguments:
        files: List of DownloadListItem objects identifying the file versions to remove.
        synapse_client: If not passed in and caching was not disabled by
            Synapse.allow_client_caching(False) this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The number of files removed from the download list.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    batch = [
        {"fileEntityId": item.file_entity_id, "versionNumber": item.version_number}
        for item in files
    ]
    request_body = {"batchToRemove": batch}
    response = await client.rest_post_async(
        "/download/list/remove", body=json.dumps(request_body)
    )
    return response["numberOfFilesRemoved"]
