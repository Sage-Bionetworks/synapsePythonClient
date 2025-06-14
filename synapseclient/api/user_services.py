"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.UserGroupController>
"""

from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from synapseclient import Synapse


async def get_user_group_headers_batch(
    ids: List[str],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Dict[str, Union[str, bool]]]:
    """
    Batch get UserGroupHeaders. This fetches information about a collection of
    users or groups, specified by Synapse IDs.

    Arguments:
        ids: List of user/group IDs to retrieve
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List representing "children" in
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserGroupHeaderResponsePage.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    ids_param = ",".join(ids)
    params = {"ids": ids_param}

    response = await client.rest_get_async(uri="/userGroupHeaders/batch", params=params)

    if "children" in response:
        return response["children"] or []

    return []
