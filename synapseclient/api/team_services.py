"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.TeamController>
"""

import json
from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from synapseclient import Synapse


async def post_team_list(
    team_ids: List[int],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[List[Dict[str, Union[str, bool]]]]:
    """
    Retrieve a list of Teams given their IDs. Invalid IDs in the list are ignored:
    The results list is simply smaller than the list of IDs passed in.

    Arguments:
        team_ids: List of team IDs to retrieve
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        List of dictionaries representing <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Team.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"list": team_ids}

    response = await client.rest_post_async(
        uri="/teamList", body=json.dumps(request_body)
    )

    if "list" in response:
        return response["list"] or None

    return None
