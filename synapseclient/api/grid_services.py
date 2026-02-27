"""API services for Grid session operations.

This module provides low-level async functions for grid replica
and presigned URL management.

https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/GridController.html
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_grid_replica(
    session_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Create a new grid replica for a grid session.

    A replica is an in-memory document that represents a 'copy' of the grid.
    Each replica is identified by a unique replicaId.

    https://rest-docs.synapse.org/rest/POST/grid/session/sessionId/replica.html

    Arguments:
        session_id: The ID of the grid session.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last
            created instance from the Synapse class constructor.

    Returns:
        CreateReplicaResponse containing the replica information.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"gridSessionId": session_id}

    return await client.rest_post_async(
        uri=f"/grid/session/{session_id}/replica",
        body=json.dumps(request_body),
    )


async def get_grid_presigned_url(
    session_id: str,
    replica_id: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Create a presigned URL to establish a WebSocket connection with a grid
    session. The presigned URL will expire 15 minutes after it is issued.

    https://rest-docs.synapse.org/rest/POST/grid/session/sessionId/presigned/url.html

    Arguments:
        session_id: The ID of the grid session.
        replica_id: The replica ID that will use this WebSocket connection.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last
            created instance from the Synapse class constructor.

    Returns:
        The presigned WebSocket URL string.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {
        "gridSessionId": session_id,
        "replicaId": replica_id,
    }

    response = await client.rest_post_async(
        uri=f"/grid/session/{session_id}/presigned/url",
        body=json.dumps(request_body),
    )

    return response.get("presignedUrl", "")


async def get_grid_session(
    session_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Get the basic information about an existing grid session.

    https://rest-docs.synapse.org/rest/GET/grid/session/sessionId.html

    Arguments:
        session_id: The ID of the grid session.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last
            created instance from the Synapse class constructor.

    Returns:
        GridSession information.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(uri=f"/grid/session/{session_id}")
