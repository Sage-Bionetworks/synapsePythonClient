"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org#org.sagebionetworks.repo.web.controller.CurationTaskController>
"""

import json
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Optional, Union

from synapseclient.api.api_client import rest_post_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_curation_task(
    curation_task: Dict[str, Union[str, int, Dict[str, Any]]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Create a CurationTask associated with a project.

    https://rest-docs.synapse.org/rest/POST/curation/task.html
    Arguments:
        curation_task: The complete CurationTask object to create.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The created CurationTask.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_post_async(
        uri="/curation/task", body=json.dumps(curation_task)
    )


async def get_curation_task(
    task_id: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Get a CurationTask by its ID.

    https://rest-docs.synapse.org/rest/GET/curation/task/taskId.html

    Arguments:
        task_id: The unique identifier of the task.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The CurationTask.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(uri=f"/curation/task/{task_id}")


async def update_curation_task(
    task_id: int,
    curation_task: Dict[str, Union[str, int, Dict[str, Any]]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Update a CurationTask.

    https://rest-docs.synapse.org/rest/PUT/curation/task/taskId.html

    Arguments:
        task_id: The unique identifier of the task.
        curation_task: The complete CurationTask object to update.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated CurationTask.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_put_async(
        uri=f"/curation/task/{task_id}", body=json.dumps(curation_task)
    )


async def delete_curation_task(
    task_id: int,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete a CurationTask.

    https://rest-docs.synapse.org/rest/DELETE/curation/task/taskId.html

    Arguments:
        task_id: The unique identifier of the task.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/curation/task/{task_id}")


async def list_curation_tasks(
    project_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Generator to get a list of CurationTasks for a project.

    https://rest-docs.synapse.org/rest/POST/curation/task/list.html

    Arguments:
        project_id: The synId of the project.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Yields:
        Individual CurationTask objects from each page of the response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"projectId": project_id}

    async for item in rest_post_paginated_async(
        "/curation/task/list", body=request_body, synapse_client=client
    ):
        yield item


async def list_grid_sessions(
    source_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Generator to get a list of active grid sessions for the user.

    https://rest-docs.synapse.org/rest/POST/grid/session/list.html

    Arguments:
        source_id: Optional. When provided, only sessions with this synId will be returned.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Yields:
        Individual GridSession objects from each page of the response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {}
    if source_id is not None:
        request_body["sourceId"] = source_id

    async for item in rest_post_paginated_async(
        "/grid/session/list", body=request_body, synapse_client=client
    ):
        yield item


async def delete_grid_session(
    session_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete a grid session.

    https://rest-docs.synapse.org/rest/DELETE/grid/session/sessionId.html

    Note: Only the user that created a grid session may delete it.

    Arguments:
        session_id: The unique identifier of the grid session to delete.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        None
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/grid/session/{session_id}")
