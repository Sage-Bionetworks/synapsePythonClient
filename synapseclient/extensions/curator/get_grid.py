"""
High-level helper for getting or creating a Grid for a CurationTask.

This module provides a single library function, get_curator_grid, that a data
contributor can call after being assigned a CurationTask by a data manager. It
returns the Grid that is currently attached to the task, creating and attaching
a new grid session first if none exists yet.
"""

from synapseclient import Synapse  # type: ignore
from synapseclient.models import (  # type: ignore
    CurationTask,
    Grid,
    GridExecutionDetails,
)


def get_curator_grid(
    task_id: int,
    *,
    owner_principal_id: int | None = None,
    timeout: int = 120,
    synapse_client: Synapse | None = None,
) -> Grid:
    """
    Get the Grid attached to a CurationTask, creating one if none exists.

    This is the high-level entry point for a data contributor who has been
    assigned a CurationTask by a data manager and wants to start curating in a
    grid. It performs the following steps:

    1. Gets the CurationTask from Synapse.
    2. Checks whether a grid session is already attached to the task.
    3. If a session is attached, gets that Grid from Synapse and returns it.
    4. If no session is attached, creates a new grid session, attaches it to the
       task, and returns the new Grid.

    When a new grid session is created it uses the task's suggested authorization
    mode (set on the task by the data manager). SESSION_OWNER limits access to
    the session owner and their team, while SOURCE_BENEFACTOR extends access to
    anyone with EDIT permission on the data being curated. When the task has no
    suggested authorization mode, the caller becomes the session owner.

    Example: Get or create the grid for an assigned curation task
        &nbsp;
        A data contributor opens the grid for a task they were assigned. The
        first call creates the grid session and attaches it to the task;
        subsequent calls return that same grid.
        ```python
        import synapseclient
        from synapseclient.extensions.curator import get_curator_grid

        syn = synapseclient.Synapse()
        syn.login()

        grid = get_curator_grid(task_id=123)
        print(f"Grid session: {grid.session_id}")
        ```

    Arguments:
        task_id: The unique identifier of the CurationTask to get or create a
            grid for.
        owner_principal_id: The principal ID (user or team) that will own a newly
            created grid session. Ignored when a session is already attached to
            the task. When not provided, the caller becomes the owner (subject to
            the task's suggested authorization mode).
        timeout: Seconds to wait for the grid creation job when a new session must
            be created. Defaults to 120.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The Grid attached to the CurationTask.

    Raises:
        ValueError: If the task's status has non-grid execution details, or the
            task's properties are of an unsupported type for grid creation.
        SynapseHTTPError: If there are issues communicating with Synapse.
    """
    client = Synapse.get_client(synapse_client=synapse_client)

    # Step 1: get the CurationTask.
    client.logger.info(f"Attempting to get CurationTask {task_id}.")
    task = CurationTask(task_id=task_id).get(synapse_client=client)
    client.logger.info(f"Got CurationTask {task_id}.")

    # Step 2: check whether a grid session is already attached to the task.
    client.logger.info(f"Attempting to get status for CurationTask {task_id}.")
    status = task.get_status(synapse_client=client)
    client.logger.info(f"Got status for CurationTask {task_id}.")

    execution_details = status.execution_details
    active_session_id = None
    if isinstance(execution_details, GridExecutionDetails):
        active_session_id = execution_details.active_session_id
    elif execution_details is not None:
        raise ValueError(
            f"CurationTask {task_id} has non-grid execution details "
            f"(got {type(execution_details).__name__}); "
            "cannot get or create a grid for it."
        )

    # Step 3: a session is attached, so get that Grid from Synapse and return it.
    if active_session_id:
        client.logger.info(
            f"CurationTask {task_id} already has grid session {active_session_id}; "
            "attempting to get the existing grid."
        )
        grid = Grid(session_id=active_session_id).get(synapse_client=client)
        client.logger.info(f"Got grid session {active_session_id}.")
        return grid

    # Step 4: no session attached, so create one, attach it, and return it.
    client.logger.info(
        f"CurationTask {task_id} has no attached grid session; "
        "attempting to create one."
    )
    grid = task.create_grid_session(
        owner_principal_id=owner_principal_id,
        timeout=timeout,
        synapse_client=client,
    )
    client.logger.info(f"Created grid session for CurationTask {task_id}.")
    return grid
