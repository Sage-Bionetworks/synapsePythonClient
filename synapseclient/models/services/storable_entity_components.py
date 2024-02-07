import asyncio
from typing import Union, TYPE_CHECKING, Optional
from synapseclient import Synapse
from synapseclient.models import Annotations

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project, Table


async def store_entity_components(
    root_resource: Union["File", "Folder", "Project", "Table"],
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """
    Function to store ancillary components of an entity to synapse. This function will
    execute the stores in parallel.

    This is responsible for storing the annotations, activity, files and folders of a
    resource to synapse.


    Arguments:
        root_resource: The root resource to store objects on.
        synapse_client: If not passed in or None this will use the last client from the `.login()` method.

    Returns:
        If a read from Synapse is required to retireve the current state of the entity.
    """
    re_read_required = False
    # pylint: disable=protected-access
    last_persistent_instance = (
        root_resource._last_persistent_instance
        if hasattr(root_resource, "_last_persistent_instance")
        else None
    )
    tasks = []

    if hasattr(root_resource, "files") and root_resource.files is not None:
        for file in root_resource.files:
            tasks.append(
                asyncio.create_task(
                    file.store(parent=root_resource, synapse_client=synapse_client)
                )
            )

    if hasattr(root_resource, "folders") and root_resource.folders is not None:
        for folder in root_resource.folders:
            tasks.append(
                asyncio.create_task(
                    folder.store(parent=root_resource, synapse_client=synapse_client)
                )
            )

    if (
        hasattr(root_resource, "annotations")
        and root_resource.annotations is not None
        and (
            last_persistent_instance is None
            or last_persistent_instance.annotations != root_resource.annotations
        )
    ):
        tasks.append(
            asyncio.create_task(
                Annotations(
                    id=root_resource.id,
                    etag=root_resource.etag,
                    annotations=root_resource.annotations,
                ).store(synapse_client=synapse_client)
            )
        )

    try:
        for task in asyncio.as_completed(tasks):
            result = await task
            if isinstance(result, Annotations):
                root_resource.annotations = result.annotations
                root_resource.etag = result.etag
            elif result.__class__.__name__ == "Folder":
                pass
            elif result.__class__.__name__ == "File":
                pass
            else:
                if isinstance(result, BaseException):
                    Synapse.get_client(synapse_client=synapse_client).logger.exception(
                        result
                    )
                    raise result
                raise ValueError(f"Unknown type: {type(result)}", result)
    except Exception as ex:
        Synapse.get_client(synapse_client=synapse_client).logger.exception(ex)
        raise ex

    re_read_required = await _store_activity(
        root_resource, synapse_client=synapse_client
    )

    return re_read_required


async def _store_activity(
    root_resource: Union["File", "Folder", "Project", "Table"],
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """
    Function to store ancillary activity of an entity to synapse. This function is split
    off from the main store_entity_components function because of 2 reasons:
    1) It is not possible to concurrently store annotations and activity because both
        intend that the latest etag is used to store the entity.
    2) The activity endpoints do not return the etag of the entity, so the etag of the
        entity has to be retrieved later on with another GET request.

    Jira: https://sagebionetworks.jira.com/browse/PLFM-8251 has been created to review
    this logic.

    Arguments:
        root_resource: The root resource to store objects on.
        synapse_client: If not passed in or None this will use the last client from the `.login()` method.

    Returns:
        If a read from Synapse is required to retireve the current state of the entity.
    """
    # pylint: disable=protected-access
    last_persistent_instance = (
        root_resource._last_persistent_instance
        if hasattr(root_resource, "_last_persistent_instance")
        else None
    )

    if (
        hasattr(root_resource, "activity")
        and root_resource.activity is not None
        and (
            last_persistent_instance is None
            or last_persistent_instance.activity != root_resource.activity
        )
    ):
        result = await root_resource.activity.store(
            parent=root_resource, synapse_client=synapse_client
        )

        root_resource.activity = result
        return True
    return False
