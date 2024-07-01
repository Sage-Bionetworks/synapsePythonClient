import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Optional, Union

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError
from synapseclient.models import Annotations

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project, Table


class FailureStrategy(Enum):
    """
    When storing a large number of items through bulk actions like
    `Project(id="syn123").store()` or `Folder(id="syn456").store()` individual failures
    may occur. Passing this ENUM will allow you to define how you want to respond to
    failures.
    """

    RAISE_EXCEPTION = "RAISE_EXCEPTION"
    """An exception is raised on the first failure and all tasks yet to be completed
    are cancelled. The exception will also be logged."""

    LOG_EXCEPTION = "LOG_EXCEPTION"
    """An exception is logged and all tasks yet to be completed continue to be
    processed."""


async def wrap_coroutine(
    task: asyncio.Task, *, synapse_client: Optional[Synapse] = None
):
    """
    Wrapper to handle exceptions in async tasks. By default as_completed will cause
    sibiling tasks to be cancelled if one fails. This wrapper will catch the exception
    and log it, allowing the other tasks to continue.
    """
    try:
        return await task
    except Exception as ex:
        Synapse.get_client(synapse_client=synapse_client).logger.exception(ex)
        return ex


async def store_entity_components(
    root_resource: Union["File", "Folder", "Project", "Table"],
    failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
    *,
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

    tasks = []

    if hasattr(root_resource, "files") and root_resource.files is not None:
        for file in root_resource.files:
            tasks.append(
                asyncio.create_task(
                    file.store_async(
                        parent=root_resource, synapse_client=synapse_client
                    )
                )
            )

    if hasattr(root_resource, "folders") and root_resource.folders is not None:
        for folder in root_resource.folders:
            tasks.append(
                asyncio.create_task(
                    folder.store_async(
                        parent=root_resource, synapse_client=synapse_client
                    )
                )
            )

    tasks.append(
        asyncio.create_task(
            _store_activity_and_annotations(
                root_resource, synapse_client=synapse_client
            )
        )
    )

    try:
        tasks = [wrap_coroutine(task) for task in tasks]
        for task in asyncio.as_completed(tasks):
            result = await task
            _resolve_store_task(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )
    except Exception as ex:
        Synapse.get_client(synapse_client=synapse_client).logger.exception(ex)
        if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
            raise ex

    return re_read_required


def _resolve_store_task(
    result: Union[bool, "Folder", "File", BaseException],
    failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
    *,
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """
    Handle the result of a store task to Synapse depending on the failure strategy.

    Arguments:
        result: The result of the store task.
        failure_strategy: The failure strategy to use.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        If a read from Synapse is required to retireve the current state of the entity.

    Raises:
        If the failure strategy is to raise exceptions and the result is or causes
            an exception.
    """
    re_read_required = False
    if isinstance(result, bool):
        re_read_required = re_read_required or result
    elif result.__class__.__name__ == "Folder":
        pass
    elif result.__class__.__name__ == "File":
        pass
    else:
        if isinstance(result, BaseException):
            Synapse.get_client(synapse_client=synapse_client).logger.exception(result)
            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise result
        else:
            exception = SynapseError(
                f"Unknown failure saving entity components: {type(result)}",
                result,
            )
            Synapse.get_client(synapse_client=synapse_client).logger.exception(
                exception
            )
            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise exception
    return re_read_required


def _has_activity_change_to_apply(
    root_resource: Union["File", "Folder", "Project", "Table"],
    last_persistent_instance: Union["File", "Folder", "Project", "Table"],
) -> bool:
    """Determines if there is a change on the Activity to apply to the root_resource.

    Arguments:
        root_resource: The root resource.
        last_persistent_instance: The last persistent instance of the root resource.

    Returns:
        If the activity should be pulled forward.
    """
    return last_persistent_instance is None or (
        (last_persistent_instance.activity != root_resource.activity)
        or _pull_activity_forward_to_new_version(
            root_resource=root_resource,
            last_persistent_instance=last_persistent_instance,
        )
    )


def _pull_activity_forward_to_new_version(
    root_resource: Union["File", "Folder", "Project", "Table"],
    last_persistent_instance: Union["File", "Folder", "Project", "Table"],
) -> bool:
    """Determine if there was a version update on the root_resource, and if so it
    determines if we should be pulling the activity forward onto a new version.

    Arguments:
        root_resource: The root resource.
        last_persistent_instance: The last persistent instance of the root resource.

    Returns:
        If the activity should be pulled forward.
    """
    return (
        hasattr(root_resource, "associate_activity_to_new_version")
        and root_resource.associate_activity_to_new_version
        and last_persistent_instance.version_number != root_resource.version_number
    )


async def _store_activity_and_annotations(
    root_resource: Union["File", "Folder", "Project", "Table"],
    *,
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """
    Function to store ancillary activity and annotations of an entity to synapse.
    This function is split off from the main store_entity_components function because
    of 2 reasons:

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
        hasattr(root_resource, "annotations")
        and (root_resource.annotations or last_persistent_instance)
        and (
            last_persistent_instance is None
            or last_persistent_instance.annotations != root_resource.annotations
        )
    ):
        result = await Annotations(
            id=root_resource.id,
            etag=root_resource.etag,
            annotations=root_resource.annotations,
        ).store_async(synapse_client=synapse_client)

        root_resource.annotations = result.annotations
        root_resource.etag = result.etag

    if (
        hasattr(root_resource, "activity")
        and root_resource.activity is not None
        and _has_activity_change_to_apply(
            root_resource=root_resource,
            last_persistent_instance=last_persistent_instance,
        )
    ):
        result = await root_resource.activity.store_async(
            parent=root_resource, synapse_client=synapse_client
        )
        root_resource.activity = result

        return True
    return False
