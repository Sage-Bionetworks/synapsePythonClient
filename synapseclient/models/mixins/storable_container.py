"""Mixin for objects that can have Folders and Files stored in them."""

import asyncio
import os
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from opentelemetry import context
from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import get_entity_id_bundle2
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import (
    FILE_ENTITY,
    FOLDER_ENTITY,
    LINK_ENTITY,
)
from synapseclient.core.constants.method_flags import COLLISION_OVERWRITE_LOCAL
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.utils import run_and_attach_otel_context
from synapseclient.models.protocols.storable_container_protocol import (
    StorableContainerSynchronousProtocol,
)
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    wrap_coroutine,
)

if TYPE_CHECKING:
    from synapseclient.models import File, Folder


@async_to_sync
class StorableContainer(StorableContainerSynchronousProtocol):
    """
    Mixin for objects that can have Folders and Files stored in them.

    In order to use this mixin, the class must have the following attributes:

    - `id`
    - `files`
    - `folders`
    - `_last_persistent_instance`

    The class must have the following method:

    - `get`
    """

    id: None = None
    name: None = None
    files: "File" = None
    folders: "Folder" = None
    _last_persistent_instance: None = None

    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__.__name__}_sync_from_synapse: {self.id}"
    )
    async def sync_from_synapse_async(
        self: Self,
        path: Optional[str] = None,
        recursive: bool = True,
        download_file: bool = True,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Self:
        """
        Sync this container and all possible sub-folders from Synapse. By default this
        will download the files that are found and it will populate the
        `files` and `folders` attributes with the found files and folders. If you only
        want to retrieve the full tree of metadata about your container specify
        `download_file` as False.

        This works similar to [synapseutils.syncFromSynapse][], however, this does not
        currently support the writing of data to a manifest TSV file. This will be a
        future enhancement.

        Only Files and Folders are supported at this time to be synced from synapse.

        Arguments:
            path: An optional path where the file hierarchy will be reproduced. If not
                specified the files will by default be placed in the synapseCache.
            recursive: Whether or not to recursively get the entire hierarchy of the
                folder and sub-folders.
            download_file: Whether to download the files found or not.
            if_collision: Determines how to handle file collisions. May be

                - `overwrite.local`
                - `keep.local`
                - `keep.both`
            failure_strategy: Determines how to handle failures when retrieving children
                under this Folder and an exception occurs.
            include_activity: Whether to include the activity of the files.
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            link_hops: The number of hops to follow the link. A number of 1 is used to
                prevent circular references. There is nothing in place to prevent
                infinite loops. Be careful if setting this above 1.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The object that was called on. This will be the same object that was called on
                to start the sync.

        Example: Using this function
            Suppose I want to walk the immediate children of a folder without downloading the files:

                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                my_folder = Folder(id="syn12345")
                await my_folder.sync_from_synapse_async(download_file=False, recursive=False)

                for folder in my_folder.folders:
                    print(folder.name)

                for file in my_folder.files:
                    print(file.name)

            Suppose I want to download the immediate children of a folder:

                from synapseclient import Synapse
                from synapseclient.models import Folder

                syn = Synapse()
                syn.login()

                my_folder = Folder(id="syn12345")
                await my_folder.sync_from_synapse_async(path="/path/to/folder", recursive=False)

                for folder in my_folder.folders:
                    print(folder.name)

                for file in my_folder.files:
                    print(file.name)


            Suppose I want to download the immediate all children of a Project and all sub-folders and files:

                from synapseclient import Synapse
                from synapseclient.models import Project

                syn = Synapse()
                syn.login()

                my_project = Project(id="syn12345")
                await my_project.sync_from_synapse_async(path="/path/to/folder")


        Raises:
            ValueError: If the folder does not have an id set.


        A sequence diagram for this method is as follows:

        ```mermaid
        sequenceDiagram
            autonumber
            participant project_or_folder
            activate project_or_folder
            project_or_folder->>sync_from_synapse: Recursive search and download files
            activate sync_from_synapse
                opt Current instance not retrieved from Synapse
                    sync_from_synapse->>project_or_folder: Call `.get()` method
                    project_or_folder-->>sync_from_synapse: .
                end

                loop For each return of the generator
                    sync_from_synapse->>client: call `.getChildren()` method
                    client-->>sync_from_synapse: .
                    note over sync_from_synapse: Append to a running list
                end

                loop For each child
                    note over sync_from_synapse: Create all `pending_tasks` at current depth

                    alt Child is File
                        note over sync_from_synapse: Append `file.get()` method
                    else Child is Folder
                        note over sync_from_synapse: Append `folder.get()` method
                        alt Recursive is True
                            note over sync_from_synapse: Append `folder.sync_from_synapse()` method
                        end
                    else Child is Link and hops > 0
                        note over sync_from_synapse: Append task to follow link
                    end
                end

                loop For each task in pending_tasks
                    par `file.get()`
                        sync_from_synapse->>File: Retrieve File metadata and Optionally download
                        File->>client: `.get()`
                        client-->>File: .
                        File-->>sync_from_synapse: .
                    and `folder.get()`
                        sync_from_synapse->>Folder: Retrieve Folder metadataa
                        Folder->>client: `.get()`
                        client-->>Folder: .
                        Folder-->>sync_from_synapse: .
                    and `folder.sync_from_synapse_async()`
                        note over sync_from_synapse: This is a recursive call to `sync_from_synapse`
                        sync_from_synapse->>sync_from_synapse: Recursive call to `.sync_from_synapse_async()`
                    and `_follow_link`
                        sync_from_synapse ->>client: call `get_entity_id_bundle2` function
                        client-->sync_from_synapse: .
                        note over sync_from_synapse: Do nothing if not link
                        note over sync_from_synapse: call `_create_task_for_child` and execute
                    end
                end

            deactivate sync_from_synapse
            deactivate project_or_folder
        ```

        """
        if not self._last_persistent_instance:
            await self.get_async(synapse_client=synapse_client)
        Synapse.get_client(synapse_client=synapse_client).logger.info(
            f"Syncing {self.__class__.__name__} ({self.id}:{self.name}) from Synapse."
        )
        path = os.path.expanduser(path) if path else None

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        children = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: self._retrieve_children(
                    follow_link=follow_link,
                    synapse_client=synapse_client,
                ),
                current_context,
            ),
        )

        pending_tasks = []
        self.folders = []
        self.files = []

        for child in children:
            pending_tasks.extend(
                self._create_task_for_child(
                    child=child,
                    recursive=recursive,
                    path=path,
                    download_file=download_file,
                    if_collision=if_collision,
                    failure_strategy=failure_strategy,
                    synapse_client=synapse_client,
                    include_activity=include_activity,
                    follow_link=follow_link,
                    link_hops=link_hops,
                )
            )

        for task in asyncio.as_completed(pending_tasks):
            result = await task
            self._resolve_sync_from_synapse_result(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )
        return self

    def flatten_file_list(self) -> List["File"]:
        """
        Recursively loop over all of the already retrieved files and folders and return
        a list of all files in the container.

        Returns:
            A list of all files in the container.
        """
        files = []
        for file in self.files:
            files.append(file)
        for folder in self.folders:
            files.extend(folder.flatten_file_list())
        return files

    def map_directory_to_all_contained_files(
        self, root_path: str
    ) -> Dict[str, List["File"]]:
        """
        Recursively loop over all of the already retrieved files and folders. Then
        return back a dictionary where the key is the path to the directory at each
        level. The value is a list of all files in that directory AND all files in
        the child directories.

        This is used during the creation of the manifest TSV file during the
        syncFromSynapse utility function.

        Example: Using this function
           Returning back a dict with 2 keys:

                Given:
                root_folder
                ├── sub_folder
                │   ├── file1.txt
                │   └── file2.txt

                Returns:
                {
                    "root_folder": [file1, file2],
                    "root_folder/sub_folder": [file1, file2]
                }


           Returning back a dict with 3 keys:

                Given:
                root_folder
                ├── sub_folder_1
                │   ├── file1.txt
                ├── sub_folder_2
                │   └── file2.txt

                Returns:
                {
                    "root_folder": [file1, file2],
                    "root_folder/sub_folder_1": [file1]
                    "root_folder/sub_folder_2": [file2]
                }

        Arguments:
            root_path: The root path where the top level files are stored.

        Returns:
            A dictionary where the key is the path to the directory at each level. The
                value is a list of all files in that directory AND all files in the child
                directories.
        """
        directory_map = {}
        directory_map.update({root_path: self.flatten_file_list()})

        for folder in self.folders:
            directory_map.update(
                **folder.map_directory_to_all_contained_files(
                    root_path=os.path.join(root_path, folder.name)
                )
            )

        return directory_map

    def _retrieve_children(
        self,
        follow_link: bool,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List:
        """
        This wraps the `getChildren` generator to return back a list of children.

        Arguments:
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.
        """
        include_types = ["folder", "file"]
        if follow_link:
            include_types.append("link")
        children_objects = Synapse.get_client(
            synapse_client=synapse_client
        ).getChildren(
            parent=self.id,
            includeTypes=include_types,
        )
        children = []
        for child in children_objects:
            children.append(child)
        return children

    async def _wrap_recursive_get_children(
        self,
        folder: "Folder",
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Wrap the recursive get children method to return nothing. We are updating
        the folder object in place. We do not want to cause the result of this
        method to cause any folder of file objects to be added to this level of the
        hierarchy.
        """
        new_resolved_path = (
            os.path.join(path, folder.name) if path and folder.name else None
        )
        if new_resolved_path and not os.path.exists(new_resolved_path):
            os.makedirs(new_resolved_path)
        await folder.sync_from_synapse_async(
            recursive=recursive,
            download_file=download_file,
            path=new_resolved_path,
            if_collision=if_collision,
            failure_strategy=failure_strategy,
            include_activity=include_activity,
            follow_link=follow_link,
            link_hops=link_hops,
            synapse_client=synapse_client,
        )

    def _create_task_for_child(
        self,
        child,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List[asyncio.Task]:
        """
        Determines based off the type of child which tasks should be created to handle
        the child. This will return a list of tasks that will be executed in parallel
        to handle the child. The tasks will retrieve the File and Folder objects from
        Synapse. In the case of a Folder object, it will also retrieve the children of
        that folder if `recursive` is set to True.


        Arguments:
            recursive: Whether or not to recursively get the entire hierarchy of the
                folder and sub-folders.
            download_file: Whether to download the files found or not.
            path: An optional path where the file hierarchy will be reproduced. If not
                specified the files will by default be placed in the synapseCache.
            if_collision: Determines how to handle file collisions. May be

                - `overwrite.local`
                - `keep.local`
                - `keep.both`
            failure_strategy: Determines how to handle failures when retrieving children
                under this Folder and an exception occurs.
            include_activity: Whether to include the activity of the files.
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            link_hops: The number of hops to follow the link. A number of 1 is used to
                prevent circular references. There is nothing in place to prevent
                infinite loops. Be careful if setting this above 1.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        """

        pending_tasks = []
        synapse_id = child.get("id", None)
        child_type = child.get("type", None)
        name = child.get("name", None)
        if synapse_id and child_type == FOLDER_ENTITY:
            # Lazy import to avoid circular import
            from synapseclient.models import Folder

            folder = Folder(id=synapse_id, name=name)
            self.folders.append(folder)

            if recursive:
                pending_tasks.append(
                    asyncio.create_task(
                        self._wrap_recursive_get_children(
                            folder=folder,
                            recursive=recursive,
                            path=path,
                            download_file=download_file,
                            if_collision=if_collision,
                            failure_strategy=failure_strategy,
                            include_activity=include_activity,
                            follow_link=follow_link,
                            link_hops=link_hops,
                            synapse_client=synapse_client,
                        )
                    )
                )
            else:
                pending_tasks.append(
                    asyncio.create_task(wrap_coroutine(folder.get_async()))
                )

        elif synapse_id and child_type == FILE_ENTITY:
            # Lazy import to avoid circular import
            from synapseclient.models import File

            file = File(id=synapse_id, name=name, download_file=download_file)
            self.files.append(file)
            if path:
                file.path = path
            if if_collision:
                file.if_collision = if_collision

            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(file.get_async(include_activity=include_activity))
                )
            )
        elif link_hops > 0 and synapse_id and child_type == LINK_ENTITY:
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(
                        self._follow_link(
                            child=child,
                            recursive=recursive,
                            path=path,
                            download_file=download_file,
                            if_collision=if_collision,
                            failure_strategy=failure_strategy,
                            synapse_client=synapse_client,
                            include_activity=include_activity,
                            follow_link=follow_link,
                            link_hops=link_hops - 1,
                        )
                    )
                )
            )

        return pending_tasks

    async def _follow_link(
        self,
        child,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Follow a link to get a target entity.

        Arguments in this function are all supplied in order to recursively traverse
        the container hierarchy.

        Returns:
            None
        """

        synapse_id = child.get("id", None)
        # TODO: Until Link is an official Model dataclass this logic will suffice for
        # the purpose of following a link to potentially download a File or open another
        # Folder.
        entity_bundle = await get_entity_id_bundle2(
            entity_id=synapse_id,
            request={"includeEntity": True},
            synapse_client=synapse_client,
        )

        if (
            entity_bundle is None
            or not (entity := entity_bundle.get("entity", None))
            or not (links_to := entity.get("linksTo", None))
            or not (link_class_name := entity.get("linksToClassName", None))
            or not (link_target_id := links_to.get("targetId", None))
        ):
            return

        pending_tasks = self._create_task_for_child(
            child={
                "id": link_target_id,
                "type": link_class_name,
            },
            recursive=recursive,
            path=path,
            download_file=download_file,
            if_collision=if_collision,
            failure_strategy=failure_strategy,
            include_activity=include_activity,
            follow_link=follow_link,
            link_hops=link_hops,
            synapse_client=synapse_client,
        )
        for task in asyncio.as_completed(pending_tasks):
            result = await task
            self._resolve_sync_from_synapse_result(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )

    def _resolve_sync_from_synapse_result(
        self,
        result: Union[None, "Folder", "File", BaseException],
        failure_strategy: FailureStrategy,
        *,
        synapse_client: Union[None, Synapse],
    ) -> None:
        """
        Handle what to do based on what was returned from the latest task to complete.
        We are updating the object in place and appending the returned Folder/Files to
        the appropriate list.

        Arguments:
            result: The result of the task that was completed.
            failure_strategy: Determines how to handle failures when retrieving children
                under this Folder and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.
        """
        if result is None:
            # We will recieve None when executing `_wrap_recursive_get_children` as
            # it will internally be recursively calling this method and setting the
            # appropriate folder/file objects in place.
            pass
        elif (
            result.__class__.__name__ == "Folder" or result.__class__.__name__ == "File"
        ):
            # Do nothing as the objects are updated in place and the container has
            # already been updated to append the new objects.
            pass
        elif isinstance(result, BaseException):
            Synapse.get_client(synapse_client=synapse_client).logger.exception(result)

            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise result
        else:
            exception = SynapseError(
                f"Unknown failure retrieving children of Folder ({self.id}): {type(result)}",
                result,
            )
            Synapse.get_client(synapse_client=synapse_client).logger.exception(
                exception
            )
            if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                raise exception
