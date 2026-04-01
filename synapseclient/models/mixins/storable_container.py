"""Mixin for objects that can have Folders and Files stored in them."""

import asyncio
import os
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Dict,
    Generator,
    List,
    NoReturn,
    Optional,
    Tuple,
    Union,
)

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import get_entity_id_bundle2
from synapseclient.api.entity_services import EntityHeader, get_children
from synapseclient.core.async_utils import (
    async_to_sync,
    otel_trace_method,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import (
    DATASET_COLLECTION_ENTITY,
    DATASET_ENTITY,
    ENTITY_VIEW,
    FILE_ENTITY,
    FOLDER_ENTITY,
    LINK_ENTITY,
    MATERIALIZED_VIEW,
    PROJECT_ENTITY,
    SUBMISSION_VIEW,
    TABLE_ENTITY,
    VIRTUAL_TABLE,
)
from synapseclient.core.constants.method_flags import COLLISION_OVERWRITE_LOCAL
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.transfer_bar import shared_download_progress_bar
from synapseclient.models.protocols.storable_container_protocol import (
    StorableContainerSynchronousProtocol,
)
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    wrap_coroutine,
)

if TYPE_CHECKING:
    # TODO: Support DockerRepo and Link in https://sagebionetworks.jira.com/browse/SYNPY-1343 epic or later
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        MaterializedView,
        SubmissionView,
        Table,
        VirtualTable,
    )


@async_to_sync
class StorableContainer(StorableContainerSynchronousProtocol):
    """
    Mixin for objects that can have Folders and Files stored in them.

    In order to use this mixin, the class must have the following attributes:

    - `id`
    - `files`
    - `folders`
    - `tables`
    - `entityviews`
    - `submissionviews`
    - `datasets`
    - `datasetcollections`
    - `materializedviews`
    - `virtualtables`
    - `_last_persistent_instance`
    - `_synced_from_synapse`

    The class must have the following method:

    - `get`
    """

    id: None = None
    name: None = None
    files: List["File"] = None
    folders: List["Folder"] = None
    tables: List["Table"] = None
    # links: List["Link"] = None
    entityviews: List["EntityView"] = None
    # dockerrepos: List["DockerRepo"] = None
    submissionviews: List["SubmissionView"] = None
    datasets: List["Dataset"] = None
    datasetcollections: List["DatasetCollection"] = None
    materializedviews: List["MaterializedView"] = None
    virtualtables: List["VirtualTable"] = None
    _last_persistent_instance: None = None
    _synced_from_synapse: bool = False

    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    async def _worker(
        self,
        queue: asyncio.Queue,
        failure_strategy: FailureStrategy,
        synapse_client: Synapse,
    ) -> NoReturn:
        """
        Coroutine that will process the queue of work items. This will process the
        work items until the queue is empty. This will be used to download files in
        parallel.

        Arguments:
            queue: The queue of work items to process.
            failure_strategy: Determines how to handle failures when retrieving items
                out of the queue and an exception occurs.
            synapse_client: The Synapse client to use to download the files.
        """
        while True:
            # Get a "work item" out of the queue.
            work_item = await queue.get()

            try:
                result = await work_item
            except asyncio.CancelledError as ex:
                raise ex
            except Exception as ex:
                result = ex

            self._resolve_sync_from_synapse_result(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=synapse_client,
            )

            queue.task_done()

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
        queue: asyncio.Queue = None,
        include_types: Optional[List[str]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Self:
        """
        Sync this container and all possible sub-folders from Synapse. By default this
        will download the files that are found and it will populate the
        `files` and `folders` attributes with the found files and folders, along with
        all other entity types (tables, entityviews, etc.) present in the container.
        If you only want to retrieve the full tree of metadata about your
        container specify `download_file` as False.

        This works similar to [synapseutils.syncFromSynapse][], however, this does not
        currently support the writing of data to a manifest TSV file. This will be a
        future enhancement.

        Supports syncing Files, Folders, Tables, EntityViews, SubmissionViews, Datasets,
        DatasetCollections, MaterializedViews, and VirtualTables from Synapse. The
        metadata for these entity types will be populated in their respective
        attributes (`files`, `folders`, `tables`, `entityviews`, `submissionviews`,
        `datasets`, `datasetcollections`, `materializedviews`, `virtualtables`) if
        they are found within the container.

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
            queue: An optional queue to use to download files in parallel.
            include_types: Must be a list of entity types (ie. ["folder","file"]) which
                can be found
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html).
                Defaults to
                `["folder", "file", "table", "entityview", "dockerrepo",
                "submissionview", "dataset", "datasetcollection", "materializedview",
                "virtualtable"]`.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The object that was called on. This will be the same object that was called on
                to start the sync.

        Example: Using this function
            Suppose I want to walk the immediate children of a folder without downloading the files:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            async def my_function():
                syn = Synapse()
                syn.login()

                my_folder = Folder(id="syn12345")
                await my_folder.sync_from_synapse_async(download_file=False, recursive=False)

                for folder in my_folder.folders:
                    print(folder.name)

                for file in my_folder.files:
                    print(file.name)

                for table in my_folder.tables:
                    print(table.name)

                for dataset in my_folder.datasets:
                    print(dataset.name)

            asyncio.run(my_function())
            ```

            Suppose I want to download the immediate children of a folder:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            async def my_function():
                syn = Synapse()
                syn.login()

                my_folder = Folder(id="syn12345")
                await my_folder.sync_from_synapse_async(path="/path/to/folder", recursive=False)

                for folder in my_folder.folders:
                    print(folder.name)

                for file in my_folder.files:
                    print(file.name)

            asyncio.run(my_function())
            ```

            Suppose I want to sync only specific entity types from a Project:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Project

            async def my_function():
                syn = Synapse()
                syn.login()

                my_project = Project(id="syn12345")
                await my_project.sync_from_synapse_async(
                    path="/path/to/folder",
                    include_types=["folder", "file", "table", "dataset"]
                )

                # Access different entity types
                for table in my_project.tables:
                    print(f"Table: {table.name}")

                for dataset in my_project.datasets:
                    print(f"Dataset: {dataset.name}")

            asyncio.run(my_function())
            ```

            Suppose I want to download all the children of a Project and all sub-folders and files:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Project

            async def my_function():
                syn = Synapse()
                syn.login()

                my_project = Project(id="syn12345")
                await my_project.sync_from_synapse_async(path="/path/to/folder")

            asyncio.run(my_function())
            ```


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
        syn = Synapse.get_client(synapse_client=synapse_client)
        custom_message = "Syncing from Synapse" if not download_file else None
        with shared_download_progress_bar(
            file_size=1, synapse_client=syn, custom_message=custom_message
        ):
            self._synced_from_synapse = True
            return await self._sync_from_synapse_async(
                path=path,
                recursive=recursive,
                download_file=download_file,
                if_collision=if_collision,
                failure_strategy=failure_strategy,
                include_activity=include_activity,
                follow_link=follow_link,
                link_hops=link_hops,
                queue=queue,
                include_types=include_types,
                synapse_client=syn,
            )

    async def _sync_from_synapse_async(
        self: Self,
        path: Optional[str] = None,
        recursive: bool = True,
        download_file: bool = True,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        queue: asyncio.Queue = None,
        include_types: Optional[List[str]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Self:
        """Function wrapped by sync_from_synapse_async in order to allow a context
        manager to be used to handle the progress bar.

        All arguments are passed through from the wrapper function.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)
        if not self._last_persistent_instance:
            await self.get_async(synapse_client=syn)
        syn.logger.info(
            f"[{self.id}:{self.name}]: Syncing {self.__class__.__name__} from Synapse."
        )
        path = os.path.expanduser(path) if path else None

        children = await self._retrieve_children(
            follow_link=follow_link,
            include_types=include_types,
            synapse_client=syn,
        )

        create_workers = not queue

        queue = queue or asyncio.Queue()
        worker_tasks = []
        if create_workers:
            for _ in range(max(syn.max_threads * 2, 1)):
                task = asyncio.create_task(
                    self._worker(
                        queue=queue,
                        failure_strategy=failure_strategy,
                        synapse_client=syn,
                    )
                )
                worker_tasks.append(task)

        pending_tasks = []
        self.folders = []
        self.files = []
        self.tables = []
        self.entityviews = []
        self.submissionviews = []
        self.datasets = []
        self.datasetcollections = []
        self.materializedviews = []
        self.virtualtables = []

        for child in children:
            pending_tasks.extend(
                self._create_task_for_child(
                    child=child,
                    recursive=recursive,
                    path=path,
                    download_file=download_file,
                    if_collision=if_collision,
                    failure_strategy=failure_strategy,
                    synapse_client=syn,
                    include_activity=include_activity,
                    follow_link=follow_link,
                    link_hops=link_hops,
                    queue=queue,
                    include_types=include_types,
                )
            )

        for task in asyncio.as_completed(pending_tasks):
            result = await task
            self._resolve_sync_from_synapse_result(
                result=result,
                failure_strategy=failure_strategy,
                synapse_client=syn,
            )

        if create_workers:
            try:
                # Wait until the queue is fully processed.
                await queue.join()
            finally:
                for task in worker_tasks:
                    task.cancel()

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

    @skip_async_to_sync
    async def walk_async(
        self,
        follow_link: bool = False,
        include_types: Optional[List[str]] = None,
        recursive: bool = True,
        display_ascii_tree: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        _newpath: Optional[str] = None,
        _tree_prefix: str = "",
        _is_last_in_parent: bool = True,
        _tree_depth: int = 0,
    ) -> AsyncGenerator[
        Tuple[Tuple[str, str], List[EntityHeader], List[EntityHeader]], None
    ]:
        """
        Traverse through the hierarchy of entities stored under this container.
        Mimics os.walk() behavior but yields EntityHeader objects, with optional
        ASCII tree display that continues printing as the walk progresses.

        Arguments:
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            include_types: Must be a list of entity types (ie. ["folder","file"]) which
                can be found
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html).
                Defaults to
                `["folder", "file", "table", "entityview", "dockerrepo",
                "submissionview", "dataset", "datasetcollection", "materializedview",
                "virtualtable"]`. The "folder" type is always included so the hierarchy
                can be traversed.
            recursive: Whether to recursively traverse subdirectories. Defaults to True.
            display_ascii_tree: If True, display an ASCII tree representation as the
                container structure is traversed. Tree lines are printed incrementally
                as each container is visited. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
            _newpath: Used internally to track the current path during recursion.
            _tree_prefix: Used internally to format the ASCII tree structure.
            _is_last_in_parent: Used internally to determine if the current entity is
                the last child in its parent.
            _tree_depth: Used internally to track the current depth in the tree.

        Yields:
            Tuple of (dirpath, dirs, nondirs) where:

                - dirpath: Tuple (directory_name, synapse_id) representing current directory
                - dirs: List of EntityHeader objects for subdirectories (folders)
                - nondirs: List of EntityHeader objects for non-directory entities (files, tables, etc.)

        Example: Traverse all entities in a container
            Basic usage - traverse all entities in a container

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            async def my_function():
                syn = Synapse()
                syn.login()

                container = Folder(id="syn12345")
                async for dirpath, dirs, nondirs in container.walk_async():
                    print(f"Directory: {dirpath[0]} ({dirpath[1]})")

                    # Print folders
                    for folder_entity in dirs:
                        print(f"  Folder: {folder_entity}")

                    # Print files and other entities
                    for entity in nondirs:
                        print(f"  File: {entity}")

            asyncio.run(my_function())
            ```

        Example: Display progressive ASCII tree as walk proceeds
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            async def my_function():
                syn = Synapse()
                syn.login()

                container = Folder(id="syn12345")
                # Display tree structure progressively as walk proceeds
                async for dirpath, dirs, nondirs in container.walk_async(
                    display_ascii_tree=True
                ):
                    # Process each directory as usual
                    print(f"Processing: {dirpath[0]}")
                    for file_entity in nondirs:
                        print(f"  Found file: {file_entity.name}")

            asyncio.run(my_function())
            ```

            Example output:

            ```
            === Container Structure ===
            📂  my-container-name (syn52948289) [Project]
            ├── 📁  bulk-upload (syn68884548) [Folder]
            │   └── 📁  file_script_folder (syn68884547) [Folder]
            │       └── 📁  file_script_sub_folder (syn68884549) [Folder]
            │           └── 📄 file_in_a_sub_folder.txt (syn68884556) [File]
            ├── 📁  root (syn67590143) [Folder]
            │   └── 📁  subdir1 (syn67590144) [Folder]
            │       ├── 📄 file1.txt (syn67590261) [File]
            │       └── 📄 file2.txt (syn67590287) [File]
            └── 📁  temp-files (syn68884954) [Folder]
                └── 📁  root (syn68884955) [Folder]
                    └── 📁  subdir1 (syn68884956) [Folder]
                        ├── 📄 file1.txt (syn68884959) [File]
                        └── 📄 file2.txt (syn68884999) [File]

            Directory: My uniquely named project about Alzheimer's Disease (syn53185532)
              File: EntityHeader(name='Gene Expression Data', id='syn66227753',
                type='org.sagebionetworks.repo.model.table.TableEntity', version_number=1,
                version_label='in progress', is_latest_version=True, benefactor_id=53185532,
                created_on='2025-04-11T21:24:28.913Z', modified_on='2025-04-11T21:24:34.996Z',
                created_by='3481671', modified_by='3481671')
              Folder: EntityHeader(name='Research Data', id='syn68327923',
                type='org.sagebionetworks.repo.model.Folder', version_number=1,
                version_label='1', is_latest_version=True, benefactor_id=68327923,
                created_on='2025-06-16T21:51:50.460Z', modified_on='2025-06-16T22:19:41.481Z',
                created_by='3481671', modified_by='3481671')
            ```

        Note:
            Each EntityHeader contains complete metadata including id, name, type,
            creation/modification dates, version information, and other Synapse properties.
            The directory path is built using os.path.join() to create hierarchical paths.

            When display_ascii_tree=True, the ASCII tree structure is displayed in proper
            hierarchical order as the entire structure is traversed sequentially. The tree
            will be printed in the correct parent-child relationships, but results will still
            be yielded as expected during the traversal process.
            like "my-container-name/bulk-upload/file_script_folder/file_script_sub_folder".
            When display_ascii_tree=True, the tree is printed progressively as each
            container is visited during the walk, making it suitable for very large
            and deeply nested structures.
        """
        if not self.id or not self.name:
            await self.get_async(synapse_client=synapse_client)
        if not include_types:
            include_types = [
                "folder",
                "file",
                "table",
                "entityview",
                "dockerrepo",
                "submissionview",
                "dataset",
                "datasetcollection",
                "materializedview",
                "virtualtable",
            ]
            if follow_link:
                include_types.append("link")
        else:
            if follow_link and "link" not in include_types:
                include_types.append("link")

        if _newpath is None:
            dirpath = (self.name, self.id)
        else:
            dirpath = (_newpath, self.id)

        all_children: List[EntityHeader] = []
        async for child in get_children(
            parent=self.id,
            include_types=include_types,
            synapse_client=synapse_client,
        ):
            converted_child = EntityHeader().fill_from_dict(synapse_response=child)
            all_children.append(converted_child)

        if display_ascii_tree and _newpath is None and _tree_depth == 0:
            client = Synapse.get_client(synapse_client=synapse_client)
            client.logger.info("=== Container Structure ===")

        if display_ascii_tree:
            client = Synapse.get_client(synapse_client=synapse_client)

            from synapseclient.models import Project

            current_entity = EntityHeader(
                id=self.id,
                name=self.name,
                type=PROJECT_ENTITY if isinstance(self, Project) else FOLDER_ENTITY,
            )

            if _tree_depth == 0:
                tree_line = self._format_entity_info_for_tree(entity=current_entity)
            else:
                connector = "└── " if _is_last_in_parent else "├── "
                entity_info = self._format_entity_info_for_tree(entity=current_entity)
                tree_line = f"{_tree_prefix}{connector}{entity_info}"

            client.logger.info(tree_line)

        nondirs = []
        dir_entities = []

        for child in all_children:
            if child.type in [
                FOLDER_ENTITY,
                PROJECT_ENTITY,
            ]:
                dir_entities.append(child)
            else:
                nondirs.append(child)

        if display_ascii_tree and nondirs:
            client = Synapse.get_client(synapse_client=synapse_client)

            if _tree_depth == 0:
                child_prefix = ""
            else:
                child_prefix = _tree_prefix + ("    " if _is_last_in_parent else "│   ")

            sorted_nondirs = sorted(nondirs, key=lambda x: x.name)

            for i, child in enumerate(sorted_nondirs):
                is_last_child = (i == len(sorted_nondirs) - 1) and (
                    len(dir_entities) == 0
                )
                connector = "└── " if is_last_child else "├── "
                entity_info = self._format_entity_info_for_tree(child)
                tree_line = f"{child_prefix}{connector}{entity_info}"
                client.logger.info(tree_line)

        # Yield the current directory's contents
        yield dirpath, dir_entities, nondirs

        if recursive and dir_entities:
            sorted_dir_entities: List[EntityHeader] = sorted(
                dir_entities, key=lambda x: x.name
            )

            if _tree_depth == 0:
                subdir_prefix = ""
            else:
                subdir_prefix = _tree_prefix + (
                    "    " if _is_last_in_parent else "│   "
                )

            # Process subdirectories SEQUENTIALLY to maintain tree structure
            for i, child_entity in enumerate(sorted_dir_entities):
                is_last_subdir = i == len(sorted_dir_entities) - 1

                new_dir_path = os.path.join(dirpath[0], child_entity.name)

                if child_entity.type == FOLDER_ENTITY:
                    from synapseclient.models import Folder

                    child_container = Folder(id=child_entity.id, name=child_entity.name)
                elif child_entity.type == PROJECT_ENTITY:
                    from synapseclient.models import Project

                    child_container = Project(
                        id=child_entity.id, name=child_entity.name
                    )
                else:
                    continue  # Skip non-container types

                async for result in child_container.walk_async(
                    follow_link=follow_link,
                    include_types=include_types,
                    recursive=recursive,
                    display_ascii_tree=display_ascii_tree,
                    _newpath=new_dir_path,
                    synapse_client=synapse_client,
                    _tree_prefix=subdir_prefix,
                    _is_last_in_parent=is_last_subdir,
                    _tree_depth=_tree_depth + 1,
                ):
                    yield result

    def walk(
        self,
        follow_link: bool = False,
        include_types: Optional[List[str]] = None,
        recursive: bool = True,
        display_ascii_tree: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        _newpath: Optional[str] = None,
        _tree_prefix: str = "",
        _is_last_in_parent: bool = True,
        _tree_depth: int = 0,
    ) -> Generator[
        Tuple[Tuple[str, str], List[EntityHeader], List[EntityHeader]], None, None
    ]:
        """
        Traverse through the hierarchy of entities stored under this container.
        Mimics os.walk() behavior but yields EntityHeader objects, with optional
        ASCII tree display that continues printing as the walk progresses.

        Arguments:
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            include_types: Must be a list of entity types (ie. ["folder","file"]) which
                can be found
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html).
                Defaults to
                `["folder", "file", "table", "entityview", "dockerrepo",
                "submissionview", "dataset", "datasetcollection", "materializedview",
                "virtualtable"]`. The "folder" type is always included so the hierarchy
                can be traversed.
            recursive: Whether to recursively traverse subdirectories. Defaults to True.
            display_ascii_tree: If True, display an ASCII tree representation as the
                container structure is traversed. Tree lines are printed incrementally
                as each container is visited. Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
            _newpath: Used internally to track the current path during recursion.
            _tree_prefix: Used internally to format the ASCII tree structure.
            _is_last_in_parent: Used internally to determine if the current entity is
                the last child in its parent.
            _tree_depth: Used internally to track the current depth in the tree.

        Yields:
            Tuple of (dirpath, dirs, nondirs) where:

                - dirpath: Tuple (directory_name, synapse_id) representing current directory
                - dirs: List of EntityHeader objects for subdirectories (folders)
                - nondirs: List of EntityHeader objects for non-directory entities (files, tables, etc.)

        Example: Traverse all entities in a container
            Basic usage - traverse all entities in a container

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            container = Folder(id="syn12345")
            for dirpath, dirs, nondirs in container.walk():
                print(f"Directory: {dirpath[0]} ({dirpath[1]})")

                # Print folders
                for folder_entity in dirs:
                    print(f"  Folder: {folder_entity}")

                # Print files and other entities
                for entity in nondirs:
                    print(f"  File: {entity}")
            ```

        Example: Display progressive ASCII tree as walk proceeds
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            container = Folder(id="syn12345")
            # Display tree structure progressively as walk proceeds
            for dirpath, dirs, nondirs in container.walk(
                display_ascii_tree=True
            ):
                # Process each directory as usual
                print(f"Processing: {dirpath[0]}")
                for file_entity in nondirs:
                    print(f"  Found file: {file_entity.name}")
            ```

            Example output:

            ```
            === Container Structure ===
            📂  my-container-name (syn52948289) [Project]
            ├── 📁  bulk-upload (syn68884548) [Folder]
            │   └── 📁  file_script_folder (syn68884547) [Folder]
            │       └── 📁  file_script_sub_folder (syn68884549) [Folder]
            │           └── 📄 file_in_a_sub_folder.txt (syn68884556) [File]
            ├── 📁  root (syn67590143) [Folder]
            │   └── 📁  subdir1 (syn67590144) [Folder]
            │       ├── 📄 file1.txt (syn67590261) [File]
            │       └── 📄 file2.txt (syn67590287) [File]
            └── 📁  temp-files (syn68884954) [Folder]
                └── 📁  root (syn68884955) [Folder]
                    └── 📁  subdir1 (syn68884956) [Folder]
                        ├── 📄 file1.txt (syn68884959) [File]
                        └── 📄 file2.txt (syn68884999) [File]

            Directory: My uniquely named project about Alzheimer's Disease (syn53185532)
              File: EntityHeader(name='Gene Expression Data', id='syn66227753',
                type='org.sagebionetworks.repo.model.table.TableEntity', version_number=1,
                version_label='in progress', is_latest_version=True, benefactor_id=53185532,
                created_on='2025-04-11T21:24:28.913Z', modified_on='2025-04-11T21:24:34.996Z',
                created_by='3481671', modified_by='3481671')
              Folder: EntityHeader(name='Research Data', id='syn68327923',
                type='org.sagebionetworks.repo.model.Folder', version_number=1,
                version_label='1', is_latest_version=True, benefactor_id=68327923,
                created_on='2025-06-16T21:51:50.460Z', modified_on='2025-06-16T22:19:41.481Z',
                created_by='3481671', modified_by='3481671')
            ```

        Note:
            Each EntityHeader contains complete metadata including id, name, type,
            creation/modification dates, version information, and other Synapse properties.
            The directory path is built using os.path.join() to create hierarchical paths.

            When display_ascii_tree=True, the ASCII tree structure is displayed in proper
            hierarchical order as the entire structure is traversed sequentially. The tree
            will be printed in the correct parent-child relationships, but results will still
            be yielded as expected during the traversal process.
            like "my-container-name/bulk-upload/file_script_folder/file_script_sub_folder".
            When display_ascii_tree=True, the tree is printed progressively as each
            container is visited during the walk, making it suitable for very large
            and deeply nested structures.
        """
        yield from wrap_async_generator_to_sync_generator(
            self.walk_async,
            follow_link=follow_link,
            include_types=include_types,
            recursive=recursive,
            display_ascii_tree=display_ascii_tree,
            synapse_client=synapse_client,
            _newpath=_newpath,
            _tree_prefix=_tree_prefix,
            _is_last_in_parent=_is_last_in_parent,
            _tree_depth=_tree_depth,
        )

    async def _retrieve_children(
        self,
        follow_link: bool,
        include_types: Optional[List[str]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List:
        """
        Retrieve children entities using the async get_children API.

        Arguments:
            follow_link: Whether to follow a link entity or not. Links can be used to
                point at other Synapse entities.
            include_types: Must be a list of entity types (ie. ["folder","file"]) which
                can be found
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html).
                Defaults to
                `["folder", "file", "table", "entityview", "dockerrepo",
                "submissionview", "dataset", "datasetcollection", "materializedview",
                "virtualtable"]`.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of child entities.
        """
        if not include_types:
            include_types = [
                "folder",
                "file",
                "table",
                "entityview",
                "dockerrepo",
                "submissionview",
                "dataset",
                "datasetcollection",
                "materializedview",
                "virtualtable",
            ]
            if follow_link:
                include_types.append("link")
        else:
            if follow_link and "link" not in include_types:
                include_types.append("link")

        children = []
        async for child in get_children(
            parent=self.id,
            include_types=include_types,
            synapse_client=synapse_client,
        ):
            children.append(child)
        return children

    async def _wrap_recursive_get_children(
        self,
        folder: "Folder",
        queue: asyncio.Queue,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        include_types: Optional[List[str]] = None,
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
        await folder._sync_from_synapse_async(
            recursive=recursive,
            download_file=download_file,
            path=new_resolved_path,
            if_collision=if_collision,
            failure_strategy=failure_strategy,
            include_activity=include_activity,
            follow_link=follow_link,
            link_hops=link_hops,
            synapse_client=synapse_client,
            queue=queue,
            include_types=include_types,
        )

    def _create_task_for_child(
        self,
        child,
        queue: asyncio.Queue,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 1,
        include_types: Optional[List[str]] = None,
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
            child: Child entity to build a task for
            queue: A queue to use to download files in parallel.
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
            include_types: Must be a list of entity types (ie. ["folder","file"]) which
                can be found
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html)
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

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
                            queue=queue,
                            include_types=include_types,
                        )
                    )
                )
            else:
                pending_tasks.append(
                    asyncio.create_task(
                        wrap_coroutine(folder.get_async(synapse_client=synapse_client))
                    )
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

            queue.put_nowait(
                wrap_coroutine(
                    file.get_async(
                        include_activity=include_activity,
                        synapse_client=synapse_client,
                    )
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
                            include_types=include_types,
                            queue=queue,
                        )
                    )
                )
            )
        elif synapse_id and child_type == TABLE_ENTITY:
            # Lazy import to avoid circular import
            from synapseclient.models import Table

            table = Table(id=synapse_id, name=name)
            self.tables.append(table)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(table.get_async(synapse_client=synapse_client))
                )
            )
        elif synapse_id and child_type == ENTITY_VIEW:
            # Lazy import to avoid circular import
            from synapseclient.models import EntityView

            entityview = EntityView(id=synapse_id, name=name)
            self.entityviews.append(entityview)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(entityview.get_async(synapse_client=synapse_client))
                )
            )
        elif synapse_id and child_type == SUBMISSION_VIEW:
            # Lazy import to avoid circular import
            from synapseclient.models import SubmissionView

            submissionview = SubmissionView(id=synapse_id, name=name)
            self.submissionviews.append(submissionview)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(
                        submissionview.get_async(synapse_client=synapse_client)
                    )
                )
            )
        elif synapse_id and child_type == DATASET_ENTITY:
            # Lazy import to avoid circular import
            from synapseclient.models import Dataset

            dataset = Dataset(id=synapse_id, name=name)
            self.datasets.append(dataset)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(dataset.get_async(synapse_client=synapse_client))
                )
            )
        elif synapse_id and child_type == DATASET_COLLECTION_ENTITY:
            # Lazy import to avoid circular import
            from synapseclient.models import DatasetCollection

            datasetcollection = DatasetCollection(id=synapse_id, name=name)
            self.datasetcollections.append(datasetcollection)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(
                        datasetcollection.get_async(synapse_client=synapse_client)
                    )
                )
            )
        elif synapse_id and child_type == MATERIALIZED_VIEW:
            # Lazy import to avoid circular import
            from synapseclient.models import MaterializedView

            materializedview = MaterializedView(id=synapse_id, name=name)
            self.materializedviews.append(materializedview)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(
                        materializedview.get_async(synapse_client=synapse_client)
                    )
                )
            )
        elif synapse_id and child_type == VIRTUAL_TABLE:
            # Lazy import to avoid circular import
            from synapseclient.models import VirtualTable

            virtualtable = VirtualTable(id=synapse_id, name=name)
            self.virtualtables.append(virtualtable)
            pending_tasks.append(
                asyncio.create_task(
                    wrap_coroutine(
                        virtualtable.get_async(synapse_client=synapse_client)
                    )
                )
            )

        return pending_tasks

    async def _follow_link(
        self,
        child,
        queue: asyncio.Queue,
        recursive: bool = False,
        path: Optional[str] = None,
        download_file: bool = False,
        if_collision: str = COLLISION_OVERWRITE_LOCAL,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        include_activity: bool = True,
        follow_link: bool = False,
        link_hops: int = 0,
        include_types: Optional[List[str]] = None,
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
            or not (link_target_name := entity.get("name", None))
            or not (link_target_id := links_to.get("targetId", None))
        ):
            return

        pending_tasks = self._create_task_for_child(
            child={
                "id": link_target_id,
                "name": link_target_name,
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
            queue=queue,
            include_types=include_types,
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
        result: Union[
            None,
            "Folder",
            "File",
            "Table",
            "EntityView",
            "SubmissionView",
            "Dataset",
            "DatasetCollection",
            "MaterializedView",
            "VirtualTable",
            BaseException,
        ],
        failure_strategy: FailureStrategy,
        *,
        synapse_client: Union[None, Synapse],
    ) -> None:
        """
        Handle what to do based on what was returned from the latest task to complete.
        We are updating the object in place and appending the returned entities to
        the appropriate list.

        Arguments:
            result: The result of the task that was completed.
            failure_strategy: Determines how to handle failures when retrieving children
                under this container and an exception occurs.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
        """
        if result is None:
            # We will recieve None when executing `_wrap_recursive_get_children` as
            # it will internally be recursively calling this method and setting the
            # appropriate folder/file objects in place.
            pass
        elif (
            result.__class__.__name__ == "Folder"
            or result.__class__.__name__ == "File"
            or result.__class__.__name__ == "Table"
            or result.__class__.__name__ == "EntityView"
            or result.__class__.__name__ == "SubmissionView"
            or result.__class__.__name__ == "Dataset"
            or result.__class__.__name__ == "DatasetCollection"
            or result.__class__.__name__ == "MaterializedView"
            or result.__class__.__name__ == "VirtualTable"
        ):
            # Do nothing as the objects are updated in place and the container has
            # already been updated to append the new objects.
            pass
        elif isinstance(result, BaseException):
            if failure_strategy is not None:
                Synapse.get_client(synapse_client=synapse_client).logger.exception(
                    result
                )

                if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                    raise result
        else:
            exception = SynapseError(
                f"Unknown failure retrieving children of Folder ({self.id}): {type(result)}",
                result,
            )
            if failure_strategy is not None:
                Synapse.get_client(synapse_client=synapse_client).logger.exception(
                    exception
                )
                if failure_strategy == FailureStrategy.RAISE_EXCEPTION:
                    raise exception

    def _format_entity_info_for_tree(
        self,
        entity: EntityHeader,
    ) -> str:
        """
        Format entity information for display in progressive tree output.

        Arguments:
            entity: Dictionary containing entity information.

        Returns:
            String representation of the entity for tree display.
        """
        name = entity.name or "Unknown"
        entity_id = entity.id or "Unknown"
        entity_type = entity.type or "Unknown"

        type_name = entity_type
        icon = ""

        if entity_type == FILE_ENTITY:
            type_name = "File"
            icon = "📄"
        elif entity_type == FOLDER_ENTITY:
            type_name = "Folder"
            icon = "📁 "
        elif entity_type == PROJECT_ENTITY:
            type_name = "Project"
            icon = "📂 "
        elif entity_type == TABLE_ENTITY:
            type_name = "Table"
            icon = "📊"
        elif entity_type == ENTITY_VIEW:
            type_name = "EntityView"
            icon = "📊"
        elif entity_type == MATERIALIZED_VIEW:
            type_name = "MaterializedView"
            icon = "📊"
        elif entity_type == VIRTUAL_TABLE:
            type_name = "VirtualTable"
            icon = "📊"
        elif entity_type == DATASET_ENTITY:
            type_name = "Dataset"
            icon = "📊"
        elif entity_type == DATASET_COLLECTION_ENTITY:
            type_name = "DatasetCollection"
            icon = "🗂️ "
        elif entity_type == SUBMISSION_VIEW:
            type_name = "SubmissionView"
            icon = "📊"
        elif "." in entity_type:
            type_name = entity_type.split(".")[-1]

        if not icon:
            icon = "❓"

        base_info = f"{icon} {name} ({entity_id}) [{type_name}]"

        return base_info
