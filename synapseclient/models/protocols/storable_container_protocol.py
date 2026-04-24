"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

import asyncio
from typing import TYPE_CHECKING, List, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.constants.method_flags import COLLISION_OVERWRITE_LOCAL
from synapseclient.models.services.storable_entity_components import (
    MANIFEST_UPLOAD_MAX_RETRIES,
    FailureStrategy,
)

if TYPE_CHECKING:
    from synapseclient.models.file import File


class StorableContainerSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def sync_from_synapse(
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
        manifest: str = "all",
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

        This works similar to [synapseutils.syncFromSynapse][], and generates a
        `manifest.csv` file in each synced directory. The manifest uses CSV format
        with `parentId` and `ID` columns, interoperable with the Synapse UI download
        cart and `synapse get-download-list` CLI output.

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
                [here](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html)
            manifest: Determines whether to generate a manifest CSV file. Options are:

                - `all` (default): generate `manifest.csv` in every synced directory
                - `root`: generate `manifest.csv` only in the root `path` directory
                - `suppress`: do not generate any manifest file
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The object that was called on. This will be the same object that was called on
                to start the sync.

        Example: Using this function
            Suppose I want to walk the immediate children of a folder without downloading the files:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            my_folder = Folder(id="syn12345")
            my_folder.sync_from_synapse(download_file=False, recursive=False)

            for folder in my_folder.folders:
                print(folder.name)

            for file in my_folder.files:
                print(file.name)

            for table in my_folder.tables:
                print(table.name)

            for dataset in my_folder.datasets:
                print(dataset.name)
            ```

            Suppose I want to download the immediate children of a folder:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            my_folder = Folder(id="syn12345")
            my_folder.sync_from_synapse(path="/path/to/folder", recursive=False)

            for folder in my_folder.folders:
                print(folder.name)

            for file in my_folder.files:
                print(file.name)
            ```

            Suppose I want to sync only specific entity types from a Project:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Project

            syn = Synapse()
            syn.login()

            my_project = Project(id="syn12345")
            my_project.sync_from_synapse(
                path="/path/to/folder",
                include_types=["folder", "file", "table", "dataset"]
            )

            # Access different entity types
            for table in my_project.tables:
                print(f"Table: {table.name}")

            for dataset in my_project.datasets:
                print(f"Dataset: {dataset.name}")
            ```

            Suppose I want to download all the children of a Project and all sub-folders and files:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Project

            syn = Synapse()
            syn.login()

            my_project = Project(id="syn12345")
            my_project.sync_from_synapse(path="/path/to/folder")
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
                    and `folder.sync_from_synapse()`
                        note over sync_from_synapse: This is a recursive call to `sync_from_synapse`
                        sync_from_synapse->>sync_from_synapse: Recursive call to `.sync_from_synapse()`
                    end
                end

            deactivate sync_from_synapse
            deactivate project_or_folder
        ```

        """
        return self

    def sync_to_synapse(
        self: Self,
        manifest_path: str,
        dry_run: bool = False,
        send_messages: bool = True,
        retries: int = MANIFEST_UPLOAD_MAX_RETRIES,
        merge_existing_annotations: bool = True,
        associate_activity_to_new_version: bool = False,
        *,
        synapse_client: Synapse | None = None,
    ) -> list["File"]:
        """Upload files to Synapse using a manifest CSV file.

        Accepts manifests produced by sync_from_synapse, the
        synapse get-download-list CLI, or the Synapse UI download cart.
        The manifest must have at minimum a path and parentId column.
        All other columns that are not part of the standard manifest column set
        are treated as file annotations.

        Standard manifest columns:
        [ID, name, parentId, contentType, path, synapseStore, activityName,
        activityDescription, forceVersion, used, executed]

        Arguments:
            manifest_path: Path to the CSV manifest file.
            dry_run: If True, perform full validation of the manifest
                (including verifying that all parent containers exist in
                Synapse) but skip the actual file upload.
            send_messages: If True, send a Synapse notification message on
                completion.
            retries: Number of notification retries (only relevant when
                send_messages=True).
            merge_existing_annotations: If True, merge manifest annotations
                with existing annotations on Synapse. If False, overwrite them.
            associate_activity_to_new_version: If True and a version update
                occurs, the existing Synapse activity is associated with the new
                version.
            synapse_client: If not passed in and caching was not disabled by
                Synapse.allow_client_caching(False) this will use the last
                created instance from the Synapse class constructor.

        Returns:
            List of File entities that were created or updated. Returns an
            empty list if dry_run=True or if no rows were eligible for
            upload.

        Example: Using this function

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Project

            syn = Synapse()
            syn.login()

            project = Project(id="syn12345")
            uploaded = project.sync_to_synapse(manifest_path="/path/to/manifest.csv")
            ```
        """
        return []
