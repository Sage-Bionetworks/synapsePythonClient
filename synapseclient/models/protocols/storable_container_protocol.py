"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.constants.method_flags import COLLISION_OVERWRITE_LOCAL
from synapseclient.models.services.storable_entity_components import FailureStrategy


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
                my_folder.sync_from_synapse(download_file=False, recursive=False)

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
                my_folder.sync_from_synapse(path="/path/to/folder", recursive=False)

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
                my_project.sync_from_synapse(path="/path/to/folder")


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
