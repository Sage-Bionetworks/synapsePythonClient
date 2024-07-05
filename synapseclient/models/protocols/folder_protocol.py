"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.models.services.storable_entity_components import FailureStrategy

if TYPE_CHECKING:
    from synapseclient.models import Folder, Project


class FolderSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Store folders and files to synapse. If you have any files or folders attached
        to this folder they will be stored as well. You may attach files and folders
        to this folder by setting the `files` and `folders` attributes.

        By default the store operation will non-destructively update the folder if
        you have not already retrieved the folder from Synapse. If you have already
        retrieved the folder from Synapse then the store operation will be destructive
        and will overwrite the folder with the current state of this object. See the
        `create_or_update` attribute for more information.

        Arguments:
            parent: The parent folder or project to store the folder in.
            failure_strategy: Determines how to handle failures when storing attached
                Files and Folders under this Folder and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The folder object.

        Raises:
            ValueError: If the folder does not have an id or a
                (name and (`parent_id` or parent with an id)) set.
        """
        return self

    def get(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """Get the folder metadata from Synapse. You are able to find a folder by
        either the id or the name and parent_id.

        Arguments:
            parent: The parent folder or project this folder exists under.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The folder object.

        Raises:
            ValueError: If the folder does not have an id or a
                (name and (`parent_id` or parent with an id)) set.
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the folder from Synapse by its id.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            None

        Raises:
            ValueError: If the folder does not have an id set.
        """
        return None

    def copy(
        self,
        parent_id: str,
        copy_annotations: bool = True,
        exclude_types: Optional[List[str]] = None,
        file_update_existing: bool = False,
        file_copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Folder":
        """
        Copy the folder to another Synapse location. This will recursively copy all
        Tables, Links, Files, and Folders within the folder.

        Arguments:
            parent_id: Synapse ID of a folder/project that the copied entity is
                being copied to
            copy_annotations: True to copy the annotations.
            exclude_types: A list of entity types ['file', 'table', 'link'] which
                determines which entity types to not copy. Defaults to an empty list.
            file_update_existing: When the destination has a file that has the same name,
                users can choose to update that file.
            file_copy_activity: Has three options to set the activity of the copied file:

                    - traceback: Creates a copy of the source files Activity.
                    - existing: Link to the source file's original Activity (if it exists)
                    - None: No activity is set
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied folder object.

        Example: Using this function
            Assuming you have a folder with the ID "syn123" and you want to copy it to a
            project with the ID "syn456":

                new_folder_instance = await Folder(id="syn123").copy(parent_id="syn456")

            Copy the folder but do not persist annotations:

                new_folder_instance = await Folder(id="syn123").copy(parent_id="syn456", copy_annotations=False)

        Raises:
            ValueError: If the folder does not have an ID and parent_id to copy.
        """
        return self
