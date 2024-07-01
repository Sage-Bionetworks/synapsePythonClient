"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.models.services.storable_entity_components import FailureStrategy

if TYPE_CHECKING:
    from synapseclient.models import Project


class ProjectSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        failure_strategy: FailureStrategy = FailureStrategy.LOG_EXCEPTION,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """
        Store project, files, and folders to synapse. If you have any files or folders
        attached to this project they will be stored as well. You may attach files
        and folders to this project by setting the `files` and `folders` attributes.

        By default the store operation will non-destructively update the project if
        you have not already retrieved the project from Synapse. If you have already
        retrieved the project from Synapse then the store operation will be destructive
        and will overwrite the project with the current state of this object. See the
        `create_or_update` attribute for more information.

        Arguments:
            failure_strategy: Determines how to handle failures when storing attached
                Files and Folders under this Project and an exception occurs.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The project object.

        Example: Using this method to update the description
            Store the project to Synapse using ID

                project = Project(id="syn123", description="new").store()

            Store the project to Synapse using Name

                project = Project(name="my_project", description="new").store()

        Raises:
            ValueError: If the project name is not set.
        """
        return self

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """
        Get the project metadata from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The project object.

        Example: Using this method
            Retrieve the project from Synapse using ID

                project = Project(id="syn123").get()

            Retrieve the project from Synapse using Name

                project = Project(name="my_project").get()

        Raises:
            ValueError: If the project ID or Name is not set.
            SynapseNotFoundError: If the project is not found in Synapse.
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the project from Synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            None

        Example: Using this method
            Delete the project from Synapse using ID

                Project(id="syn123").delete()

            Delete the project from Synapse using Name

                Project(name="my_project").delete()

        Raises:
            ValueError: If the project ID or Name is not set.
            SynapseNotFoundError: If the project is not found in Synapse.
        """
        return None

    def copy(
        self,
        destination_id: str,
        copy_annotations: bool = True,
        copy_wiki: bool = True,
        exclude_types: Optional[List[str]] = None,
        file_update_existing: bool = False,
        file_copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Project":
        """
        You must have already created the Project you will be copying to. It will have
        it's own Synapse ID and unique name that you will use as the destination_id.


        Copy the project to another Synapse project. This will recursively copy all
        Tables, Links, Files, and Folders within the project.

        Arguments:
            destination_id: Synapse ID of a project to copy to.
            copy_annotations: True to copy the annotations.
            copy_wiki: True to copy the wiki pages.
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
            The copied project object.

        Example: Using this function
            Assuming you have a project with the ID "syn123" and you want to copy it to a
            project with the ID "syn456":

                new_instance = Project(id="syn123").copy(destination_id="syn456")

            Copy the project but do not persist annotations:

                new_instance = Project(id="syn123").copy(destination_id="syn456", copy_annotations=False)

        Raises:
            ValueError: If the project does not have an ID and destination_id to copy.
        """
        return self
