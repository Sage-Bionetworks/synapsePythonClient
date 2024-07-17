"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project


@async_to_sync
class FileSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Store the file in Synapse. With this method you may:

        - Upload a file into Synapse
        - Update the metadata of a file in Synapse
        - Store a File object in Synapse without updating a file by setting
            `synapse_store` to False.
        - Change the name of a file in Synapse by setting the `name` attribute of the
            File object. Also see the [synapseclient.models.File.change_metadata][]
            method for changing the name of the downloaded file.
        - Moving a file to a new parent by setting the `parent_id` attribute of the
            File object.

        Arguments:
            parent: The parent folder or project to store the file in. May also be
                specified in the File object. If both are provided the parent passed
                into `store` will take precedence.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.


        Example: Using this function
            File with the ID `syn123` at path `path/to/file.txt`:

                file_instance = File(id="syn123", path="path/to/file.txt").store()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = File(path="path/to/file.txt", parent_id="syn456").store()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = File(path="path/to/file.txt").store(parent=Folder(id="syn456"))

            Rename a file (Does not update the file on disk or the name of the downloaded file):

                file_instance = File(id="syn123", download_file=False).get()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                file_instance.change_metadata(name="my_new_name_file.txt")

            Rename a file, and the name of the file as downloaded (Does not update the file on disk):

                file_instance = File(id="syn123", download_file=False).get()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                file_instance.change_metadata(name="my_new_name_file.txt", download_as="my_new_name_file.txt")

        """
        return self

    def change_metadata(
        self,
        name: Optional[str] = None,
        download_as: Optional[str] = None,
        content_type: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Change File Entity metadata for properties that are immutable after creation
        through the store method.

        Arguments:
            name: Specify to change the filename of a file as seen on Synapse.
            download_as: Specify filename to change the filename of a filehandle.
            content_type: Specify content type to change the content type of a
                filehandle.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Can be used to change the filename, the filename when the file is downloaded, or the file content-type without downloading:

                file_entity = File(id="syn123", download_file=False).get()
                print(os.path.basename(file_entity.path))  ## prints, e.g., "my_file.txt"
                file_entity = file_entity.change_metadata(name="my_new_name_file.txt", download_as="my_new_downloadAs_name_file.txt", content_type="text/plain")
                print(os.path.basename(file_entity.path))  ## prints, "my_new_downloadAs_name_file.txt"
                print(file_entity.name) ## prints, "my_new_name_file.txt"

        Raises:
            ValueError: If the file does not have an ID to change metadata.
        """
        return self

    def get(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Get the file from Synapse. You may retrieve a File entity by either:

        - id
        - path


        If you specify both, the `id` will take precedence.


        If you specify the `path` and the file is stored in multiple locations in Synapse
        only the first one found will be returned. The other matching files will be
        printed to the console.


        You may also specify a `version_number` to get a specific version of the file.

        Arguments:
            include_activity: If True the activity will be included in the file if it exists.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.

        Raises:
            ValueError: If the file does not have an ID or path to get.


        Example: Using this function
            Assuming you have a file with the ID "syn123":

                file_instance = File(id="syn123").get()

            Assuming you want to download a file to this directory: "path/to/directory":

                file_instance = File(path="path/to/directory").get()
        """
        return self

    @classmethod
    def from_id(
        cls,
        synapse_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Wrapper for [synapseclient.models.File.get][].

        Arguments:
            synapse_id: The ID of the file in Synapse.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Assuming you have a file with the ID "syn123":

                file_instance = File.from_id(synapse_id="syn123")
        """
        from synapseclient.models import File

        return File()

    @classmethod
    def from_path(
        cls,
        path: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Get the file from Synapse. If the path of the file matches multiple files
        within Synapse the first one found will be returned. The other matching
        files will be printed to the console.


        Wrapper for [synapseclient.models.File.get][].

        Arguments:
            path: The path to the file on disk.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Assuming you have a file at the path "path/to/file.txt":

                file_instance = File.from_path(path="path/to/file.txt")
        """
        from synapseclient.models import File

        return File()

    def delete(
        self,
        version_only: Optional[bool] = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Delete the file from Synapse.

        Arguments:
            version_only: If True only the version specified in the `version_number`
                attribute of the file will be deleted. If False the entire file will
                be deleted.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            None

        Raises:
            ValueError: If the file does not have an ID to delete.
            ValueError: If the file does not have a version number to delete a version,
                and `version_only` is True.

        Example: Using this function
            Assuming you have a file with the ID "syn123":

                File(id="syn123").delete()
        """
        return None

    def copy(
        self,
        parent_id: str,
        update_existing: bool = False,
        copy_annotations: bool = True,
        copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Copy the file to another Synapse location. Defaults to the latest version of the
        file, or the version_number specified in the instance.

        Arguments:
            parent_id: Synapse ID of a folder/project that the copied entity is being
                copied to
            update_existing: When the destination has a file that has the same name,
                users can choose to update that file.
            copy_annotations: True to copy the annotations.
            copy_activity: Has three options to set the activity of the copied file:

                    - traceback: Creates a copy of the source files Activity.
                    - existing: Link to the source file's original Activity (if it exists)
                    - None: No activity is set
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied file object.

        Example: Using this function
            Assuming you have a file with the ID "syn123" and you want to copy it to a folder with the ID "syn456":

                new_file_instance = File(id="syn123").copy(parent_id="syn456")

            Copy the file but do not persist annotations or activity:

                new_file_instance = File(id="syn123").copy(parent_id="syn456", copy_annotations=False, copy_activity=None)

        Raises:
            ValueError: If the file does not have an ID and parent_id to copy.
        """
        from synapseclient.models import File

        return File()
