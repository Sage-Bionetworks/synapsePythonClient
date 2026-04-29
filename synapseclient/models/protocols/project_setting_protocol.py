"""Protocol for the specific methods of ProjectSetting that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Optional, Protocol

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models.project_setting import ProjectSetting


class ProjectSettingSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "ProjectSetting":
        """Retrieve this project setting from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The ProjectSetting object populated with data from Synapse, or None if
            no setting exists for this project and settings_type.

        Raises:
            ValueError: If `project_id` is not set.

        Example: Using this function
            Get the upload project setting for a project:

                from synapseclient.models import ProjectSetting

                import synapseclient
                synapseclient.login()

                setting = ProjectSetting(project_id="syn123", settings_type="upload").get()
                if setting:
                    print(f"Storage locations: {setting.locations}")
        """
        return self

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "ProjectSetting":
        """Create or update this project setting in Synapse.

        If this setting does not yet have an `id`, a new project setting is created.
        If `id` is already set (e.g. retrieved via `get()`), the existing setting
        is updated.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The ProjectSetting object with server-assigned fields populated.

        Raises:
            ValueError: If `project_id` is not set.

        Example: Creating a new project setting
            Assign a custom storage location to a project for the first time:

                from synapseclient.models import ProjectSetting

                import synapseclient
                synapseclient.login()

                setting = ProjectSetting(
                    project_id="syn123",
                    settings_type="upload",
                    locations=[12345],
                ).store()
                print(f"Created setting ID: {setting.id}")

        Example: Updating an existing project setting
            Retrieve and then update the storage locations on an existing setting:

                from synapseclient.models import ProjectSetting

                import synapseclient
                synapseclient.login()

                setting = ProjectSetting(project_id="syn123", settings_type="upload").get()
                setting.locations = [12345, 67890]
                setting.store()
                print(f"Updated setting ID: {setting.id}")
        """
        return self

    def delete(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Delete this project setting from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If `id` is not set.

        Example: Using this function
            Delete a project setting:

                from synapseclient.models import ProjectSetting

                import synapseclient
                synapseclient.login()

                setting = ProjectSetting(project_id="syn123", settings_type="upload").get()
                if setting:
                    setting.delete()
        """
        return self
