"""ProjectSetting model for managing project settings in Synapse."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from synapseclient import Synapse
from synapseclient.api.project_setting_services import (
    create_project_setting,
    delete_project_setting,
    get_project_setting,
    update_project_setting,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants import concrete_types
from synapseclient.models.protocols.project_setting_protocol import (
    ProjectSettingSynchronousProtocol,
)


@dataclass()
@async_to_sync
class ProjectSetting(ProjectSettingSynchronousProtocol):
    """Represents a project setting in Synapse, controlling how files are uploaded
    and stored within a project or folder.

    Currently supports the ``"upload"`` settings type, which is backed by
    ``UploadDestinationListSetting`` in the Synapse REST API. Additional settings
    types (e.g. ``"external_sync"``, ``"requester_pays"``) may be introduced in
    future subclasses.

    Attributes:
        id: (Read Only) The unique ID of this project setting, assigned by the
            server on creation.
        project_id: The Synapse ID of the project or folder this setting belongs to.
            Required for `get()` and `store()`.
        settings_type: The type of project setting. Currently only ``"upload"``
            is supported. Default: ``"upload"``.
        locations: The list of storage location IDs for upload. The first ID is the
            default upload destination. A project may have at most 10 storage locations.
            To obtain a storage location ID, create a
            [StorageLocation][synapseclient.models.StorageLocation] and use its
            `storage_location_id`. See
            [StorageLocationType][synapseclient.models.StorageLocationType] for the
            available storage backend types.
        concrete_type: (Read Only) The concrete type returned by the Synapse REST API.
        etag: (Read Only) Synapse employs an Optimistic Concurrency Control (OCC)
            scheme. The etag changes every time the setting is updated; it must be
            included on updates.

    Example: Creating a project setting from a new storage location:
        Create a StorageLocation first, then use its ID when creating the
        project setting:

            from synapseclient.models import (
                ProjectSetting,
                StorageLocation,
                StorageLocationType,
            )

            import synapseclient
            synapseclient.login()

            storage = StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_S3,
                bucket="my-bucket",
                base_key="my/prefix",
            ).store()

            setting = ProjectSetting(
                project_id="syn123",
                locations=[storage.storage_location_id],
            ).store()
            print(f"Created setting ID: {setting.id}")

    Example: Creating a project setting:

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
        Retrieve and update the storage locations on an existing setting:

            from synapseclient.models import ProjectSetting

            import synapseclient
            synapseclient.login()

            setting = ProjectSetting(project_id="syn123", settings_type="upload").get()
            setting.locations = [12345, 67890]
            setting.store()
            print(f"Updated setting ID: {setting.id}")

    Example: Deleting a project setting
        Remove the project setting entirely:

            from synapseclient.models import ProjectSetting

            import synapseclient
            synapseclient.login()

            setting = ProjectSetting(project_id="syn123", settings_type="upload").get()
            if setting:
                setting.delete()
    """

    id: Optional[str] = None
    """(Read Only) The unique ID of this project setting, assigned by the server on
    creation."""

    project_id: Optional[str] = None
    """The Synapse ID of the project or folder this setting belongs to. Required for
    `get()` and `store()`."""

    settings_type: str = "upload"
    """The type of project setting. Currently only ``"upload"`` is supported."""

    locations: List[int] = field(default_factory=list)
    """The list of storage location IDs for upload. The first ID is the default upload
    destination. A project may have at most 10 storage locations. To obtain a storage
    location ID, create a [StorageLocation][synapseclient.models.StorageLocation] and
    use its `storage_location_id`. See
    [StorageLocationType][synapseclient.models.StorageLocationType] for the available
    storage backend types."""

    concrete_type: Optional[str] = field(default=None, compare=False)
    """(Read Only) The concrete type returned by the Synapse REST API."""

    etag: Optional[str] = field(default=None, compare=False)
    """(Read Only) Synapse employs an Optimistic Concurrency Control (OCC) scheme.
    The etag changes every time the setting is updated."""

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "ProjectSetting":
        """Populate this dataclass from a REST API response dict.

        Arguments:
            synapse_response: The response from the REST API.

        Returns:
            The ProjectSetting object.
        """
        self.id = synapse_response.get("id", None)
        self.project_id = synapse_response.get("projectId", None)
        self.settings_type = synapse_response.get("settingsType", self.settings_type)
        self.locations = synapse_response.get("locations", [])
        self.concrete_type = synapse_response.get("concreteType", None)
        self.etag = synapse_response.get("etag", None)
        return self

    def _to_synapse_request(self) -> Dict[str, Any]:
        """Convert this dataclass to a request body for the REST API.

        Returns:
            A dictionary suitable for the REST API.
        """
        request: Dict[str, Any] = {
            "concreteType": concrete_types.UPLOAD_DESTINATION_LIST_SETTING,
            "settingsType": self.settings_type,
            "projectId": self.project_id,
            "locations": self.locations,
        }
        if self.id is not None:
            request["id"] = self.id
        if self.etag is not None:
            request["etag"] = self.etag
        return request

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"ProjectSetting_Get: {self.project_id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional["ProjectSetting"]:
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import ProjectSetting

                syn = Synapse()
                syn.login()

                async def main():
                    setting = await ProjectSetting(
                        project_id="syn123", settings_type="upload"
                    ).get_async()
                    if setting:
                        print(f"Storage locations: {setting.locations}")

                asyncio.run(main())
        """
        if not self.project_id:
            raise ValueError("project_id is required to retrieve a project setting.")

        response = await get_project_setting(
            project_id=self.project_id,
            setting_type=self.settings_type,
            synapse_client=synapse_client,
        )
        if not response:
            return None
        self.fill_from_dict(response)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"ProjectSetting_Store: {self.project_id}"
    )
    async def store_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "ProjectSetting":
        """Create or update this project setting in Synapse.

        If this setting does not yet have an `id`, a new project setting is created.
        If `id` is already set (e.g. retrieved via `get_async()`), the existing
        setting is updated.

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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import ProjectSetting

                syn = Synapse()
                syn.login()

                async def main():
                    setting = await ProjectSetting(
                        project_id="syn123",
                        settings_type="upload",
                        locations=[12345],
                    ).store_async()
                    print(f"Created setting ID: {setting.id}")

                asyncio.run(main())

        Example: Updating an existing project setting
            Retrieve and then update the storage locations on an existing setting:

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import ProjectSetting

                syn = Synapse()
                syn.login()

                async def main():
                    setting = await ProjectSetting(
                        project_id="syn123", settings_type="upload"
                    ).get_async()
                    setting.locations = [12345, 67890]
                    await setting.store_async()
                    print(f"Updated setting ID: {setting.id}")

                asyncio.run(main())
        """
        if not self.project_id:
            raise ValueError("project_id is required to store a project setting.")

        request = self._to_synapse_request()

        if self.id is None:
            response = await create_project_setting(
                request=request,
                synapse_client=synapse_client,
            )
        else:
            await update_project_setting(
                request=request,
                synapse_client=synapse_client,
            )
            response = await get_project_setting(
                project_id=self.project_id,
                setting_type=self.settings_type,
                synapse_client=synapse_client,
            )

        self.fill_from_dict(response)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"ProjectSetting_Delete: {self.id}"
    )
    async def delete_async(
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import ProjectSetting

                syn = Synapse()
                syn.login()

                async def main():
                    await ProjectSetting(
                        id = "123"
                    ).delete_async()
                asyncio.run(main())
        """
        if not self.id:
            raise ValueError("id is required to delete a project setting.")

        await delete_project_setting(
            setting_id=self.id,
            synapse_client=synapse_client,
        )
