import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient.api import delete_entity_acl
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models.protocols.access_control_protocol import (
    AccessControllableSynchronousProtocol,
)

if TYPE_CHECKING:
    from synapseclient.core.models.permission import Permissions


@async_to_sync
class AccessControllable(AccessControllableSynchronousProtocol):
    """
    Mixin for objects that can be controlled by an Access Control List (ACL).

    In order to use this mixin, the class must have an `id` attribute.
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued."""

    async def get_permissions_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Permissions":
        """
        Get the [permissions][synapseclient.core.models.permission.Permissions]
        that the caller has on an Entity.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A Permissions object


        Example: Using this function:
            Getting permissions for a Synapse Entity

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                permissions = await File(id="syn123").get_permissions_async()

            asyncio.run(main())
            ```

            Getting access types list from the Permissions object

            ```
            permissions.access_types
            ```
        """
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_permissions(
                entity=self.id
            ),
        )

    async def get_acl_async(
        self,
        principal_id: int = None,
        check_benefactor: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List[str]:
        """
        Get the [ACL][synapseclient.core.models.permission.Permissions.access_types]
        that a user or group has on an Entity.

        Note: If the entity does not have local sharing settings, or ACL set directly
        on it, this will look up the ACL on the benefactor of the entity. The
        benefactor is the entity that the current entity inherits its permissions from.
        The benefactor is usually the parent entity, but it can be any ancestor in the
        hierarchy. For example, a newly created Project will be its own benefactor,
        while a new FileEntity's benefactor will start off as its containing Project or
        Folder. If the entity already has local sharing settings, the benefactor would
        be itself.

        Arguments:
            principal_id: Identifier of a user or group (defaults to PUBLIC users)
            check_benefactor: If True (default), check the benefactor for the entity
                to get the ACL. If False, only check the entity itself.
                This is useful for checking the ACL of an entity that has local sharing
                settings, but you want to check the ACL of the entity itself and not
                the benefactor it may inherit from.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            An array containing some combination of
                ['READ', 'UPDATE', 'CREATE', 'DELETE', 'DOWNLOAD', 'MODERATE',
                'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS']
                or an empty array
        """
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_acl(
                entity=self.id,
                principal_id=principal_id,
                check_benefactor=check_benefactor,
            ),
        )

    async def set_permissions_async(
        self,
        principal_id: int = None,
        access_type: List[str] = None,
        modify_benefactor: bool = False,
        warn_if_inherits: bool = True,
        overwrite: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Union[str, list]]:
        """
        Sets permission that a user or group has on an Entity.
        An Entity may have its own ACL or inherit its ACL from a benefactor.

        Arguments:
            principal_id: Identifier of a user or group. `273948` is for all
                registered Synapse users and `273949` is for public access.
                None implies public access.
            access_type: Type of permission to be granted. One or more of CREATE,
                READ, DOWNLOAD, UPDATE, DELETE, CHANGE_PERMISSIONS.

                **Defaults to ['READ', 'DOWNLOAD']**
            modify_benefactor: Set as True when modifying a benefactor's ACL. The term
                'benefactor' is used to indicate which Entity an Entity inherits its
                ACL from. For example, a newly created Project will be its own
                benefactor, while a new FileEntity's benefactor will start off as its
                containing Project. If the entity already has local sharing settings
                the benefactor would be itself. It may also be the immediate parent,
                somewhere in the parent tree, or the project itself.
            warn_if_inherits: When `modify_benefactor` is True, this does not have any
                effect. When `modify_benefactor` is False, and `warn_if_inherits` is
                True, a warning log message is produced if the benefactor for the
                entity you passed into the function is not itself, i.e., it's the
                parent folder, or another entity in the parent tree.
            overwrite: By default this function overwrites existing permissions for
                the specified user. Set this flag to False to add new permissions
                non-destructively.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            An Access Control List object

        Example: Setting permissions
            Grant all registered users download access

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").set_permissions_async(principal_id=273948, access_type=['READ','DOWNLOAD'])

            asyncio.run(main())
            ```

            Grant the public view access

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").set_permissions_async(principal_id=273949, access_type=['READ'])

            asyncio.run(main())
            ```
        """
        if access_type is None:
            access_type = ["READ", "DOWNLOAD"]
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).setPermissions(
                entity=self.id,
                principalId=principal_id,
                accessType=access_type,
                modify_benefactor=modify_benefactor,
                warn_if_inherits=warn_if_inherits,
                overwrite=overwrite,
            ),
        )

    async def delete_permissions_async(
        self,
        include_self: bool = True,
        recursive: bool = False,
        include_container_content: bool = False,
        target_entity_types: Optional[List[str]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the Access Control List (ACL) for a given Entity.

        By default, Entities such as FileEntity and Folder inherit their permission from
        their containing Project. For such Entities the Project is the Entity's 'benefactor'.
        This permission inheritance can be overridden by creating an ACL for the Entity.
        When this occurs the Entity becomes its own benefactor and all permission are
        determined by its own ACL.

        If the ACL of an Entity is deleted, then its benefactor will automatically be set
        to its parent's benefactor.

        **Special notice for Projects:** The ACL for a Project cannot be deleted, you
        must individually update or revoke the permissions for each user or group.

        Arguments:
            include_self: If True (default), delete the ACL of the current entity.
                If False, skip deleting the ACL of the current entity and only process
                children if recursive=True and include_container_content=True.
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively process child containers. Note that this must be used with
                include_container_content=True to have any effect. Setting recursive=True
                with include_container_content=False will raise a ValueError.
                Only works on classes that support the `sync_from_synapse_async` method.
            include_container_content: If True, delete ACLs from contents directly within
                containers (files and folders inside Projects/Folders). This must be set to
                True for recursive to have any effect. Defaults to False.
            target_entity_types: Specify which entity types to process when deleting ACLs.
                Allowed values are "folder" and "file" (case-insensitive).
                If None, defaults to ["folder", "file"].
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If the entity does not have an ID or if an invalid entity type is provided.
            SynapseHTTPError: If there are permission issues or if the entity already inherits permissions.
            Exception: For any other errors that may occur during the process.

        Note: The caller must be granted ACCESS_TYPE.CHANGE_PERMISSIONS on the Entity to
        call this method.

        Example: Delete permissions for a single entity
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").delete_permissions_async()

            asyncio.run(main())
            ```

        Example: Delete permissions recursively for a folder and all its children
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            async def main():
                # Delete permissions for this folder only (does not affect children)
                await Folder(id="syn123").delete_permissions_async()

                # Delete permissions for all files and folders directly within this folder,
                # but not the folder itself
                await Folder(id="syn123").delete_permissions_async(
                    include_self=False,
                    include_container_content=True
                )

                # Delete permissions for all items in the entire hierarchy (folders and their files)
                # Both recursive and include_container_content must be True
                await Folder(id="syn123").delete_permissions_async(
                    recursive=True,
                    include_container_content=True
                )

                # Delete permissions only for folder entities within this folder recursively
                # and their contents
                await Folder(id="syn123").delete_permissions_async(
                    recursive=True,
                    include_container_content=True,
                    target_entity_types=["folder"]
                )

                # Delete permissions only for files within this folder and all subfolders
                await Folder(id="syn123").delete_permissions_async(
                    include_self=False,
                    recursive=True,
                    include_container_content=True,
                    target_entity_types=["file"]
                )
            asyncio.run(main())
            ```
        """
        if not self.id:
            raise ValueError("The entity must have an ID to delete permissions.")

        normalized_types = self._normalize_target_entity_types(target_entity_types)
        client = Synapse.get_client(synapse_client=synapse_client)
        entity_info = self._get_entity_type_info(normalized_types)

        if include_self:
            await self._delete_current_entity_acl(client, entity_info)

        should_process_children = recursive or include_container_content
        if should_process_children and hasattr(self, "sync_from_synapse_async"):
            if recursive and not include_container_content:
                raise ValueError(
                    "When recursive=True, include_container_content must also be True. "
                    "Setting recursive=True with include_container_content=False has no effect."
                )

            synced = await self._sync_container_structure(client)
            if not synced:
                return

            if include_container_content:
                await self._process_container_contents(
                    client=client, target_entity_types=normalized_types
                )

            if recursive and hasattr(self, "folders"):
                await self._process_folders_recursively(
                    client=client,
                    target_entity_types=normalized_types,
                    include_container_content=include_container_content,
                )

    def _normalize_target_entity_types(
        self, target_entity_types: Optional[List[str]]
    ) -> List[str]:
        """
        Normalize and validate the target entity types.

        Arguments:
            target_entity_types: A list of entity types to validate. If None, returns default types.

        Returns:
            List[str]: A normalized list (lowercase) of valid entity types.

        Raises:
            ValueError: If any provided entity type is not in the list of valid types.
        """
        valid_types = ["folder", "file"]

        if target_entity_types is None:
            return valid_types

        normalized_types = [t.lower() for t in target_entity_types]

        invalid_types = [t for t in normalized_types if t not in valid_types]
        if invalid_types:
            raise ValueError(
                f"Invalid entity type(s): {', '.join(invalid_types)}. "
                f"Allowed values are: {', '.join(valid_types)}"
            )

        return normalized_types

    def _get_entity_type_info(self, target_entity_types: List[str]) -> Dict[str, Any]:
        """
        Determine the entity type and if it's in the target types.

        Arguments:
            target_entity_types: A list of normalized entity types to check against.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - 'entity_type': The detected entity type ('folder', 'file', or None)
                - 'is_target_type': Whether the entity's type is in the target types
        """
        is_folder = hasattr(self, "folders")
        is_file = self.__class__.__name__.lower() == "file"

        entity_type = "folder" if is_folder else "file" if is_file else None
        is_target_type = entity_type in target_entity_types if entity_type else False

        return {"entity_type": entity_type, "is_target_type": is_target_type}

    async def _delete_current_entity_acl(
        self, client: Synapse, entity_info: Dict[str, Any]
    ) -> None:
        """
        Delete the ACL for the current entity if it's a target type or has no specific type.

        Arguments:
            client: The Synapse client instance to use for API calls.
            entity_info: Dictionary containing entity type information with keys:
                - 'entity_type': The detected entity type ('folder', 'file', or None)
                - 'is_target_type': Whether the entity's type is in the target types

        Returns:
            None

        Raises:
            SynapseHTTPError: If there are permission issues or if the entity already inherits permissions.
            Exception: For any other errors that may occur during deletion.
        """
        if not entity_info["is_target_type"] and entity_info["entity_type"] is not None:
            client.logger.debug(
                f"Skipping ACL deletion for entity {self.id} as its type '{entity_info['entity_type']}' does not match the target types."
            )
            return

        try:
            await delete_entity_acl(entity_id=self.id, synapse_client=client)
            client.logger.debug(f"Deleted ACL for entity {self.id}")
        except SynapseHTTPError as e:
            if (
                e.response.status_code == 403
                and "Resource already inherits its permissions." in e.response.text
            ):
                client.logger.debug(
                    f"Entity {self.id} already inherits permissions from its parent."
                )
            else:
                client.logger.warning(
                    f"Failed to delete ACL for entity {self.id}: {str(e)}"
                )
        except Exception as e:
            client.logger.warning(
                f"Failed to delete ACL for entity {self.id}: {str(e)}"
            )

    async def _sync_container_structure(self, client: Synapse) -> bool:
        """
        Sync the container structure from Synapse and return success status.

        Arguments:
            client: The Synapse client instance to use for API calls.

        Returns:
            bool: True if synchronization was successful, False otherwise.

        Raises:
            Exception: For any errors that may occur during synchronization.
        """
        try:
            await self.sync_from_synapse_async(
                recursive=False, download_file=False, synapse_client=client
            )
            return True
        except Exception as e:
            client.logger.warning(
                f"Failed to sync from Synapse for entity {self.id}: {str(e)}. "
                f"Cannot process children."
            )
            return False

    async def _process_container_contents(
        self, client: Synapse, target_entity_types: List[str]
    ) -> None:
        """
        Process the direct contents of a container entity.

        Arguments:
            client: The Synapse client instance to use for API calls.
            target_entity_types: A list of normalized entity types to process.
            recursive: Whether recursive processing is being applied in the parent call.

        Returns:
            None
        """
        if "file" in target_entity_types and hasattr(self, "files"):
            await self._process_files(client)

        if "folder" in target_entity_types and hasattr(self, "folders"):
            await self._process_direct_folders(client)

    async def _process_files(self, client: Synapse) -> None:
        """
        Process the files directly within this container.

        Arguments:
            client: The Synapse client instance to use for API calls.

        Returns:
            None

        Raises:
            Exception: For any errors that may occur during processing, which are caught and logged.
        """
        for file in getattr(self, "files", []):
            if hasattr(file, "delete_permissions_async"):
                try:
                    await file.delete_permissions_async(
                        recursive=False,
                        include_self=True,
                        target_entity_types=["file"],
                        synapse_client=client,
                    )
                except Exception as e:
                    client.logger.warning(
                        f"Failed to delete ACL for file {file.id}: {str(e)}"
                    )

    async def _process_direct_folders(self, client: Synapse) -> None:
        """
        Process folders directly within this container (non-recursively).

        Arguments:
            client: The Synapse client instance to use for API calls.

        Returns:
            None

        Raises:
            Exception: For any errors that may occur during processing, which are caught and logged.
        """
        for folder in getattr(self, "folders", []):
            if hasattr(folder, "delete_permissions_async"):
                try:
                    await folder.delete_permissions_async(
                        include_self=True,
                        recursive=False,
                        include_container_content=False,
                        target_entity_types=["folder"],
                        synapse_client=client,
                    )
                except Exception as e:
                    client.logger.warning(
                        f"Failed to delete ACL for folder {folder.id}: {str(e)}"
                    )

    async def _process_folders_recursively(
        self,
        client: Synapse,
        target_entity_types: List[str],
        include_container_content: bool,
    ) -> None:
        """
        Process child folders recursively.

        Arguments:
            client: The Synapse client instance to use for API calls.
            target_entity_types: A list of normalized entity types to process.
            include_container_content: Whether to include the content of containers in processing.

        Returns:
            None

        Raises:
            Exception: For any errors that may occur during processing, which are caught and logged.
        """
        for folder in getattr(self, "folders", []):
            if hasattr(folder, "delete_permissions_async"):
                try:
                    should_delete_folder_acl = (
                        "folder" in target_entity_types and include_container_content
                    )

                    await folder.delete_permissions_async(
                        include_self=should_delete_folder_acl,
                        recursive=True,
                        include_container_content=include_container_content,
                        target_entity_types=target_entity_types,
                        synapse_client=client,
                    )
                except Exception as e:
                    client.logger.warning(
                        f"Failed to delete ACL for folder {folder.id}: {str(e)}"
                    )
