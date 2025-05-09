"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.core.models.permission import Permissions


class AccessControllableSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get_permissions(
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
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            permissions = File(id="syn123").get_permissions()
            ```

            Getting access types list from the Permissions object

            ```
            permissions.access_types
            ```
        """
        return self

    def get_acl(
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
        return [""]

    def set_permissions(
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
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            File(id="syn123").set_permissions(principal_id=273948, access_type=['READ','DOWNLOAD'])
            ```

            Grant the public view access

            ```python
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            File(id="syn123").set_permissions(principal_id=273949, access_type=['READ'])
            ```
        """
        return {}

    def delete_permissions(
        self,
        recursive: bool = False,
        include_self: bool = True,
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
                children if recursive=True or include_container_content=True.
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively delete ACLs from all child containers and their children.
                Only works on classes that support the `sync_from_synapse_async` method.
            include_container_content: If True, delete ACLs from contents directly within
                containers (files and folders inside Projects/Folders). Defaults to False.
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
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            File(id="syn123").delete_permissions()
            ```

        Example: Delete permissions recursively for a folder and all its children
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            # Delete permissions for this folder and all container entities (folders) within it recursively
            Folder(id="syn123").delete_permissions(recursive=True)

            # Delete permissions for all entities within this folder, but not the folder itself
            Folder(id="syn123").delete_permissions(
                include_self=False,
                include_container_content=True
            )

            # Delete permissions only for folder entities within this folder recursively
            Folder(id="syn123").delete_permissions(
                recursive=True,
                target_entity_types=["folder"]
            )

            # Delete all permissions in the entire hierarchy
            Folder(id="syn123").delete_permissions(
                recursive=True,
                include_container_content=True
            )
            ```
        """
        return None
