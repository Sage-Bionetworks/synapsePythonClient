"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from tqdm import tqdm

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.core.models.permission import Permissions
    from synapseclient.models.mixins.access_control import (
        AclListResult,
        BenefactorTracker,
    )


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
        include_self: bool = True,
        include_container_content: bool = False,
        recursive: bool = False,
        target_entity_types: Optional[List[str]] = None,
        dry_run: bool = False,
        show_acl_details: bool = True,
        show_files_in_containers: bool = True,
        *,
        benefactor_tracker: Optional["BenefactorTracker"] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the entire Access Control List (ACL) for a given Entity. This is not
        scoped to a specific user or group, but rather removes all permissions
        associated with the Entity. After this operation, the Entity will inherit
        permissions from its benefactor, which is typically its parent entity or
        the Project it belongs to.

        In order to remove permissions for a specific user or group, you
        should use the `set_permissions` method with the `access_type` set to
        an empty list.

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
                If False, skip deleting the ACL of the current entity.
            include_container_content: If True, delete ACLs from contents directly within
                containers (files and folders inside self). This must be set to
                True for recursive to have any effect. Defaults to False.
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively process child containers. Note that this must be used with
                include_container_content=True to have any effect. Setting recursive=True
                with include_container_content=False will raise a ValueError.
                Only works on classes that support the `sync_from_synapse_async` method.
            target_entity_types: Specify which entity types to process when deleting ACLs.
                Allowed values are "folder" and "file" (case-insensitive).
                If None, defaults to ["folder", "file"]. This does not affect the
                entity type of the current entity, which is always processed if
                `include_self=True`.
            dry_run: If True, log the changes that would be made instead of actually
                performing the deletions. When enabled, all ACL deletion operations are
                simulated and logged at info level. Defaults to False.
            show_acl_details: When dry_run=True, controls whether current ACL details are
                displayed for entities that will have their permissions changed. If True (default),
                shows detailed ACL information. If False, hides ACL details for cleaner output.
                Has no effect when dry_run=False.
            show_files_in_containers: When dry_run=True, controls whether files within containers
                are displayed in the preview. If True (default), shows all files. If False, hides
                files when their only change is benefactor inheritance (but still shows files with
                local ACLs being deleted). Has no effect when dry_run=False.
            benefactor_tracker: Optional tracker for managing benefactor relationships.
                Used for recursive functionality to track which entities will be affected
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

            # Delete permissions for this folder only (does not affect children)
            Folder(id="syn123").delete_permissions()

            # Delete permissions for all files and folders directly within this folder,
            # but not the folder itself
            Folder(id="syn123").delete_permissions(
                include_self=False,
                include_container_content=True
            )

            # Delete permissions for all items in the entire hierarchy (folders and their files)
            # Both recursive and include_container_content must be True
            Folder(id="syn123").delete_permissions(
                recursive=True,
                include_container_content=True
            )

            # Delete permissions only for folder entities within this folder recursively
            # and their contents
            Folder(id="syn123").delete_permissions(
                recursive=True,
                include_container_content=True,
                target_entity_types=["folder"]
            )

            # Delete permissions only for files within this folder and all subfolders
            Folder(id="syn123").delete_permissions(
                include_self=False,
                recursive=True,
                include_container_content=True,
                target_entity_types=["file"]
            )

            # Dry run example: Log what would be deleted without making changes
            Folder(id="syn123").delete_permissions(
                recursive=True,
                include_container_content=True,
                dry_run=True
            )
            ```
        """
        return None

    def list_acl(
        self,
        recursive: bool = False,
        include_container_content: bool = False,
        target_entity_types: Optional[List[str]] = None,
        log_tree: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        _progress_bar: Optional[tqdm] = None,  # Internal parameter for recursive calls
    ) -> "AclListResult":
        """
        List the Access Control Lists (ACLs) for this entity and optionally its children.

        This function returns the local sharing settings for the entity and optionally
        its children. It provides a mapping of all ACLs for the given container/entity.

        **Important Note:** This function returns the LOCAL sharing settings only, not
        the effective permissions that each Synapse User ID/Team has on the entities.
        More permissive permissions could be granted via a Team that the user has access
        to that has permissions on the entity, or through inheritance from parent entities.

        Arguments:
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively process child containers. Note that this must be used with
                include_container_content=True to have any effect. Setting recursive=True
                with include_container_content=False will raise a ValueError.
                Only works on classes that support the `sync_from_synapse_async` method.
            include_container_content: If True, include ACLs from contents directly within
                containers (files and folders inside self). This must be set to
                True for recursive to have any effect. Defaults to False.
            target_entity_types: Specify which entity types to process when listing ACLs.
                Allowed values are "folder" and "file" (case-insensitive).
                If None, defaults to ["folder", "file"].
            log_tree: If True, logs the ACL results to console in ASCII tree format showing
                entity hierarchies and their ACL permissions in a tree-like structure.
                Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
            _progress_bar: Internal parameter. Progress bar instance to use for updates
                when called recursively. Should not be used by external callers.

        Returns:
            An AclListResult object containing a structured representation of ACLs where:
            - entity_acls: A list of EntityAcl objects, each representing one entity's ACL
            - Each EntityAcl contains acl_entries (a list of AclEntry objects)
            - Each AclEntry contains the principal_id and their list of permissions

        Raises:
            ValueError: If the entity does not have an ID or if an invalid entity type is provided.
            SynapseHTTPError: If there are permission issues accessing ACLs.
            Exception: For any other errors that may occur during the process.

        Example: List ACLs for a single entity
            ```python
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            acl_result = File(id="syn123").list_acl()
            print(acl_result)

            # Access entity ACLs (entity_acls is a list, not a dict)
            for entity_acl in acl_result.all_entity_acls:
                if entity_acl.entity_id == "syn123":
                    # Access individual ACL entries
                    for acl_entry in entity_acl.acl_entries:
                        if acl_entry.principal_id == "273948":
                            print(f"Principal 273948 has permissions: {acl_entry.permissions}")

            # I can also access the ACL for the file itself
            print(acl_result.entity_acl)

            print(acl_result)

            ```

        Example: List ACLs recursively for a folder and all its children
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            acl_result = Folder(id="syn123").list_acl(
                recursive=True,
                include_container_content=True
            )

            # Access each entity's ACL (entity_acls is a list)
            for entity_acl in acl_result.all_entity_acls:
                print(f"Entity {entity_acl.entity_id} has ACL with {len(entity_acl.acl_entries)} principals")

            # I can also access the ACL for the folder itself
            print(acl_result.entity_acl)

            # List ACLs for only folder entities
            folder_acl_result = Folder(id="syn123").list_acl(
                recursive=True,
                include_container_content=True,
                target_entity_types=["folder"]
            )
            ```

        Example: List ACLs with ASCII tree visualization
            When `log_tree=True`, the ACLs will be logged in a tree format. Additionally,
            the `ascii_tree` attribute of the AclListResult will contain the ASCII tree
            representation of the ACLs.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            acl_result = Folder(id="syn123").list_acl(
                recursive=True,
                include_container_content=True,
                log_tree=True, # Enable ASCII tree logging
            )

            # The ASCII tree representation of the ACLs will also be available
            # in acl_result.ascii_tree
            print(acl_result.ascii_tree)
            ```
        """
        return None
