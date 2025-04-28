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
        self, principal_id: int = None, *, synapse_client: Optional[Synapse] = None
    ) -> List[str]:
        """
        Get the [ACL][synapseclient.core.models.permission.Permissions.access_types]
        that a user or group has on an Entity.

        Arguments:
            principal_id: Identifier of a user or group (defaults to PUBLIC users)
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
