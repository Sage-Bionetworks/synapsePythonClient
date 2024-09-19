import asyncio
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
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

                permissions = await File(id="syn123").get_permissions_async()

            Getting access types list from the Permissions object

                permissions.access_types
        """
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_permissions(
                entity=self.id
            ),
        )

    async def get_acl_async(
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
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_acl(
                entity=self.id, principal_id=principal_id
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
            modify_benefactor: Set as True when modifying a benefactor's ACL
            warn_if_inherits: Set as False, when creating a new ACL. Trying to modify
                the ACL of an Entity that inherits its ACL will result in a warning
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

                await File(id="syn123").set_permissions_async(principal_id=273948, access_type=['READ','DOWNLOAD'])

            Grant the public view access

                await File(id="syn123").set_permissions_async(principal_id=273949, access_type=['READ'])
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
