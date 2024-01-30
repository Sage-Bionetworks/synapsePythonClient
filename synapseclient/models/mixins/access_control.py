import asyncio
from typing import Dict, List, Optional, TYPE_CHECKING, Union
from synapseclient import Synapse


from opentelemetry import trace, context
from synapseclient.core.utils import run_and_attach_otel_context

tracer = trace.get_tracer("synapseclient")

if TYPE_CHECKING:
    from synapseclient.core.models.permission import Permissions


class AccessControllable:
    """
    Mixin for objects that can be controlled by an Access Control List (ACL).

    In order to use this mixin, the class must have an `id` attribute.
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued."""

    async def get_permissions(
        self,
        synapse_client: Optional[Synapse] = None,
    ) -> "Permissions":
        """
        Get the [permissions][synapseclient.core.models.permission.Permissions]
        that the caller has on an Entity.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            An Permissions object


        Example: Using this function:
            Getting permissions for a Synapse Entity

                permissions = File(id="syn123").get_permissions()

            Getting access types list from the Permissions object

                permissions.access_types
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()

        return await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(
                    synapse_client=synapse_client
                ).get_permissions(entity=self.id),
                current_context,
            ),
        )

    async def get_acl(
        self, principal_id: int = None, synapse_client: Optional[Synapse] = None
    ) -> List[str]:
        """
        Get the [ACL][synapseclient.core.models.permission.Permissions.access_types]
        that a user or group has on an Entity.

        Arguments:
            principal_id: Identifier of a user or group (defaults to PUBLIC users)
            synapse_client: If not passed in or None this will use the last client from the `.login()` method.

        Returns:
            An array containing some combination of
                ['READ', 'UPDATE', 'CREATE', 'DELETE', 'DOWNLOAD', 'MODERATE',
                'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS']
                or an empty array
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()

        return await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).get_acl(
                    entity=self.id, principal_id=principal_id
                ),
                current_context,
            ),
        )

    async def set_permissions(
        self,
        principal_id=None,
        access_type: List[str] = None,
        modify_benefactor=False,
        warn_if_inherits=True,
        overwrite=True,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Union[str, list]]:
        """
        Sets permission that a user or group has on an Entity.
        An Entity may have its own ACL or inherit its ACL from a benefactor.

        Arguments:
            principalId: Identifier of a user or group. '273948' is for all registered Synapse users
                            and '273949' is for public access.
            accessType: Type of permission to be granted. One or more of CREATE, READ, DOWNLOAD, UPDATE,
                            DELETE, CHANGE_PERMISSIONS.

                **Defaults to ['READ', 'DOWNLOAD']**
            modify_benefactor: Set as True when modifying a benefactor's ACL
            warn_if_inherits: Set as False, when creating a new ACL.
                                Trying to modify the ACL of an Entity that inherits its ACL will result in a warning
            overwrite: By default this function overwrites existing permissions for the specified user.
                        Set this flag to False to add new permissions non-destructively.

        Returns:
            An Access Control List object

        Example: Setting permissions
            Grant all registered users download access

                File(id="syn123").set_permissions(principal_id='273948', access_type=['READ','DOWNLOAD'])

            Grant the public view access
                File(id="syn123").set_permissions(principal_id='273949', access_type=['READ'])
        """
        if access_type is None:
            access_type = ["READ", "DOWNLOAD"]
        loop = asyncio.get_event_loop()
        current_context = context.get_current()

        return await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(
                    synapse_client=synapse_client
                ).setPermissions(
                    entity=self.id,
                    principalId=principal_id,
                    accessType=access_type,
                    modify_benefactor=modify_benefactor,
                    warn_if_inherits=warn_if_inherits,
                    overwrite=overwrite,
                ),
                current_context,
            ),
        )
