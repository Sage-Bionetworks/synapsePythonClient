from typing import TYPE_CHECKING, Optional

from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse

from dataclasses import dataclass

from synapseclient.models.mixins.form import FormGroup as FormGroupMixin


@dataclass
@async_to_sync
class FormGroup(FormGroupMixin):
    async def create_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "FormGroup":
        """
        Create a FormGroup with the provided name. This method is idempotent. If a group with the provided name already exists and the caller has ACCESS_TYPE.READ permission the existing FormGroup will be returned.

        Arguments:
            name: A globally unique name for the group. Required. Between 3 and 256 characters.
            synapse_client: Optional Synapse client instance for authentication.

        Returns:
            A FormGroup object containing the details of the created group.

        Examples: create a form group

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormGroup
        import asyncio

        async def create_my_form_group():
            syn = Synapse()
            syn.login()

            form_group = FormGroup(name="my_unique_form_group_name")
            form_group = await form_group.create_async()
            print(form_group)

        asyncio.run(create_my_form_group())
        ```
        """
        if not self.name:
            raise ValueError("FormGroup 'name' must be provided to create a FormGroup.")

        from synapseclient.api.form_services import create_form_group_async

        response = await create_form_group_async(
            synapse_client=synapse_client,
            name=self.name,
        )
        return self.fill_from_dict(response)
