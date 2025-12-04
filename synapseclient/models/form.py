from typing import TYPE_CHECKING, Optional

from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse

from dataclasses import dataclass

from synapseclient.models.mixins.form import FormChangeRequest
from synapseclient.models.mixins.form import FormData as FormDataMixin
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


@dataclass
@async_to_sync
class FormData(FormDataMixin):
    async def create_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "FormData":
        """
        Create a new FormData object. The caller will own the resulting object and will have access to read, update, and delete the FormData object.

        Arguments:
            synapse_client: The Synapse client to use for the request.

        Returns:
            A FormData object containing the details of the created form data.

        Examples: create a form data

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData, File
        import asyncio

        async def create_my_form_data():
            syn = Synapse()
            syn.login()

            file = File(id="syn123", download_file=True).get()
            file_handle_id = file.file_handle.id

            form_data = FormData(
                group_id="123",
                name="my_form_data_name",
                data_file_handle_id=file_handle_id
            )
            form_data = await form_data.create_async()

            print(f"Created FormData: {form_data.form_data_id}")
            print(f"Name: {form_data.name}")
            print(f"Group ID: {form_data.group_id}")
            print(f"Created By: {form_data.created_by}")
            print(f"Created On: {form_data.created_on}")
            print(f"Data File Handle ID: {form_data.data_file_handle_id}")

            if form_data.submission_status:
                print(f"Submission State: {form_data.submission_status.state.value}")

        asyncio.run(create_my_form_data())
        ```

        """
        from synapseclient.api.form_services import create_form_data_async

        if not self.group_id or not self.name or not self.data_file_handle_id:
            raise ValueError(
                "'group_id', 'name', and 'data_file_handle_id' must be provided to create a FormData."
            )

        form_change_request = FormChangeRequest(
            name=self.name, file_handle_id=self.data_file_handle_id
        ).to_dict()

        response = await create_form_data_async(
            synapse_client=synapse_client,
            group_id=self.group_id,
            form_change_request=form_change_request,
        )
        return self.fill_from_dict(response)
