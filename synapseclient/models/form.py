import os
from dataclasses import dataclass
from typing import AsyncGenerator, Generator, List, Optional

from synapseclient import Synapse
from synapseclient.core.async_utils import (
    async_to_sync,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.models.mixins.form import FormChangeRequest
from synapseclient.models.mixins.form import FormData as FormDataMixin
from synapseclient.models.mixins.form import FormGroup as FormGroupMixin
from synapseclient.models.mixins.form import StateEnum
from synapseclient.models.protocols.form_protocol import (
    FormDataProtocol,
    FormGroupProtocol,
)


@dataclass
@async_to_sync
class FormGroup(FormGroupMixin, FormGroupProtocol):
    """Dataclass representing a FormGroup."""

    # Default states for listing FormData
    DEFAULT_OWNER_STATES = [
        "waiting_for_submission",
        "submitted_waiting_for_review",
        "accepted",
        "rejected",
    ]
    DEFAULT_REVIEWER_STATES = [
        "submitted_waiting_for_review",
        "accepted",
        "rejected",
    ]

    async def create_or_get_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "FormGroup":
        """
        Create or get a FormGroup with the provided name. This method is idempotent. If a group with the provided name already exists and the caller has ACCESS_TYPE.READ permission the existing FormGroup will be returned.

        Arguments:
            synapse_client: Optional Synapse client instance for authentication.

        Returns:
            A FormGroup object containing the details of the created group.

        Examples: create a FormGroup

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormGroup
        import asyncio

        async def create_my_form_group():
            syn = Synapse()
            syn.login()

            form_group = FormGroup(name="my_unique_form_group_name")
            form_group = await form_group.create_or_get_async()
            print(form_group)

        asyncio.run(create_my_form_group())
        ```
        """
        if not self.name:
            raise ValueError("FormGroup 'name' must be provided to create a FormGroup.")

        from synapseclient.api.form_services import create_form_group

        response = await create_form_group(
            synapse_client=synapse_client,
            name=self.name,
        )
        return self.fill_from_dict(response)

    def _validate_filter_by_state(
        self,
        filter_by_state: List[str],
        as_reviewer: bool = False,
    ) -> None:
        """
        Validate filter_by_state values.

        Arguments:
            filter_by_state: List of str values to validate.
            as_reviewer: If True, uses the POST POST /form/data/list/reviewer endpoint to review submission. If False (default), use POST /form/data/list endpoint to list only FormData owned by the caller.
        """
        if not filter_by_state:
            return
        valid_string_values = [
            "waiting_for_submission",
            "submitted_waiting_for_review",
            "accepted",
            "rejected",
        ]

        if as_reviewer:
            valid_string_values.remove("waiting_for_submission")

        for state in filter_by_state:
            if state not in valid_string_values:
                raise ValueError(
                    f"Invalid state: {state}. Valid values are: {', '.join(valid_string_values)}"
                )

    def _convert_state_enum_strings(
        self,
        state_enum_list: List[str],
    ) -> List[StateEnum]:
        """
        Convert list of state enum strings to StateEnum values.

        Arguments:
            state_enum_list: List of StateEnum values as string.

        Returns:
            List of string values corresponding to the StateEnum.
        """
        state_enum_mapping = {
            "waiting_for_submission": StateEnum.WAITING_FOR_SUBMISSION,
            "submitted_waiting_for_review": StateEnum.SUBMITTED_WAITING_FOR_REVIEW,
            "accepted": StateEnum.ACCEPTED,
            "rejected": StateEnum.REJECTED,
        }
        return [
            state_enum_mapping.get(state)
            for state in state_enum_list
            if state in state_enum_mapping
        ]

    @skip_async_to_sync
    async def list_async(
        self,
        *,
        filter_by_state: Optional[List[str]] = None,
        synapse_client: Optional["Synapse"] = None,
        as_reviewer: bool = False,
    ) -> AsyncGenerator["FormData", None]:
        """
        List FormData objects in a FormGroup.

        Arguments:
            filter_by_state: list of StateEnum to filter the results.
                When as_reviewer=False (default), valid values are:
                - waiting_for_submission
                - submitted_waiting_for_review
                - accepted
                - rejected

                When as_reviewer=True, valid values are:
                - submitted_waiting_for_review
                - accepted
                - rejected
                Note: waiting_for_submission is NOT allowed when as_reviewer=True.
            synapse_client: The Synapse client to use for the request.

            as_reviewer: If True, uses the POST POST /form/data/list/reviewer endpoint to review submission. If False (default), use POST /form/data/list endpoint to list only FormData owned by the caller.

        Yields:
            FormData objects matching the request.

        Raises:
            ValueError: If group_id is not set or filter_by_state contains invalid values.

        Examples: List your own form data

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormGroup
        import asyncio

        async def list_my_form_data():
            syn = Synapse()
            syn.login()

            form_group = await FormGroup(name="test").create_or_get_async()

            async for form_data in form_group.list_async(
                filter_by_state=["submitted_waiting_for_review"]
            ):
                status = form_data.submission_status
                print(f"Form name: {form_data.name}")
                print(f"State: {status.state.value}")
                print(f"Submitted on: {status.submitted_on}")
        asyncio.run(list_my_form_data())
        ```

        Examples: List all form data as a reviewer
        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormGroup
        import asyncio

        async def list_my_form_data():
            syn = Synapse()
            syn.login()

            form_group = await FormGroup(name="test").create_or_get_async()

            async for form_data in form_group.list_async(as_reviewer=True):
                status = form_data.submission_status
                print(f"Form name: {form_data.name}")
                print(f"State: {status.state.value}")
                print(f"Submitted on: {status.submitted_on}")

        asyncio.run(list_my_form_data())
        ```
        """
        from synapseclient.api import list_form_data

        if not self.group_id:
            raise ValueError(
                "'group_id' must be provided to list FormData within a form group."
            )

        if not filter_by_state:
            if as_reviewer:
                filter_by_state = self.DEFAULT_REVIEWER_STATES
            else:
                filter_by_state = self.DEFAULT_OWNER_STATES

        self._validate_filter_by_state(
            filter_by_state=filter_by_state,
            as_reviewer=as_reviewer,
        )

        filter_by_state_enum = self._convert_state_enum_strings(
            state_enum_list=filter_by_state
        )

        gen = list_form_data(
            synapse_client=synapse_client,
            group_id=self.group_id,
            filter_by_state=filter_by_state_enum,
            as_reviewer=as_reviewer,
        )
        async for item in gen:
            yield FormData().fill_from_dict(item)

    def list(
        self,
        *,
        filter_by_state: Optional[List[str]] = None,
        synapse_client: Optional["Synapse"] = None,
        as_reviewer: bool = False,
    ) -> Generator["FormData", None, None]:
        """
        List FormData objects in a FormGroup.

        Arguments:
            filter_by_state: Optional list of StateEnum to filter the results.
                When as_reviewer=False (default), valid values are:
                - waiting_for_submission
                - submitted_waiting_for_review
                - accepted
                - rejected

                When as_reviewer=True, valid values are:
                - submitted_waiting_for_review
                - accepted
                - rejected
                Note: waiting_for_submission is NOT allowed when as_reviewer=True.

            as_reviewer: If True, uses the reviewer endpoint (requires READ_PRIVATE_SUBMISSION
                permission). If False (default), lists only FormData owned by the caller.
            synapse_client: The Synapse client to use for the request.

        Yields:
            FormData objects matching the request.

        Raises:
            ValueError: If group_id is not set or filter_by_state contains invalid values.

        Examples: List your own form data

        ```python
        from synapseclient.models import FormGroup
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

        form_group = FormGroup(name="test").create_or_get()

        list_data = form_group.list(filter_by_state=["waiting_for_submission"], as_reviewer=False)

        for form_data in list_data:
            print(f"FormData ID: {form_data.form_data_id}, State: {form_data.submission_status.state.value}")
        ```

        Examples: List all form data as a reviewer

        ```python
        from synapseclient.models import FormGroup
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

        form_group = FormGroup(name="test").create_or_get()

        list_data = form_group.list(as_reviewer=True)

        for form_data in list_data:
            print(f"FormData ID: {form_data.form_data_id}, State: {form_data.submission_status.state.value}")
        ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=self.list_async,
            synapse_client=synapse_client,
            filter_by_state=filter_by_state,
            as_reviewer=as_reviewer,
        )


@dataclass
@async_to_sync
class FormData(FormDataMixin, FormDataProtocol):
    """Dataclass representing a FormData."""

    async def create_or_get_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "FormData":
        """
        Create or get a new FormData object. The caller will own the resulting object and will have access to read, update, and delete the FormData object.

        Arguments:
            synapse_client: The Synapse client to use for the request.

        Returns:
            A FormData object containing the details of the created form data.

        Examples: Create a FormData

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData, File
        import asyncio

        async def create_my_form_data():
            syn = Synapse()
            syn.login()

            file = await File(id="syn123", download_file=False).get_async()
            file_handle_id = file.file_handle.id

            form_data = FormData(
                group_id="123",
                name="my_form_data_name",
                data_file_handle_id=file_handle_id
            )
            form_data = await form_data.create_or_get_async()

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
        from synapseclient.api import create_form_data

        if not self.group_id or not self.name or not self.data_file_handle_id:
            raise ValueError(
                "Missing required fields: 'group_id', 'name', and 'data_file_handle_id' are required to create a FormData."
            )

        form_change_request = FormChangeRequest(
            name=self.name, file_handle_id=self.data_file_handle_id
        ).to_dict()
        response = await create_form_data(
            synapse_client=synapse_client,
            group_id=self.group_id,
            form_change_request=form_change_request,
        )
        return self.fill_from_dict(response)

    async def download_async(
        self,
        synapse_id: str,
        download_location: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Download the data file associated with this FormData object.

        Arguments:
            synapse_id: The Synapse ID of the entity that owns the file handle (e.g., "syn12345678").
            download_location: The directory where the file should be downloaded.
            synapse_client: The Synapse client to use for the request.

        Returns:
            The path to the downloaded file.

        Examples: Download form data file

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import File, FormData

        async def download_form_data():
            syn = Synapse()
            syn.login()

            file = await File(id="syn123", download_file=False).get_async()
            file_handle_id = file.file_handle.id

            path = await FormData(data_file_handle_id=file_handle_id).download_async(synapse_id="syn123")

            print(f"Downloaded to: {path}")


        asyncio.run(download_form_data())
        ```
        """

        from synapseclient.core.download.download_functions import (
            download_by_file_handle,
            ensure_download_location_is_directory,
        )

        client = Synapse.get_client(synapse_client=synapse_client)

        if not self.data_file_handle_id:
            raise ValueError("data_file_handle_id must be set to download the file.")

        if download_location:
            download_dir = ensure_download_location_is_directory(
                download_location=download_location
            )
        else:
            download_dir = client.cache.get_cache_dir(
                file_handle_id=self.data_file_handle_id
            )

        filename = f"SYNAPSE_FORM_{self.data_file_handle_id}.csv"

        path = await download_by_file_handle(
            file_handle_id=self.data_file_handle_id,
            synapse_id=synapse_id,
            entity_type="FileEntity",
            destination=os.path.join(download_dir, filename),
            synapse_client=client,
        )
        return path
