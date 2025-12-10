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

    async def create_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "FormGroup":
        """
        Create a FormGroup with the provided name. This method is idempotent. If a group with the provided name already exists and the caller has ACCESS_TYPE.READ permission the existing FormGroup will be returned.

        Arguments:
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

        from synapseclient.api.form_services import create_form_group

        response = await create_form_group(
            synapse_client=synapse_client,
            name=self.name,
        )
        return self.fill_from_dict(response)


@dataclass
@async_to_sync
class FormData(FormDataMixin, FormDataProtocol):
    """Dataclass representing a FormData."""

    def _validate_filter_by_state(
        self,
        filter_by_state: List["StateEnum"],
        allow_waiting_submission: bool = True,
    ) -> None:
        """
        Validate filter_by_state values.

        Arguments:
            filter_by_state: List of StateEnum values to validate.
            allow_waiting_submission: If False, raises error if WAITING_FOR_SUBMISSION is present.
        """
        # Define valid states based on whether WAITING_FOR_SUBMISSION is allowed
        valid_states = {
            StateEnum.SUBMITTED_WAITING_FOR_REVIEW,
            StateEnum.ACCEPTED,
            StateEnum.REJECTED,
        }
        if allow_waiting_submission:
            valid_states.add(StateEnum.WAITING_FOR_SUBMISSION)

        # Check each state
        for state in filter_by_state:
            if not isinstance(state, StateEnum):
                valid_values = ", ".join(s.value for s in valid_states)
                raise ValueError(
                    f"Invalid state type. Expected StateEnum. Valid values are: {valid_values}"
                )

            if state not in valid_states:
                valid_values = ", ".join(s.value for s in valid_states)
                raise ValueError(
                    f"StateEnum.{state.value} is not allowed. Valid values are: {valid_values}"
                )

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

            file = await File(id="syn123", download_file=True).get_async()
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
        from synapseclient.api import create_form_data

        if not self.group_id or not self.name or not self.data_file_handle_id:
            raise ValueError(
                "'group_id', 'name', and 'data_file_handle_id' must be provided to create a FormData."
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

    @skip_async_to_sync
    async def list_async(
        self,
        *,
        filter_by_state: List["StateEnum"],
        synapse_client: Optional["Synapse"] = None,
        as_reviewer: bool = False,
    ) -> AsyncGenerator["FormData", None]:
        """
        List FormData objects in a FormGroup.

        Arguments:
            filter_by_state: list of StateEnum to filter the results.
                When as_reviewer=False (default), valid values are:
                - StateEnum.WAITING_FOR_SUBMISSION
                - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
                - StateEnum.ACCEPTED
                - StateEnum.REJECTED

                When as_reviewer=True, valid values are:
                - StateEnum.SUBMITTED_WAITING_FOR_REVIEW (default if None)
                - StateEnum.ACCEPTED
                - StateEnum.REJECTED
                Note: WAITING_FOR_SUBMISSION is NOT allowed when as_reviewer=True.
            synapse_client: The Synapse client to use for the request.

            as_reviewer: If True, uses the reviewer endpoint (requires READ_PRIVATE_SUBMISSION
                permission). If False (default), lists only FormData owned by the caller.

        Yields:
            FormData objects matching the request.

        Raises:
            ValueError: If group_id is not set or filter_by_state contains invalid values.

        Examples: List your own form data

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData
        from synapseclient.models.mixins.form import StateEnum
        import asyncio

        async def list_my_form_data():
            syn = Synapse()
            syn.login()

            async for form_data in FormData(group_id="123").list_async(
                filter_by_state=[StateEnum.SUBMITTED_WAITING_FOR_REVIEW]
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
        from synapseclient.models import FormData
        from synapseclient.models.mixins.form import StateEnum
        import asyncio

        async def list_for_review():
            syn = Synapse()
            syn.login()

            # List all submissions waiting for review (reviewer mode)
            async for form_data in FormData(group_id="123").list_async(
                as_reviewer=True,
                filter_by_state=[StateEnum.SUBMITTED_WAITING_FOR_REVIEW]
            ):
                status = form_data.submission_status
                print(f"Form name: {form_data.name}")
                print(f"State: {status.state.value}")
                print(f"Submitted on: {status.submitted_on}")

        asyncio.run(list_for_review())
        ```
        """
        from synapseclient.api import list_form_data

        if not self.group_id:
            raise ValueError("'group_id' must be provided to list FormData.")

        # Validate filter_by_state based on reviewer mode
        if as_reviewer:
            allow_waiting_submission = False
        else:
            allow_waiting_submission = True

        self._validate_filter_by_state(
            filter_by_state=filter_by_state,
            allow_waiting_submission=allow_waiting_submission,
        )

        gen = list_form_data(
            synapse_client=synapse_client,
            group_id=self.group_id,
            filter_by_state=filter_by_state,
            as_reviewer=as_reviewer,
        )
        async for item in gen:
            yield FormData().fill_from_dict(item)

    def list(
        self,
        *,
        filter_by_state: List["StateEnum"],
        synapse_client: Optional["Synapse"] = None,
        as_reviewer: bool = False,
    ) -> Generator["FormData", None, None]:
        """
        List FormData objects in a FormGroup.

        Arguments:
            filter_by_state: Optional list of StateEnum to filter the results.
                When as_reviewer=False (default), valid values are:
                - StateEnum.WAITING_FOR_SUBMISSION
                - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
                - StateEnum.ACCEPTED
                - StateEnum.REJECTED

                When as_reviewer=True, valid values are:
                - StateEnum.SUBMITTED_WAITING_FOR_REVIEW (default if None)
                - StateEnum.ACCEPTED
                - StateEnum.REJECTED
                Note: WAITING_FOR_SUBMISSION is NOT allowed when as_reviewer=True.

            as_reviewer: If True, uses the reviewer endpoint (requires READ_PRIVATE_SUBMISSION
                permission). If False (default), lists only FormData owned by the caller.
            synapse_client: The Synapse client to use for the request.

        Yields:
            FormData objects matching the request.

        Raises:
            ValueError: If group_id is not set or filter_by_state contains invalid values.

        Examples: List your own form data

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData
        from synapseclient.models.mixins.form import StateEnum

        syn = Synapse()
        syn.login()

        for form_data in FormData(group_id="123").list(
            filter_by_state=[StateEnum.SUBMITTED_WAITING_FOR_REVIEW]
        ):
            status = form_data.submission_status
            print(f"Form name: {form_data.name}")
            print(f"State: {status.state.value}")
            print(f"Submitted on: {status.submitted_on}")
        ```

        Examples: List all form data as a reviewer

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData
        from synapseclient.models.mixins.form import StateEnum

        syn = Synapse()
        syn.login()

        # List all submissions waiting for review (reviewer mode)
        for form_data in FormData(group_id="123").list(
            as_reviewer=True,
            filter_by_state=[StateEnum.SUBMITTED_WAITING_FOR_REVIEW]
        ):
            status = form_data.submission_status
            print(f"Form name: {form_data.name}")
            print(f"State: {status.state.value}")
            print(f"Submitted on: {status.submitted_on}")
        ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=self.list_async,
            synapse_client=synapse_client,
            filter_by_state=filter_by_state,
            as_reviewer=as_reviewer,
        )

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

            file = await File(id="syn123", download_file=True).get_async()
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
