from typing import TYPE_CHECKING, Generator, List, Optional, Protocol

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins import (
        FormGroup,
        FormData,
    )
    from synapseclient.models.mixins.form import StateEnum

from synapseclient.core.async_utils import wrap_async_generator_to_sync_generator


class FormGroupProtocol(Protocol):
    """Protocol for FormGroup operations."""

    def create(
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

        syn = Synapse()
        syn.login()

        form_group = FormGroup(name="my_unique_form_group_name")
        form_group = form_group.create()
        print(form_group)
        ```
        """
        return FormGroup()


class FormDataProtocol(Protocol):
    """Protocol for FormData operations."""

    def create(
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

        syn = Synapse()
        syn.login()

        file = File(id="syn123", download_file=True).get()
        file_handle_id = file.file_handle.id

        form_data = FormData(
            group_id="123",
            name="my_form_data_name",
            data_file_handle_id=file_handle_id
        )
        form_data = form_data.create()

        print(f"Created FormData: {form_data.form_data_id}")
        print(f"Name: {form_data.name}")
        print(f"Group ID: {form_data.group_id}")
        print(f"Created By: {form_data.created_by}")
        print(f"Created On: {form_data.created_on}")
        print(f"Data File Handle ID: {form_data.data_file_handle_id}")

        if form_data.submission_status:
            print(f"Submission State: {form_data.submission_status.state.value}")
        ```
        """
        return FormData()

    def list(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
        filter_by_state: Optional[List["StateEnum"]] = None,
        as_reviewer: bool = False,
    ) -> Generator["FormData", None, None]:
        """
        List FormData objects in a FormGroup.

        Arguments:
            synapse_client: The Synapse client to use for the request.
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

    def download(
        self,
        synapse_id: str,
        download_location: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Download the data file associated with this FormData object.

        Arguments:
            download_location: The directory where the file should be downloaded.
            synapse_id: The Synapse ID of the entity that owns the file handle (e.g., "syn12345678").
            synapse_client: The Synapse client to use for the request.

        Returns:
            The path to the downloaded file.

        Examples: Download form data file

        ```python
        from synapseclient import Synapse
        from synapseclient.models import FormData

        syn = Synapse()
        syn.login()

        form_data = FormData(form_data_id="123").get()  # First get the FormData
        path = form_data.download(  # Then download
            synapse_id="syn12345678",
            download_location="/tmp"
        )
        print(f"Downloaded to: {path}")
        ```
        """
        return str()
