import json
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Optional

from synapseclient.api.api_client import rest_post_paginated_async
from synapseclient.core.async_utils import wrap_async_generator_to_sync_generator

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins.form import StateEnum


async def create_form_group(
    synapse_client: "Synapse",
    name: str,
) -> dict[str, Any]:
    """
    <https://rest-docs.synapse.org/rest/POST/form/group.html>
    Create a form group asynchronously.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        name: A globally unique name for the group. Required. Between 3 and 256 characters.

    Returns:
        A Form group object as a dictionary.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/FormGroup.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_post_async(uri=f"/form/group?name={name}", body={})


async def create_form_data(
    synapse_client: "Synapse",
    group_id: str,
    form_change_request: dict[str, Any],
) -> dict[str, Any]:
    """
    <https://rest-docs.synapse.org/rest/POST/form/data.html>
    Create a new FormData object. The caller will own the resulting object and will have access to read, update, and delete the FormData object.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        group_id: The ID of the form group.
        form_change_request: a dictionary of form change request matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/FormChangeRequest.html>.

    Returns:
        A Form data object as a dictionary.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/FormData.html>

    Note: The caller must have the SUBMIT permission on the FormGroup to create/update/submit FormData.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_post_async(
        uri=f"/form/data?groupId={group_id}",
        body=json.dumps(form_change_request),
    )


async def list_form_data(
    synapse_client: "Synapse",
    group_id: str,
    filter_by_state: Optional[list["StateEnum"]] = None,
    as_reviewer: bool = False,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    List FormData objects and their associated status that match the filters of the provided request.

    When as_reviewer=False: <https://rest-docs.synapse.org/rest/POST/form/data/list.html>
    Returns FormData objects owned by the caller. Only objects owned by the caller will be returned.

    When as_reviewer=True: <https://rest-docs.synapse.org/rest/POST/form/data/list/reviewer.html>
    Returns FormData objects for the entire group. This is used by service accounts to review submissions.
    Requires READ_PRIVATE_SUBMISSION permission on the FormGroup.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        group_id: The ID of the form group. Required.
        filter_by_state: Optional list of StateEnum values to filter the FormData objects.
            When as_reviewer=False (default), valid values are:
            - StateEnum.WAITING_FOR_SUBMISSION
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED
            If None, returns all FormData objects.

            When as_reviewer=True, valid values are:
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW (default if None)
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED
            Note: WAITING_FOR_SUBMISSION is NOT allowed when as_reviewer=True.

        as_reviewer: If True, uses the reviewer endpoint to list FormData for the entire group.
            If False (default), lists only FormData owned by the caller.

    Yields:
        A single page of FormData objects matching the request.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/ListResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    body: dict[str, Any] = {"groupId": group_id, "filterByState": filter_by_state}

    if as_reviewer:
        uri = "/form/data/list/reviewer"
    else:
        uri = "/form/data/list"

    async for item in rest_post_paginated_async(
        uri=uri,
        body=body,
        synapse_client=client,
    ):
        yield item


def list_form_data_sync(
    synapse_client: "Synapse",
    group_id: str,
    filter_by_state: Optional[list["StateEnum"]] = None,
    as_reviewer: bool = False,
) -> Generator[dict[str, Any], None, None]:
    """
    List FormData objects and their associated status that match the filters of the provided request.

    This is the synchronous version of list_form_data_async.

    When as_reviewer=False: <https://rest-docs.synapse.org/rest/POST/form/data/list.html>
    Returns FormData objects owned by the caller. Only objects owned by the caller will be returned.

    When as_reviewer=True: <https://rest-docs.synapse.org/rest/POST/form/data/list/reviewer.html>
    Returns FormData objects for the entire group. This is used by service accounts to review submissions.
    Requires READ_PRIVATE_SUBMISSION permission on the FormGroup.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        group_id: The ID of the form group. Required.
        filter_by_state: Optional list of StateEnum values to filter the FormData objects.
            When as_reviewer=False (default), valid values are:
            - StateEnum.WAITING_FOR_SUBMISSION
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED
            If None, returns all FormData objects.

            When as_reviewer=True, valid values are:
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW (default if None)
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED
            Note: WAITING_FOR_SUBMISSION is NOT allowed when as_reviewer=True.

        as_reviewer: If True, uses the reviewer endpoint to list FormData for the entire group.
            If False (default), lists only FormData owned by the caller.

    Yields:
        A single page of FormData objects matching the request.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/ListResponse.html>
    """
    return wrap_async_generator_to_sync_generator(
        list_form_data(
            synapse_client=synapse_client,
            group_id=group_id,
            filter_by_state=filter_by_state,
            as_reviewer=as_reviewer,
        )
    )
