import json
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Optional

from synapseclient.api.api_client import rest_post_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins.form import StateEnum


async def create_form_group_async(
    synapse_client: "Synapse",
    name: str,
) -> dict[str, Any]:
    """
    <https://rest-docs.synapse.org/rest/POST/form/group.html>
    Create a form group asynchronously.

    Args:
        synapse_client: The Synapse client to use for the request.
        name: A globally unique name for the group. Required. Between 3 and 256 characters.

    Returns:
        A Form group object as a dictionary.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/FormGroup.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_post_async(uri=f"/form/group?name={name}", body={})


async def create_form_data_async(
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


async def list_form_reviewer_async(
    synapse_client: "Synapse",
    group_id: str,
    filter_by_state: Optional[list["StateEnum"]] = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    <https://rest-docs.synapse.org/rest/POST/form/data/list/reviewer.html>
    List FormData objects in a FormGroup that are awaiting review.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        group_id: The ID of the form group.
        filter_by_state: List of StateEnum values to filter the FormData objects.
            Must include at least one element. Valid values are:
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED

    Yields:
        A single page of result matching the request
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/ListResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if not filter_by_state:
        raise ValueError("filter_by_state must include at least one StateEnum value.")

    async for item in rest_post_paginated_async(
        uri="/form/data/list/reviewer",
        body={
            "groupId": group_id,
            "filterByState": filter_by_state,
        },
        synapse_client=client,
    ):
        yield item


def list_form_reviewer_sync(
    synapse_client: "Synapse",
    group_id: str,
    filter_by_state: Optional[list["StateEnum"]] = None,
) -> Generator[dict[str, Any], None, None]:
    """
    <https://rest-docs.synapse.org/rest/POST/form/data/list/reviewer.html>
    List FormData objects in a FormGroup that are awaiting review.

    Arguments:
        synapse_client: The Synapse client to use for the request.
        group_id: The ID of the form group.
        filter_by_state: List of StateEnum values to filter the FormData objects.
            Must include at least one element. Valid values are:
            - StateEnum.SUBMITTED_WAITING_FOR_REVIEW
            - StateEnum.ACCEPTED
            - StateEnum.REJECTED

    Yields:
        A single page of result matching the request
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/form/ListResponse.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if not filter_by_state:
        raise ValueError("filter_by_state must include at least one StateEnum value.")

    for item in client._POST_paginated(
        uri="/form/data/list/reviewer",
        body={
            "groupId": group_id,
            "filterByState": filter_by_state,
        },
    ):
        yield item
