import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from synapseclient import Synapse


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
