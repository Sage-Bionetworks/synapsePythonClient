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
