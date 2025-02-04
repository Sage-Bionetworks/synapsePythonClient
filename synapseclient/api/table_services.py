"""
The purpose of this module is to provide any functions that are needed to interact with
columns in the Synapse REST API.
"""

import json
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import Column


async def get_columns(
    table_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List["Column"]:
    """Get the columns for a Table given the Table's ID.

    Arguments:
        table_id: The ID of the Table to get the columns for.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns: The annotations set in Synapse.
    """
    from synapseclient import Synapse
    from synapseclient.models import Column

    result = await Synapse.get_client(synapse_client=synapse_client).rest_get_async(
        f"/entity/{table_id}/column",
    )

    columns = []

    for column in result.get("results", []):
        columns.append(Column().fill_from_dict(synapse_column=column))

    return columns


async def post_columns(
    columns: List["Column"], *, synapse_client: Optional["Synapse"] = None
) -> List["Column"]:
    """Creates a batch of [synapseclient.models.table.Column][]'s within a single request.

    Arguments:
        columns: The columns to post to Synapse.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor
    """
    from synapseclient import Synapse

    column_values = [column.to_synapse_request() for column in columns]
    request_body = {
        "concreteType": "org.sagebionetworks.repo.model.ListWrapper",
        "list": list(column_values),
    }

    result = await Synapse.get_client(synapse_client=synapse_client).rest_post_async(
        "/column/batch", body=json.dumps(request_body)
    )

    # Fill the results back onto the original columns
    for i, column in enumerate(columns):
        column.fill_from_dict(result["list"][i])

    return columns
