"""
The purpose of this module is to provide any functions that are needed to interact with
columns in the Synapse REST API.
"""

import json
from enum import Enum
from typing import TYPE_CHECKING, AsyncGenerator, Dict, Generator, List, Optional, Union

from synapseclient.core.utils import delete_none_keys, id_of

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import Column


class ViewEntityType(str, Enum):
    """String enum representing the type of view. This is used to determine
    the default columns that are added to the table.
    As defined in the Synapse REST API:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ViewEntityType.html>
    """

    ENTITY_VIEW = "entityview"
    SUBMISSION_VIEW = "submissionview"
    DATASET = "dataset"
    DATASET_COLLECTION = "datasetcollection"


class ViewTypeMask(int, Enum):
    """Bit mask representing the types to include in the view.
    As defined in the Synapse REST API:
    <https://rest-docs.synapse.org/rest/GET/column/tableview/defaults.html>
    """

    FILE = 0x01
    PROJECT = 0x02
    TABLE = 0x04
    FOLDER = 0x08
    VIEW = 0x10
    DOCKER = 0x20
    SUBMISSION_VIEW = 0x40
    DATASET = 0x80
    DATASET_COLLECTION = 0x100
    MATERIALIZED_VIEW = 0x200


async def create_table_snapshot(
    table_id: str,
    comment: Optional[str] = None,
    label: Optional[str] = None,
    activity_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Creates a table snapshot using the Synapse REST API.

    Arguments:
        table_id: Table ID to create a snapshot for.
        comment: Optional snapshot comment.
        label: Optional snapshot label.
        activity_id: Optional activity ID or activity instance applied to snapshot version.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A dictionary containing the snapshot response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if activity_id and not isinstance(activity_id, str):
        activity_id = str(activity_id)

    snapshot_body = {
        "snapshotComment": comment,
        "snapshotLabel": label,
        "snapshotActivityId": activity_id,
    }

    delete_none_keys(snapshot_body)

    table_id = id_of(table_id)
    uri = f"/entity/{table_id}/table/snapshot"

    snapshot_response = await client.rest_post_async(
        uri, body=json.dumps(snapshot_body)
    )

    return snapshot_response


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


async def get_column(
    column_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """Get a single column by ID.

    Arguments:
        column_id: The ID of the column to retrieve.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns:
        The column matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ColumnModel.html>
    """
    from synapseclient import Synapse

    return await Synapse.get_client(synapse_client=synapse_client).rest_get_async(
        f"/column/{column_id}",
    )


async def list_columns(
    prefix: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator["Column", None]:
    """List columns with optional prefix filtering.

    Arguments:
        prefix: Optional prefix to filter columns by name.
        limit: Number of columns to retrieve per request to Synapse (pagination parameter).
            The function will continue retrieving results until all matching columns are returned.
        offset: The index of the first column to return (pagination parameter).
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Yields: Column instances.
    """
    from synapseclient.api.api_client import rest_get_paginated_async
    from synapseclient.models import Column

    if prefix is None:
        uri = "/column"
    else:
        uri = f"/column?prefix={prefix}"

    async for result in rest_get_paginated_async(
        uri, limit=limit, offset=offset, synapse_client=synapse_client
    ):
        yield Column().fill_from_dict(synapse_column=result)


def list_columns_sync(
    prefix: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Generator["Column", None, None]:
    """List columns with optional prefix filtering (synchronous version).

    Arguments:
        prefix: Optional prefix to filter columns by name.
        limit: Number of columns to retrieve per request to Synapse (pagination parameter).
            The function will continue retrieving results until all matching columns are returned.
        offset: The index of the first column to return (pagination parameter).
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Yields: Column instances.
    """
    from synapseclient import Synapse
    from synapseclient.models import Column

    client = Synapse.get_client(synapse_client=synapse_client)

    if prefix is None:
        uri = "/column"
    else:
        uri = f"/column?prefix={prefix}"

    for result in client._GET_paginated(uri, limit=limit, offset=offset):
        yield Column().fill_from_dict(synapse_column=result)


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


async def get_default_columns(
    view_entity_type: Optional[ViewEntityType] = None,
    view_type_mask: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List["Column"]:
    """Get the default columns for a given view type. This will query the following API:
    <https://rest-docs.synapse.org/rest/GET/column/tableview/defaults.html> in order to
    retrieve this information.

    If providing a view_type_mask, you can use the ViewTypeMask enum to get the
    appropriate value for the entity type you are interested in. ViewTypeMask values are
    hexadecimal values, so you can use the `|` operator to combine them.

    Example:
        To get the default columns for a Dataset, you can use:

        ```python
        import asyncio

        from synapseclient.api.table_services import ViewEntityType, ViewTypeMask, get_default_columns

        async def main():
            view_type_mask = ViewTypeMask.DATASET.value
            columns = await get_default_columns(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=view_type_mask,
            )
            print(columns)

        asyncio.run(main())
        ```

        To get the default columns for a File or a Folder, you can use:

        ```python
        import asyncio

        from synapseclient.api.table_services import ViewEntityType, ViewTypeMask, get_default_columns

        async def main():
            view_type_mask = ViewTypeMask.FILE.value | ViewTypeMask.FOLDER.value
            columns = await get_default_columns(
                view_entity_type=ViewEntityType.DATASET,
                view_type_mask=view_type_mask,
            )
            print(columns)

        asyncio.run(main())
        ```

    Arguments:
        view_type: The type of view to get the default columns for.
        view_type_mask: The type of view to get the default columns for. Not required
            in some cases like a submission view.
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

    Returns: The annotations set in Synapse.
    """

    from synapseclient import Synapse
    from synapseclient.models import Column

    uri = "/column/tableview/defaults"

    if view_entity_type and not view_type_mask:
        uri += f"?viewEntityType={view_entity_type.value}"
    if view_type_mask and not view_entity_type:
        uri += f"?viewTypeMask={view_type_mask}"
    if view_entity_type and view_type_mask:
        uri += f"?viewEntityType={view_entity_type.value}&viewTypeMask={view_type_mask}"

    result = await Synapse.get_client(synapse_client=synapse_client).rest_get_async(uri)

    return [
        Column().fill_from_dict(synapse_column=column)
        for column in result.get("list", [])
    ]
