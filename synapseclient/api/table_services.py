"""
The purpose of this module is to provide any functions that are needed to interact with
columns in the Synapse REST API.
"""

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import Column


async def get_columns(
    table_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List["Column"]:
    """Call to synapse and set the annotations for the given input.

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


# TODO: Finish this function, this was copied out of the Synapse class and will be used to implement this API: https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableSchemaChangeRequest.html
# async def table_updates(
#     self,
#     table_id: str,
#     changes: List[dict] = [],
#     create_snapshot: bool = False,
#     comment: str = None,
#     label: str = None,
#     activity: str = None,
#     wait: bool = True,
# ) -> dict:
#     """
#     Creates view updates and snapshots

#     Arguments:
#         table:           The schema of the EntityView or its ID.
#         changes:         Array of Table changes
#         create_snapshot: Create snapshot
#         comment:         Optional snapshot comment.
#         label:           Optional snapshot label.
#         activity:        Optional activity ID applied to snapshot version.
#         wait:            True to wait for async table update to complete

#     Returns:
#         A Snapshot Response
#     """
#     snapshot_options = {
#         "snapshotComment": comment,
#         "snapshotLabel": label,
#         "snapshotActivityId": activity,
#     }
#     new_snapshot = {
#         key: value for key, value in snapshot_options.items() if value is not None
#     }
#     table_update_body = {
#         "changes": changes,
#         "createSnapshot": create_snapshot,
#         "snapshotOptions": new_snapshot,
#     }

#     uri = "/entity/{}/table/transaction/async".format(id_of(table))

#     if wait:
#         result = self._waitForAsync(uri, table_update_body)

#     else:
#         result = self.restPOST(
#             "{}/start".format(uri), body=json.dumps(table_update_body)
#         )

#     return result
