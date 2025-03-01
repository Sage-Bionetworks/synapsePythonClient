import asyncio
import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient import Table as Synapse_Table
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_operator import (
    Column,
    TableOperator,
    TableRowOperator,
)
from synapseclient.models.protocols.table_protocol import TableSynchronousProtocol


@dataclass()
@async_to_sync
class Table(
    TableSynchronousProtocol, AccessControllable, TableOperator, TableRowOperator
):
    """A Table represents the metadata of a table.

    Attributes:
        id: The unique immutable ID for this table. A new ID will be generated for new
            Tables. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this table. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of this entity. Must be 1000 characters or less.
        parent_id: The ID of the Entity that is the parent of this table.
        columns: The columns of this table. This is an ordered dictionary where the key is the
            name of the column and the value is the Column object. When creating a new instance
            of a Table object you may pass any of the following types as the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the Column object
            - An OrderedDict where the key is the name of the column and the value is the Column object

            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your table class instance.

            You may modify the attributes of the Column object to change the column
            type, name, or other attributes. For example suppose I'd like to change a
            column from a INTEGER to a DOUBLE. I can do so by changing the column type
            attribute of the Column object. The next time you store the table the column
            will be updated in Synapse with the new type.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table, Column, ColumnType

            syn = Synapse()
            syn.login()

            table = Table(id="syn1234").get()
            table.columns["my_column"].column_type = ColumnType.DOUBLE
            table.store()
            ```

            Note that the keys in this dictionary should match the column names as they are in
            Synapse. However, know that the name attribute of the Column object is used for
            all interactions with the Synapse API. The OrderedDict key is purely for the usage
            of this interface. For example, if you wish to rename a column you may do so by
            changing the name attribute of the Column object. The key in the OrderedDict does
            not need to be changed. The next time you store the table the column will be updated
            in Synapse with the new name and the key in the OrderedDict will be updated.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: The date this table was created.
        created_by: The ID of the user that created this table.
        modified_on: The date this table was last modified.
            In YYYY-MM-DD-Thh:mm:ss.sssZ format
        modified_by: The ID of the user that last modified this table.
        version_number: (Read Only) The version number issued to this version on the
            object. Use this `.snapshot()` method to create a new version of the
            table.
        version_label: (Read Only) The version label for this table. Use the
            `.snapshot()` method to create a new version of the table.
        version_comment: (Read Only) The version comment for this table. Use the
            `.snapshot()` method to create a new version of the table.
        is_latest_version: (Read Only) If this is the latest version of the object.
        is_search_enabled: When creating or updating a table or view specifies if full
            text search should be enabled. Note that enabling full text search might
            slow down the indexing of the table or view.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance. Activity
            cannot be removed during a store operation by setting it to None. You must
            use: [synapseclient.models.Activity.delete_async][] or
            [synapseclient.models.Activity.disassociate_from_entity_async][].
        annotations: Additional metadata associated with the table. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list. To remove all annotations set this
            to an empty dict `{}` or None and store the entity.

    Example: Create a table with data without specifying columns
        This API is setup to allow the data to define which columns are created on the
        Synapse table automatically. The limitation with this behavior is that the
        columns created will only be of the following types:

        - STRING
        - LARGETEXT
        - INTEGER
        - DOUBLE
        - BOOLEAN
        - DATE

        The determination of the column type is based on the data that is passed in
        using the pandas function
        [infer_dtype](https://pandas.pydata.org/docs/reference/api/pandas.api.types.infer_dtype.html).
        If you need a more specific column type, or need to add options to the colums
        follow the examples below.

        ```python
        import pandas as pd

        from synapseclient import Synapse
        from synapseclient.models import Table, SchemaStorageStrategy

        syn = Synapse()
        syn.login()

        my_data = pd.DataFrame(
            {
                "my_string_column": ["a", "b", "c", "d"],
                "my_integer_column": [1, 2, 3, 4],
                "my_double_column": [1.0, 2.0, 3.0, 4.0],
                "my_boolean_column": [True, False, True, False],
            }
        )

        table = Table(
            name="my_table",
            parent_id="syn1234",
        ).store()

        table.store_rows(values=my_data, schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)

        # Prints out the stored data about this specific column
        print(table.columns["my_string_column"])
        ```

    Example: Rename an existing column
        This examples shows how you may retrieve a table from synapse, rename a column,
        and then store the table back in synapse.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Table

        syn = Synapse()
        syn.login()

        table = Table(
            name="my_table",
            parent_id="syn1234",
        ).get()

        # You may also get the table by id:
        table = Table(
            id="syn4567"
        ).get()

        table.columns["my_old_column"].name = "my_new_column"

        # Before the data is stored in synapse you'll still be able to use the old key to access the column entry
        print(table.columns["my_old_column"])

        table.store()

        # After the data is stored in synapse you'll be able to use the new key to access the column entry
        print(table.columns["my_new_column"])
        ```

    Example: Create a table with a list of columns
        A list of columns may be passed in when creating a new table. The order of the
        columns in the list will be the order they are stored in Synapse. If the table
        already exists and you create the Table instance in this way the columns will
        be appended to the end of the existing columns.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Column, ColumnType, Table

        syn = Synapse()
        syn.login()

        columns = [
            Column(name="my_string_column", column_type=ColumnType.STRING),
            Column(name="my_integer_column", column_type=ColumnType.INTEGER),
            Column(name="my_double_column", column_type=ColumnType.DOUBLE),
            Column(name="my_boolean_column", column_type=ColumnType.BOOLEAN),
        ]

        table = Table(
            name="my_table",
            parent_id="syn1234",
            columns=columns
        )

        table.store()
        ```


    Example: Creating a table with a dictionary of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Column, ColumnType, Table

        syn = Synapse()
        syn.login()

        columns = {
            "my_string_column": Column(column_type=ColumnType.STRING),
            "my_integer_column": Column(column_type=ColumnType.INTEGER),
            "my_double_column": Column(column_type=ColumnType.DOUBLE),
            "my_boolean_column": Column(column_type=ColumnType.BOOLEAN),
        }

        table = Table(
            name="my_table",
            parent_id="syn1234",
            columns=columns
        )

        table.store()
        ```

    Example: Creating a table with an OrderedDict of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

        ```python
        from collections import OrderedDict
        from synapseclient import Synapse
        from synapseclient.models import Column, ColumnType, Table

        syn = Synapse()
        syn.login()

        columns = OrderedDict({
            "my_string_column": Column(column_type=ColumnType.STRING),
            "my_integer_column": Column(column_type=ColumnType.INTEGER),
            "my_double_column": Column(column_type=ColumnType.DOUBLE),
            "my_boolean_column": Column(column_type=ColumnType.BOOLEAN),
        })

        table = Table(
            name="my_table",
            parent_id="syn1234",
            columns=columns
        )

        table.store()
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this table. A new ID will be generated for new
    Tables. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this table. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
    apostrophes, and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this table."""

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict, compare=False)
    """
    The columns of this table. This is an ordered dictionary where the key is the
    name of the column and the value is the Column object. When creating a new instance
    of a Table object you may pass any of the following types as the `columns` argument:

    - A list of Column objects
    - A dictionary where the key is the name of the column and the value is the Column object
    - An OrderedDict where the key is the name of the column and the value is the Column object

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your table class instance.

    You may modify the attributes of the Column object to change the column
    type, name, or other attributes. For example suppose I'd like to change a
    column from a INTEGER to a DOUBLE. I can do so by changing the column type
    attribute of the Column object. The next time you store the table the column
    will be updated in Synapse with the new type.

    ```python
    from synapseclient import Synapse
    from synapseclient.models import Table, Column, ColumnType

    syn = Synapse()
    syn.login()

    table = Table(id="syn1234").get()
    table.columns["my_column"].column_type = ColumnType.DOUBLE
    table.store()
    ```

    Note that the keys in this dictionary should match the column names as they are in
    Synapse. However, know that the name attribute of the Column object is used for
    all interactions with the Synapse API. The OrderedDict key is purely for the usage
    of this interface. For example, if you wish to rename a column you may do so by
    changing the name attribute of the Column object. The key in the OrderedDict does
    not need to be changed. The next time you store the table the column will be updated
    in Synapse with the new name and the key in the OrderedDict will be updated.
    """

    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)
    """
    Columns to delete when the table is stored. The key in this dict is the ID of the
    column to delete. The value is the Column object that represents the column to
    delete.
    """

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """The date this table was created."""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this table."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this table was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ format"""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this table."""

    version_number: Optional[int] = field(default=None, compare=False)
    """(Read Only) The version number issued to this version on the object. Use this
    `.snapshot()` method to create a new version of the table."""

    version_label: Optional[str] = None
    """(Read Only) The version label for this table. Use this `.snapshot()` method
    to create a new version of the table."""

    version_comment: Optional[str] = None
    """(Read Only) The version comment for this table. Use this `.snapshot()` method
    to create a new version of the table."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """(Read Only) If this is the latest version of the object."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a table or view specifies if full text search
    should be enabled. Note that enabling full text search might slow down the
    indexing of the table or view."""

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analygous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance. Activity cannot
    be removed during a store operation by setting it to None. You must use:
    [synapseclient.models.Activity.delete_async][] or
    [synapseclient.models.Activity.disassociate_from_entity_async][].
    """

    annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = field(default_factory=dict, compare=False)
    """Additional metadata associated with the table. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`
    or None and store the entity."""

    _last_persistent_instance: Optional["Table"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def __post_init__(self):
        """Post initialization of the Table object. This is used to set the columns
        attribute to an OrderedDict if it is a list or dict."""
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    @property
    def has_columns_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance
            or (not self._last_persistent_instance.columns and self.columns)
            or self._last_persistent_instance.columns != self.columns
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = dataclasses.replace(self)
        self._last_persistent_instance.activity = (
            dataclasses.replace(self.activity) if self.activity else None
        )
        self._last_persistent_instance.columns = (
            OrderedDict(
                (key, dataclasses.replace(column))
                for key, column in self.columns.items()
            )
            if self.columns
            else OrderedDict()
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

    def fill_from_dict(
        self, entity: Synapse_Table, set_annotations: bool = True
    ) -> "Table":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse API

        Returns:
            The Table object instance.
        """
        self.id = entity.get("id", None)
        self.name = entity.get("name", None)
        self.description = entity.get("description", None)
        self.parent_id = entity.get("parentId", None)
        self.etag = entity.get("etag", None)
        self.created_on = entity.get("createdOn", None)
        self.created_by = entity.get("createdBy", None)
        self.modified_on = entity.get("modifiedOn", None)
        self.modified_by = entity.get("modifiedBy", None)
        self.version_number = entity.get("versionNumber", None)
        self.version_label = entity.get("versionLabel", None)
        self.version_comment = entity.get("versionComment", None)
        self.is_latest_version = entity.get("isLatestVersion", None)
        self.is_search_enabled = entity.get("isSearchEnabled", False)

        if set_annotations:
            self.annotations = Annotations.from_dict(entity.get("annotations", {}))
        return self

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        entity = {
            "name": self.name,
            "description": self.description,
            "id": self.id,
            "etag": self.etag,
            "parentId": self.parent_id,
            "concreteType": concrete_types.TABLE_ENTITY,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isSearchEnabled": self.is_search_enabled,
            # When saving other (non-column) fields to Synapse we still need to pass
            # in the list of columns, otherwise Synapse will wipe out the columns. We
            # are using the last known columns to ensure that we are not losing any
            "columnIds": [
                column.id for column in self._last_persistent_instance.columns.values()
            ]
            if self._last_persistent_instance and self._last_persistent_instance.columns
            else [],
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    async def snapshot_async(
        self,
        comment: str = None,
        label: str = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Any]:
        """
        Request to create a new snapshot of a table. The provided comment, label, and
        activity will be applied to the current version thereby creating a snapshot
        and locking the current version. After the snapshot is created a new version
        will be started with an 'in-progress' label.

        Arguments:
            comment: Comment to add to this snapshot to the table.
            label: Label to add to this snapshot to the table. The label must be unique,
                if a label is not provided a unique label will be generated.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Table
                and calling the `store()` method on the Table instance. Adding an
                activity to a snapshot of a table is meant to capture the provenance of
                the data at the time of the snapshot.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the table. If False the activity will not be
                associated with the new version of the table.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Creating a snapshot of a table
            Comment and label are optional, but filled in for this example.

            ```python
            import asyncio
            from synapseclient.models import Table
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()


            async def main():
                my_table = Table(id="syn1234")
                await my_table.snapshot_async(
                    comment="This is a new snapshot comment",
                    label="3This is a unique label"
                )

            asyncio.run(main())
            ```

        Example: Including the activity (Provenance) in the snapshot and not pulling it forward to the new `in-progress` version of the table.
            By default this method is set up to include the activity in the snapshot and
            then pull the activity forward to the new version. If you do not want to
            include the activity in the snapshot you can set `include_activity` to
            False. If you do not want to pull the activity forward to the new version
            you can set `associate_activity_to_new_version` to False.

            See the [activity][synapseclient.models.Activity] attribute on the Table
            class for more information on how to interact with the activity.

            ```python
            import asyncio
            from synapseclient.models import Table
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()


            async def main():
                my_table = Table(id="syn1234")
                await my_table.snapshot_async(
                    comment="This is a new snapshot comment",
                    label="This is a unique label",
                    include_activity=True,
                    associate_activity_to_new_version=False
                )

            asyncio.run(main())
            ```

        Returns:
            A dictionary that matches: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SnapshotResponse.html>
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        # Ensure that we have seeded the table with the latest data
        await self.get_async(include_activity=True, synapse_client=client)
        client.logger.info(
            f"[{self.id}:{self.name}]: Creating a snapshot of the table."
        )

        loop = asyncio.get_event_loop()
        snapshot_response = await loop.run_in_executor(
            None,
            lambda: client._create_table_snapshot(
                table=self.id,
                comment=comment,
                label=label,
                activity=self.activity.id
                if self.activity and include_activity
                else None,
            ),
        )

        if associate_activity_to_new_version and self.activity:
            self._last_persistent_instance.activity = None
            await self.store_async(synapse_client=synapse_client)
        else:
            await self.get_async(include_activity=True, synapse_client=synapse_client)

        return snapshot_response
