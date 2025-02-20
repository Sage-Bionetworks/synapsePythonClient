"""Mixin for objects that may perform table like operations such as managing columns
or querying for data."""

import asyncio
import json
import logging
import os
import tempfile
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Optional, TypeVar, Union

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from typing_extensions import Self

from synapseclient import Column as Synapse_Column
from synapseclient import Synapse
from synapseclient.api import (
    delete_entity,
    get_columns,
    get_from_entity_factory,
    post_columns,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants import concrete_types
from synapseclient.core.upload.multipart_upload_async import multipart_upload_file_async
from synapseclient.core.utils import (
    MB,
    delete_none_keys,
    log_dataclass_diff,
    merge_dataclass_entities,
)
from synapseclient.models import Activity
from synapseclient.models.mixins import AsynchronousCommunicator
from synapseclient.models.protocols.table_operator_protocol import (
    TableOperatorSynchronousProtocol,
    TableRowOperatorSynchronousProtocol,
)
from synapseclient.models.protocols.table_protocol import ColumnSynchronousProtocol
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)

CLASSES_THAT_CONTAIN_ROW_ETAG = ["Dataset"]

PANDAS_TABLE_TYPE = {
    "floating": "DOUBLE",
    "decimal": "DOUBLE",
    "integer": "INTEGER",
    "mixed-integer-float": "DOUBLE",
    "boolean": "BOOLEAN",
    "datetime64": "DATE",
    "datetime": "DATE",
    "date": "DATE",
}


DEFAULT_QUOTE_CHARACTER = '"'
DEFAULT_SEPARATOR = ","
DEFAULT_ESCAPSE_CHAR = "\\"

# Taken from <https://github.com/Sage-Bionetworks/Synapse-Repository-Services/blob/cce01ec2c9f8ae44dabe957ca70e87942431aff5/lib/models/src/main/java/org/sagebionetworks/repo/model/table/TableConstants.java#L77>
RESERVED_COLUMN_NAMES = [
    "ROW_ID",
    "ROW_VERSION",
    "ROW_ETAG",
    "ROW_BENEFACTOR",
    "ROW_SEARCH_CONTENT",
    "ROW_HASH_CODE",
]

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")
SERIES_TYPE = TypeVar("pd.Series")


def test_import_pandas() -> None:
    try:
        import pandas as pd  # noqa F401
    # used to catch when pandas isn't installed
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            """\n\nThe pandas package is required for this function!\n
        Most functions in the synapseclient package don't require the
        installation of pandas, but some do. Please refer to the installation
        instructions at: http://pandas.pydata.org/ or
        https://python-docs.synapse.org/tutorials/installation/#installation-guide-for-pypi-users.
        \n\n\n"""
        )
    # catch other errors (see SYNPY-177)
    except:  # noqa
        raise


@dataclass
class SumFileSizes:
    sum_file_size_bytes: int
    """The sum of the file size in bytes."""

    greater_than: bool
    """When true, the actual sum of the files sizes is greater than the value provided with 'sum_file_size_bytes'. When false, the actual sum of the files sizes is equals the value provided with 'sum_file_size_bytes'"""


@dataclass
class QueryResultBundle:
    """
    The result of querying Synapse with an included `part_mask`. This class contains a
    subnet of the available items that may be returned by specifying a `part_mask`.


    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryResultBundle.html>
    """

    result: "DATA_FRAME_TYPE"
    """The result of the query"""

    count: Optional[int] = None
    """The total number of rows that match the query. Use mask = 0x2 to include in the
    bundle."""

    sum_file_sizes: Optional[SumFileSizes] = None
    """The sum of the file size for all files in the given view query. Use mask = 0x40
    to include in the bundle."""

    last_updated_on: Optional[str] = None
    """The date-time when this table/view was last updated. Note: Since views are
    eventually consistent a view might still be out-of-date even if it was recently
    updated. Use mask = 0x80 to include in the bundle."""


@async_to_sync
class TableOperator(TableOperatorSynchronousProtocol):
    """Mixin that extends the functionality of any `table` like entities in Synapse
    to perform a number of operations on the entity such as getting, deleting, or
    updating columns, querying for data, and more."""

    id: None = None
    name: None = None
    parent_id: None = None
    activity: None = None
    version_number: None = None
    _last_persistent_instance: None = None
    _columns_to_delete: Dict = None

    def _set_last_persistent_instance(self) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    def to_synapse_request(self) -> Dict:
        """Used to satisfy the usage in this mixin from the parent class."""

    def fill_from_dict(self, entity: Dict, set_annotations: bool) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    @property
    def has_changed(self) -> bool:
        """Used to satisfy the usage in this mixin from the parent class."""

    @property
    def has_columns_changed(self) -> bool:
        """Used to satisfy the usage in this mixin from the parent class."""

    async def _generate_schema_change_request(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
    ) -> Union["TableSchemaChangeRequest", None]:
        """
        Create a `TableSchemaChangeRequest` object that will be used to update the
        schema of the table. This method will only create a `TableSchemaChangeRequest`
        if the columns have changed. If the columns have not changed this method will
        return `None`. Since columns are idompotent, the columns will always be stored
        to Synapse if there is a change, but the table will not be updated if `dry_run`
        is set to `True`.

        Arguments:
            dry_run: If True, will not actually store the table but will log to
                the console what would have been stored.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A `TableSchemaChangeRequest` object that will be used to update the schema
            of the table. If there are no changes to the columns this method will
            return `None`.
        """
        if not self.has_columns_changed or not self.columns:
            return None

        column_name_to_id = {}
        column_changes = []
        client = Synapse.get_client(synapse_client=synapse_client)
        # This portion of the code is checking if the content of the Column has
        # changed, and if it has, the column will be stored in Synapse and a
        # `ColumnChange` will be created to track the changes and submit it as
        # part of the `TableSchemaChangeRequest`
        columns_to_persist = []
        for column in self.columns.values():
            if column.has_changed:
                if (
                    column._last_persistent_instance
                    and column._last_persistent_instance.id
                ):
                    column_name_to_id[column.name] = column._last_persistent_instance.id

                if (
                    column._last_persistent_instance
                    and column._last_persistent_instance.id
                ):
                    log_dataclass_diff(
                        logger=client.logger,
                        prefix=f"[{self.id}:{self.name}:Column_{column.name}]: ",
                        obj1=column._last_persistent_instance,
                        obj2=column,
                        fields_to_ignore=["_last_persistent_instance", "id"],
                    )
                else:
                    client.logger.info(
                        f"[{self.id}:{self.name}:Column_{column.name} (Add)]: {column}"
                    )
                if not dry_run:
                    columns_to_persist.append(column)

        if columns_to_persist:
            await post_columns(
                columns=columns_to_persist, synapse_client=synapse_client
            )
            for column in columns_to_persist:
                old_id = column_name_to_id.get(column.name, None)
                if not old_id:
                    column_changes.append(ColumnChange(new_column_id=column.id))
                elif old_id != column.id:
                    column_changes.append(
                        ColumnChange(old_column_id=old_id, new_column_id=column.id)
                    )

        if self._columns_to_delete:
            for column in self._columns_to_delete.values():
                column_changes.append(ColumnChange(old_column_id=column.id))

        order_of_existing_columns = (
            [column.id for column in self._last_persistent_instance.columns.values()]
            if self._last_persistent_instance and self._last_persistent_instance.columns
            else []
        )
        order_of_new_columns = []

        for column in self.columns.values():
            if (
                not self._columns_to_delete
                or column.id not in self._columns_to_delete.keys()
            ):
                order_of_new_columns.append(column.id)

        if (order_of_existing_columns != order_of_new_columns) or column_changes:
            # To be human readable we're using the names of the columns,
            # however, it's slightly incorrect as a replacement of a column
            # might have occurred if a field of the column was modified
            # since columns are immutable after creation each column
            # modification recieves a new ID.
            order_of_existing_column_names = (
                [
                    column.name
                    for column in self._last_persistent_instance.columns.values()
                ]
                if self._last_persistent_instance
                and self._last_persistent_instance.columns
                else []
            )
            order_of_new_column_names = [
                column.name for column in self.columns.values()
            ]
            columns_being_deleted = (
                [column.name for column in self._columns_to_delete.values()]
                if self._columns_to_delete
                else []
            )
            if columns_being_deleted:
                client.logger.info(
                    f"[{self.id}:{self.name}]: (Columns Being Deleted): {columns_being_deleted}"
                )
            if order_of_existing_column_names != order_of_new_column_names:
                client.logger.info(
                    f"[{self.id}:{self.name}]: (Column Order): "
                    f"{[column.name for column in self.columns.values()]}"
                )
            return TableSchemaChangeRequest(
                entity_id=self.id,
                changes=column_changes,
                ordered_column_ids=order_of_new_columns,
            )
        return None

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__}_Store: {self.name}"
    )
    async def store_async(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
    ) -> "Self":
        """Store non-row information about a table including the columns and annotations.


        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the contruction of your `Table` instance, ie.
            `Table(columns=[Column(name="foo")])`, they will be added at the begining
            of the columns list.
        - If you use the `store_rows` method and the `schema_storage_strategy` is set to
            `INFER_FROM_DATA` the columns will be added at the end of the columns list.


        Arguments:
            dry_run: If True, will not actually store the table but will log to
                the console what would have been stored.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Table instance stored in synapse.
        """
        client = Synapse.get_client(synapse_client=synapse_client)

        if (
            (not self._last_persistent_instance)
            and (
                existing_id := await get_id(
                    entity=self, synapse_client=synapse_client, failure_strategy=None
                )
            )
            and (
                existing_entity := await self.__class__(id=existing_id).get_async(
                    include_columns=True, synapse_client=synapse_client
                )
            )
        ):
            merge_dataclass_entities(
                source=existing_entity,
                destination=self,
            )

        if dry_run:
            client.logger.info(
                f"[{self.id}:{self.name}]: Dry run enabled. No changes will be made."
            )

        if self.has_changed:
            if self.id:
                if dry_run:
                    client.logger.info(
                        f"[{self.id}:{self.name}]: Dry run {self.__class__} update, expected changes:"
                    )
                    log_dataclass_diff(
                        logger=client.logger,
                        prefix=f"[{self.id}:{self.name}]: ",
                        obj1=self._last_persistent_instance,
                        obj2=self,
                        fields_to_ignore=["columns", "_last_persistent_instance"],
                    )
                else:
                    entity = await put_entity_id_bundle2(
                        entity_id=self.id,
                        request=self.to_synapse_request(),
                        synapse_client=synapse_client,
                    )
                    self.fill_from_dict(entity=entity["entity"], set_annotations=False)
            else:
                if dry_run:
                    client.logger.info(
                        f"[{self.id}:{self.name}]: Dry run {self.__class__} update, expected changes:"
                    )
                    log_dataclass_diff(
                        logger=client.logger,
                        prefix=f"[{self.name}]: ",
                        obj1=self.__class__(),
                        obj2=self,
                        fields_to_ignore=["columns", "_last_persistent_instance"],
                    )
                else:
                    entity = await post_entity_bundle2_create(
                        request=self.to_synapse_request(), synapse_client=synapse_client
                    )
                    self.fill_from_dict(entity=entity["entity"], set_annotations=False)

        schema_change_request = await self._generate_schema_change_request(
            dry_run=dry_run, synapse_client=synapse_client
        )

        if dry_run:
            return self

        if schema_change_request:
            await TableUpdateTransaction(
                entity_id=self.id, changes=[schema_change_request]
            ).send_job_and_wait_async(synapse_client=client)

            # Replace the columns after a schema change in case any column names were updated
            updated_columns = OrderedDict()
            for column in self.columns.values():
                updated_columns[column.name] = column
            self.columns = updated_columns

        re_read_required = await store_entity_components(
            root_resource=self,
            synapse_client=synapse_client,
            failure_strategy=FailureStrategy.RAISE_EXCEPTION,
        )
        if re_read_required:
            await self.get_async(
                include_columns=False,
                synapse_client=synapse_client,
            )
        self._set_last_persistent_instance()

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__}_Get: {self.name}"
    )
    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the table from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the file
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Table instance stored in synapse.

        Example: Getting metadata about a table using id
            Get a table by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get_async` call, then you'll make the changes, and finally call the
            `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(id="syn4567").get_async(include_activity=True)
                print(table)

                # Columns are retrieved by default
                print(table.columns)
                print(table.activity)

            asyncio.run(main())
            ```

        Example: Getting metadata about a table using name and parent_id
            Get a table by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get_async` call, then you'll make the changes,
            and finally call the `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(name="my_table", parent_id="syn1234").get_async(include_columns=True, include_activity=True)
                print(table)
                print(table.columns)
                print(table.activity)

            asyncio.run(main())
            ```
        """
        if not (self.id or (self.name and self.parent_id)):
            raise ValueError(
                "The table must have an id or a " "(name and `parent_id`) set."
            )

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await get_from_entity_factory(
            entity_to_update=self,
            synapse_id_or_path=entity_id,
            synapse_client=synapse_client,
        )

        if include_columns:
            column_instances = await get_columns(
                table_id=self.id, synapse_client=synapse_client
            )
            for column in column_instances:
                if column.name not in self.columns:
                    self.columns[column.name] = column

        if include_activity:
            self.activity = await Activity.from_parent_async(
                parent=self, synapse_client=synapse_client
            )

        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__}_Delete: {self.name}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the entity from synapse. This is not version specific. If you'd like
        to delete a specific version of the entity you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a table
            Deleting a table is only supported by the ID of the table.

            ```python
            import asyncio
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            async def main():
                await Table(id="syn4567").delete_async()

            asyncio.run(main())
            ```
        """
        if not (self.id or (self.name and self.parent_id)):
            raise ValueError(
                "The table must have an id or a " "(name and `parent_id`) set."
            )

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await delete_entity(
            entity_id=entity_id,
            synapse_client=synapse_client,
        )

    def delete_column(self, name: str) -> None:
        """
        Mark a column for deletion. Note that this does not delete the column from
        Synapse. You must call the `.store()` function on this table class instance to
        delete the column from Synapse. This is a convenience function to eliminate
        the need to manually delete the column from the dictionary and add it to the
        `._columns_to_delete` attribute.

        Arguments:
            name: The name of the column to delete.

        Returns:
            None

        Example: Deleting a column
            This example shows how you may delete a column from a table and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            table = Table(
                id="syn1234"
            ).get(include_columns=True)

            table.delete_column(name="my_column")
            table.store()
            ```

        Example: Deleting a column (async)
            This example shows how you may delete a column from a table and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(
                    id="syn1234"
                ).get_async(include_columns=True)

                table.delete_column(name="my_column")
                table.store_async()

            asyncio.run(main())
            ```
        """
        if not self._last_persistent_instance:
            raise ValueError(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            )
        if not self.columns:
            raise ValueError(
                "There are no columns. Make sure you use the `include_columns` parameter in the `.get()` method."
            )

        column_to_delete = self.columns.get(name, None)
        if not column_to_delete:
            raise ValueError(f"Column with name {name} does not exist in the table.")

        self._columns_to_delete[column_to_delete.id] = column_to_delete
        self.columns.pop(column_to_delete.name, None)

    def add_column(
        self, column: Union["Column", List["Column"]], index: int = None
    ) -> None:
        """Add column(s) to the table. Note that this does not store the column(s) in
        Synapse. You must call the `.store()` function on this table class instance to
        store the column(s) in Synapse. This is a convenience function to eliminate
        the need to manually add the column(s) to the dictionary.


        This function will add an item to the `.columns` attribute of this class
        instance. `.columns` is a dictionary where the key is the name of the column
        and the value is the Column object.

        Arguments:
            column: The column(s) to add, may be a single Column object or a list of
                Column objects.
            index: The index to insert the column at. If not passed in the column will
                be added to the end of the list.

        Returns:
            None

        Example: Adding a single column
            This example shows how you may add a single column to a table and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            table = Table(
                id="syn1234"
            ).get(include_columns=True)

            table.add_column(
                Column(name="my_column", column_type=ColumnType.STRING)
            )
            table.store()
            ```


        Example: Adding multiple columns
            This example shows how you may add multiple columns to a table and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            table = Table(
                id="syn1234"
            ).get(include_columns=True)

            table.add_column([
                Column(name="my_column", column_type=ColumnType.STRING),
                Column(name="my_column2", column_type=ColumnType.INTEGER),
            ])
            table.store()
            ```

        Example: Adding a column at a specific index
            This example shows how you may add a column at a specific index to a table
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            table = Table(
                id="syn1234"
            ).get(include_columns=True)

            table.add_column(
                Column(name="my_column", column_type=ColumnType.STRING),
                # Add the column at the beginning of the list
                index=0
            )
            table.store()
            ```

        Example: Adding a single column (async)
            This example shows how you may add a single column to a table and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(
                    id="syn1234"
                ).get_async(include_columns=True)

                table.add_column(
                    Column(name="my_column", column_type=ColumnType.STRING)
                )
                await table.store_async()

            asyncio.run(main())
            ```

        Example: Adding multiple columns (async)
            This example shows how you may add multiple columns to a table and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(
                    id="syn1234"
                ).get_async(include_columns=True)

                table.add_column([
                    Column(name="my_column", column_type=ColumnType.STRING),
                    Column(name="my_column2", column_type=ColumnType.INTEGER),
                ])
                await table.store_async()

            asyncio.run(main())
            ```

        Example: Adding a column at a specific index (async)
            This example shows how you may add a column at a specific index to a table
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(
                    id="syn1234"
                ).get_async(include_columns=True)

                table.add_column(
                    Column(name="my_column", column_type=ColumnType.STRING),
                    # Add the column at the beginning of the list
                    index=0
                )
                await table.store_async()

            asyncio.run(main())
            ```
        """
        if not self._last_persistent_instance:
            raise ValueError(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            )

        if index is not None:
            if isinstance(column, list):
                columns_to_insert = []
                for i, col in enumerate(column):
                    if col.name in self.columns:
                        raise ValueError(f"Duplicate column name: {col.name}")
                    columns_to_insert.append((col.name, col))
                insert_index = min(index + i, len(self.columns))
                self.columns = OrderedDict(
                    list(self.columns.items())[:insert_index]
                    + columns_to_insert
                    + list(self.columns.items())[insert_index:]
                )
            else:
                if column.name in self.columns:
                    raise ValueError(f"Duplicate column name: {column.name}")
                insert_index = min(index, len(self.columns))
                self.columns = OrderedDict(
                    list(self.columns.items())[:insert_index]
                    + [(column.name, column)]
                    + list(self.columns.items())[insert_index:]
                )

        else:
            if isinstance(column, list):
                for col in column:
                    if col.name in self.columns:
                        raise ValueError(f"Duplicate column name: {col.name}")
                    self.columns[col.name] = col
            else:
                if column.name in self.columns:
                    raise ValueError(f"Duplicate column name: {column.name}")
                self.columns[column.name] = column

    def reorder_column(self, name: str, index: int) -> None:
        """Reorder a column in the table. Note that this does not store the column in
        Synapse. You must call the `.store()` function on this table class instance to
        store the column in Synapse. This is a convenience function to eliminate
        the need to manually reorder the `.columns` attribute dictionary.

        You must ensure that the index is within the bounds of the number of columns in
        the table. If you pass in an index that is out of bounds the column will be
        added to the end of the list.

        Arguments:
            name: The name of the column to reorder.
            index: The index to move the column to starting with 0.

        Returns:
            None

        Example: Reordering a column
            This example shows how you may reorder a column in a table and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            table = Table(
                id="syn1234"
            ).get(include_columns=True)

            # Move the column to the beginning of the list
            table.reorder_column(name="my_column", index=0)
            table.store()
            ```


        Example: Reordering a column (async)
            This example shows how you may reorder a column in a table and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            async def main():
                table = await Table(
                    id="syn1234"
                ).get_async(include_columns=True)

                # Move the column to the beginning of the list
                table.reorder_column(name="my_column", index=0)
                table.store_async()

            asyncio.run(main())
            ```
        """
        if not self._last_persistent_instance:
            raise ValueError(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            )

        column_to_reorder = self.columns.pop(name, None)
        if index >= len(self.columns):
            self.columns[name] = column_to_reorder
            return self

        self.columns = OrderedDict(
            list(self.columns.items())[:index]
            + [(name, column_to_reorder)]
            + list(self.columns.items())[index:]
        )

    @staticmethod
    def _convert_columns_to_ordered_dict(
        columns: Union[
            List["Column"], OrderedDict[str, "Column"], Dict[str, "Column"], None
        ]
    ) -> OrderedDict[str, "Column"]:
        """Converts the columns attribute to an OrderedDict if it is a list or dict."""
        if not columns:
            return OrderedDict()

        if isinstance(columns, list):
            results = OrderedDict()
            for column in columns:
                if column.name in results:
                    raise ValueError(f"Duplicate column name: {column.name}")
                results[column.name] = column
            return results
        elif isinstance(columns, dict):
            results = OrderedDict()
            for key, column in columns.items():
                if column.name:
                    if column.name in results:
                        raise ValueError(f"Duplicate column name: {column.name}")
                    results[column.name] = column
                else:
                    column.name = key
                    if key in results:
                        raise ValueError(f"Duplicate column name: {key}")
                    results[key] = column
            return results
        elif isinstance(columns, OrderedDict):
            results = OrderedDict()
            for key, column in columns.items():
                if column.name:
                    if column.name in results:
                        raise ValueError(f"Duplicate column name: {column.name}")
                    results[column.name] = column
                else:
                    column.name = key
                    if key in results:
                        raise ValueError(f"Duplicate column name: {key}")
                    results[key] = column
            return results

        else:
            raise ValueError("columns must be a list, dict, or OrderedDict")

    @staticmethod
    async def query_async(
        query: str,
        include_row_id_and_row_version: bool = True,
        convert_to_datetime: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        **kwargs,
    ) -> DATA_FRAME_TYPE:
        """Query for data on a table stored in Synapse. The results will always be
        returned as a Pandas DataFrame.

        Arguments:
            query: The query to run. The query must be valid syntax that Synapse can
                understand. See this document that describes the expected syntax of the
                query:
                <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>
            include_row_id_and_row_version: If True the `ROW_ID` and `ROW_VERSION`
                columns will be returned in the DataFrame. These columns are required
                if using the query results to update rows in the table. These columns
                are the primary keys used by Synapse to uniquely identify rows in the
                table.
            convert_to_datetime: If set to True, will convert all Synapse DATE columns
                from UNIX timestamp integers into UTC datetime objects

            **kwargs: Additional keyword arguments to pass to pandas.read_csv. See
                    <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>
                    for complete list of supported arguments. This is exposed as
                    internally the query downloads a CSV from Synapse and then loads
                    it into a dataframe.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.

        Example: Querying for data
            This example shows how you may query for data in a table and print out the
            results.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import query_async

            syn = Synapse()
            syn.login()

            async def main():
                results = await query_async(query="SELECT * FROM syn1234")
                print(results)

            asyncio.run(main())
            ```
        """
        loop = asyncio.get_event_loop()

        client = Synapse.get_client(synapse_client=synapse_client)

        if client.logger.isEnabledFor(logging.DEBUG):
            client.logger.debug(f"Running query: {query}")

        # TODO: Implementation should not download CSV to disk, instead the ideal
        # solution will load the result into BytesIO and then pass that to
        # pandas.read_csv. During implmentation a determination on how large of a CSV
        # that can be loaded from Memory will be needed. When that limit is reached we
        # should continue to force the download of those results to disk.
        results = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).tableQuery(
                query=query,
                includeRowIdAndRowVersion=include_row_id_and_row_version,
            ),
        )
        return results.asDataFrame(
            rowIdAndVersionInIndex=False,
            convert_to_datetime=convert_to_datetime,
            **kwargs,
        )

    @staticmethod
    async def query_part_mask_async(
        query: str,
        part_mask: int,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> QueryResultBundle:
        """Query for data on a table stored in Synapse. This is a more advanced use case
        of the `query` function that allows you to determine what addiitional metadata
        about the table or query should also be returned. If you do not need this
        additional information then you are better off using the `query` function.

        The query for this method uses this Rest API:
        <https://rest-docs.synapse.org/rest/POST/entity/id/table/query/async/start.html>

        Arguments:
            query: The query to run. The query must be valid syntax that Synapse can
                understand. See this document that describes the expected syntax of the
                query:
                <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>
            part_mask: The bitwise OR of the part mask values you want to return in the
                results. The following list of part masks are implemented to be returned
                in the results:

                - Query Results (queryResults) = 0x1
                - Query Count (queryCount) = 0x2
                - The sum of the file sizes (sumFileSizesBytes) = 0x40
                - The last updated on date of the table (lastUpdatedOn) = 0x80

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.

        Example: Querying for data with a part mask
            This example shows how to use the bitwise `OR` of Python to combine the
            part mask values and then use that to query for data in a table and print
            out the results.

            In this case we are getting the results of the query, the count of rows, and
            the last updated on date of the table.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import query_part_mask_async

            syn = Synapse()
            syn.login()

            QUERY_RESULTS = 0x1
            QUERY_COUNT = 0x2
            LAST_UPDATED_ON = 0x80

            # Combine the part mask values using bitwise OR
            part_mask = QUERY_RESULTS | QUERY_COUNT | LAST_UPDATED_ON


            async def main():
                result = await query_part_mask_async(query="SELECT * FROM syn1234", part_mask=part_mask)
                print(result)

            asyncio.run(main())
            ```
        """
        loop = asyncio.get_event_loop()

        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.info(f"Running query: {query}")

        results = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).tableQuery(
                query=query,
                resultsAs="rowset",
                partMask=part_mask,
            ),
        )

        as_df = await loop.run_in_executor(
            None,
            lambda: results.asDataFrame(rowIdAndVersionInIndex=False),
        )
        return QueryResultBundle(
            result=as_df,
            count=results.count,
            sum_file_sizes=results.sumFileSizes,
            last_updated_on=results.lastUpdatedOn,
        )


@dataclass
class CsvTableDescriptor:
    """Derived from <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/CsvTableDescriptor.html>"""

    separator: str = ","
    """The delimiter to be used for separating entries in the resulting file. The default character ',' will be used if this is not provided by the caller. For tab-separated values use '\t'"""

    quote_character: str = '"'
    """The character to be used for quoted elements in the resulting file. The default character '"' will be used if this is not provided by the caller."""

    escape_character: str = "\\"
    """The escape character to be used for escaping a separator or quote in the resulting file. The default character '\\' will be used if this is not provided by the caller."""

    line_end: str = os.linesep
    """The line feed terminator to be used for the resulting file. The default value of '\n' will be used if this is not provided by the caller."""

    is_file_line_header: bool = True
    """Is the first line a header? The default value of 'true' will be used if this is not provided by the caller."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        request = {
            "separator": self.separator,
            "quoteCharacter": self.quote_character,
            "escapeCharacter": self.escape_character,
            "lineEnd": self.line_end,
            "isFirstLineHeader": self.is_file_line_header,
        }
        delete_none_keys(request)
        return request


@dataclass
class PartialRow:
    """
    A partial row to be added to a table. This is used in the `PartialRowSet` to
    indicate what rows should be updated in a table during the upsert operation.
    """

    row_id: str
    values: List[Dict[str, Any]]
    etag: Optional[str] = None

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        result = {
            "etag": self.etag,
            "rowId": self.row_id,
            "values": self.values,
        }
        delete_none_keys(result)
        return result

    def size(self) -> int:
        """
        Returns the size of the PartialRow in bytes. This is not an exact size but
        follows the calculation as used in the Rest API:

        <https://github.com/Sage-Bionetworks/Synapse-Repository-Services/blob/8bf7f60c46b76625c0d4be33fafc5cf896e50b36/lib/lib-table-cluster/src/main/java/org/sagebionetworks/table/cluster/utils/TableModelUtils.java#L952-L965>
        """
        char_count = 0
        if self.values:
            for value in self.values:
                char_count += len(value["key"])
                if value["value"] is not None:
                    char_count += len(str(value["value"]))
        return 4 * char_count


@dataclass
class PartialRowSet:
    """
    A set of partial rows to be added to a table. This is used in the
    `AppendableRowSetRequest` to indicate what rows should be updated in a table
    during the upsert operation.
    """

    table_id: str
    rows: List[PartialRow]
    concrete_type: str = concrete_types.PARTIAL_ROW_SET

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "tableId": self.table_id,
            "rows": [row.to_synapse_request() for row in self.rows],
        }


@dataclass
class AppendableRowSetRequest:
    """
    A request to append rows to a table. This is used to append rows to a table. This
    request is used in the `TableUpdateTransaction` to indicate what rows should
    be upserted in the table.
    """

    entity_id: str
    to_append: PartialRowSet
    concrete_type: str = concrete_types.APPENDABLE_ROWSET_REQUEST

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "toAppend": self.to_append.to_synapse_request(),
        }


@dataclass
class UploadToTableRequest:
    """
    A request to upload a file to a table. This is used to insert any rows via a CSV
    file into a table. This request is used in the `TableUpdateTransaction`.
    """

    table_id: str
    upload_file_handle_id: str
    update_etag: str
    lines_to_skip: int = 0
    csv_table_descriptor: CsvTableDescriptor = field(default_factory=CsvTableDescriptor)
    concrete_type: str = concrete_types.UPLOAD_TO_TABLE_REQUEST

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        request = {
            "concreteType": self.concrete_type,
            "tableId": self.table_id,
            "uploadFileHandleId": self.upload_file_handle_id,
            "updateEtag": self.update_etag,
            "linesToSkip": self.lines_to_skip,
            "csvTableDescriptor": self.csv_table_descriptor.to_synapse_request(),
        }

        delete_none_keys(request)
        return request


@dataclass
class ColumnChange:
    """
    A change to a column in a table. This is used in the `TableSchemaChangeRequest` to
    indicate what changes should be made to the columns in the table.
    """

    concrete_type: str = concrete_types.COLUMN_CHANGE

    old_column_id: Optional[str] = None
    """The ID of the old ColumnModel to be replaced with the new. Set to null to indicate a new column should be added without replacing an old column."""

    new_column_id: Optional[str] = None
    """The ID of the new ColumnModel to replace the old. Set to null to indicate the old column should be removed without being replaced."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""

        return {
            "concreteType": self.concrete_type,
            "oldColumnId": self.old_column_id,
            "newColumnId": self.new_column_id,
        }


@dataclass
class TableSchemaChangeRequest:
    """
    A request to change the schema of a table. This is used to change the columns in a
    table. This request is used in the `TableUpdateTransaction` to indicate what
    changes should be made to the columns in the table.
    """

    entity_id: str
    changes: List[ColumnChange]
    ordered_column_ids: List[str]
    concrete_type: str = concrete_types.TABLE_SCHEMA_CHANGE_REQUEST

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "changes": [change.to_synapse_request() for change in self.changes],
            "orderedColumnIds": self.ordered_column_ids,
        }


@dataclass
class TableUpdateTransaction(AsynchronousCommunicator):
    """
    A request to update a table. This is used to update a table with a set of changes.

    After calling the `send_job_and_wait_async` method the `results` attribute will be
    filled in based off <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateTransactionResponse.html>.
    """

    entity_id: str
    changes: List[
        Union[TableSchemaChangeRequest, UploadToTableRequest, AppendableRowSetRequest]
    ]
    concrete_type: str = concrete_types.TABLE_UPDATE_TRANSACTION_REQUEST
    results: Optional[List[Dict[str, Any]]] = None
    """This will be an array of
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateResponse.html>."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "changes": [change.to_synapse_request() for change in self.changes],
        }

    def fill_from_dict(self, synapse_response: Dict[str, str]) -> "Self":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API that matches <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateTransactionResponse.html>

        Returns:
            An instance of this class.
        """
        self.results = synapse_response.get("results", None)
        return self


class FacetType(str, Enum):
    """Set to one of the enumerated values to indicate a column should be treated as
    a facet."""

    ENUMERATION = "enumeration"
    """Returns the most frequently seen values and their respective frequency counts;
    selecting these returned values will cause the table results to be filtered such
    that only rows with the selected values are returned."""

    RANGE = "range"
    """Allows the column to be filtered by a chosen lower and upper bound; these bounds
    are inclusive."""


class ColumnType(str, Enum):
    """The column type determines the type of data that can be stored in a column.
    Switching between types (using a transaction with TableUpdateTransaction
    in the "changes" list) is generally allowed except for switching to "_LIST"
    suffixed types. In such cases, a new column must be created and data must be
    copied over manually"""

    STRING = "STRING"
    """The STRING data type is a small text strings with between 1 and 1,000 characters.
    Each STRING column will have a declared maximum size between 1 and 1,000 characters
    (with 50 characters as the default when maximumSize = null). The maximum STRING size
    is applied to the budget of the maximum table width, therefore it is best to use the
    smallest possible maximum size for the data. For strings larger than 250 characters,
    consider using the LARGETEXT column type for improved performance. Each STRING column
    counts as maxSize*4 (4 bytes per character) towards the total width of a table."""

    DOUBLE = "DOUBLE"
    """The DOUBLE data type is a double-precision 64-bit IEEE 754 floating point. Its
    range of values is approximately +/-1.79769313486231570E+308 (15 significant decimal
    digits). Each DOUBLE column counts as 23 bytes towards the total width of a table."""

    INTEGER = "INTEGER"
    """The INTEGER data type is a 64-bit two's complement integer. The signed integer has
    a minimum value of -2^63 and a maximum value of 2^63-1. Each INTEGER column counts as
    20 bytes towards the total width of a table."""

    BOOLEAN = "BOOLEAN"
    """The BOOLEAN data type has only two possible values: 'true' and 'false'. Each
    BOOLEAN column counts as 5 bytes towards the total width of a table."""

    DATE = "DATE"
    """The DATE data type represent the specified number of milliseconds since the
    standard base time known as 'the epoch', namely January 1, 1970, 00:00:00 GM.
    Each DATE column counts as 20 bytes towards the total width of a table."""

    FILEHANDLEID = "FILEHANDLEID"
    """The FILEHANDLEID data type represents a file stored within a table. To store a
    file in a table, first use the 'File Services' to upload a file to generate a new
    FileHandle, then apply the fileHandle.id as the value for this column. Note: This
    column type works best for files that are binary (non-text) or text files that are 1
    MB or larger. For text files that are smaller than 1 MB consider using the LARGETEXT
    column type to improve download performance. Each FILEHANDLEID column counts as 20
    bytes towards the total width of a table."""

    ENTITYID = "ENTITYID"
    """The ENTITYID type represents a reference to a Synapse Entity. Values will include
    the 'syn' prefix, such as 'syn123'. Each ENTITYID column counts as 44 bytes towards
    the total width of a table."""

    SUBMISSIONID = "SUBMISSIONID"
    """The SUBMISSIONID type represents a reference to an evaluation submission. The
    value should be the ID of the referenced submission. Each SUBMISSIONID column counts
    as 20 bytes towards the total width of a table."""

    EVALUATIONID = "EVALUATIONID"
    """The EVALUATIONID type represents a reference to an evaluation. The value should be
    the ID of the referenced evaluation. Each EVALUATIONID column counts as 20 bytes
    towards the total width of a table."""

    LINK = "LINK"
    """The LINK data type represents any URL with 1,000 characters or less. Each LINK
    column counts as maxSize*4 (4 bytes per character) towards the total width of a
    table."""

    MEDIUMTEXT = "MEDIUMTEXT"
    """The MEDIUMTEXT data type represents a string that is between 1 and 2,000
    characters without the need to specify a maximum size. For smaller strings where the
    maximum size is known consider using the STRING column type. For larger strings,
    consider using the LARGETEXT or FILEHANDLEID column types. Each MEDIUMTEXT column
    counts as 421 bytes towards the total width of a table."""

    LARGETEXT = "LARGETEXT"
    """The LARGETEXT data type represents a string that is greater than 250 characters
    but less than 524,288 characters (2 MB of UTF-8 4 byte chars). For smaller strings
    consider using the STRING or MEDIUMTEXT column types. For larger strings, consider
    using the FILEHANDELID column type. Each LARGE_TEXT column counts as 2133 bytes
    towards the total width of a table."""

    USERID = "USERID"
    """The USERID data type represents a reference to a Synapse User. The value should
    be the ID of the referenced User. Each USERID column counts as 20 bytes towards the
    total width of a table."""

    STRING_LIST = "STRING_LIST"
    """Multiple values of STRING."""

    INTEGER_LIST = "INTEGER_LIST"
    """Multiple values of INTEGER."""

    BOOLEAN_LIST = "BOOLEAN_LIST"
    """Multiple values of BOOLEAN."""

    DATE_LIST = "DATE_LIST"
    """Multiple values of DATE."""

    ENTITYID_LIST = "ENTITYID_LIST"
    """Multiple values of ENTITYID."""

    USERID_LIST = "USERID_LIST"
    """Multiple values of USERID."""

    JSON = "JSON"
    """A flexible type that allows to store JSON data. Each JSON column counts as 2133
    bytes towards the total width of a table. A JSON value string should be less than
    524,288 characters (2 MB of UTF-8 4 byte chars)."""

    def __repr__(self) -> str:
        """Print out the string value of self"""
        return self.value


@dataclass
class JsonSubColumn:
    """For column of type JSON that represents the combination of multiple
    sub-columns, this property is used to define each sub-column."""

    name: str
    """The display name of the column."""

    column_type: ColumnType
    """The column type determines the type of data that can be stored in a column.
    Switching between types (using a transaction with TableUpdateTransaction
    in the "changes" list) is generally allowed except for switching to "_LIST" suffixed
    types. In such cases, a new column must be created and data must be copied
    over manually"""

    json_path: str
    """Defines the JSON path of the sub column. Use the '$' char to represent the root
    of JSON object. If the JSON key of a sub column is 'a', then the jsonPath for that
    column would be: '$.a'."""

    facet_type: Optional[FacetType] = None
    """Set to one of the enumerated values to indicate a column should be
    treated as a facet"""

    def to_synapse_request(self) -> Dict[str, Any]:
        """Converts the Column object into a dictionary that can be passed into the
        REST API."""
        result = {
            "name": self.name,
            "columnType": self.column_type.value if self.column_type else None,
            "jsonPath": self.json_path,
            "facetType": self.facet_type.value if self.facet_type else None,
        }
        delete_none_keys(result)
        return result


@dataclass()
@async_to_sync
class Column(ColumnSynchronousProtocol):
    """A column model contains the metadata of a single column of a table or view."""

    id: Optional[str] = None
    """The immutable ID issued to new columns"""

    name: Optional[str] = None
    """The display name of the column"""

    column_type: Optional[ColumnType] = None
    """The column type determines the type of data that can be stored in a column.
    Switching between types (using a transaction with TableUpdateTransaction
    in the "changes" list) is generally allowed except for switching to "_LIST"
    suffixed types. In such cases, a new column must be created and data must be
    copied over manually"""

    facet_type: Optional[FacetType] = None
    """Set to one of the enumerated values to indicate a column should be
    treated as a facet"""

    default_value: Optional[str] = None
    """The default value for this column. Columns of type ENTITYID, FILEHANDLEID,
    USERID, and LARGETEXT are not allowed to have default values."""

    maximum_size: Optional[int] = None
    """A parameter for columnTypes with a maximum size. For example, ColumnType.STRINGs
    have a default maximum size of 50 characters, but can be set to a maximumSize
    of 1 to 1000 characters. For columnType of STRING_LIST, this limits the size
    of individual string elements in the list"""

    maximum_list_length: Optional[int] = None
    """Required if using a columnType with a "_LIST" suffix. Describes the maximum number
    of values that will appear in that list. Value range 1-100 inclusive. Default 100"""

    enum_values: Optional[List[str]] = None
    """Columns of type STRING can be constrained to an enumeration values set on this
    list. The maximum number of entries for an enum is 100"""

    json_sub_columns: Optional[List[JsonSubColumn]] = None
    """For column of type JSON that represents the combination of multiple sub-columns,
    this property is used to define each sub-column."""

    _last_persistent_instance: Optional["Column"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def fill_from_dict(
        self, synapse_column: Union[Synapse_Column, Dict[str, Any]]
    ) -> "Column":
        """Converts a response from the synapseclient into this dataclass."""
        self.id = synapse_column.get("id", None)
        self.name = synapse_column.get("name", None)
        self.column_type = (
            ColumnType(synapse_column.get("columnType", None))
            if synapse_column.get("columnType", None)
            else None
        )
        self.facet_type = (
            FacetType(synapse_column.get("facetType", None))
            if synapse_column.get("facetType", None)
            else None
        )
        self.default_value = synapse_column.get("defaultValue", None)
        self.maximum_size = synapse_column.get("maximumSize", None)
        self.maximum_list_length = synapse_column.get("maximumListLength", None)
        self.enum_values = synapse_column.get("enumValues", None)
        # TODO: This needs to be converted to it's Dataclass. It also needs to be tested to verify conversion.
        self.json_sub_columns = synapse_column.get("jsonSubColumns", None)
        self._set_last_persistent_instance()
        return self

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = replace(self)
        self._last_persistent_instance.json_sub_columns = (
            replace(self.json_sub_columns) if self.json_sub_columns else None
        )

    def to_synapse_request(self) -> Dict[str, Any]:
        """Converts the Column object into a dictionary that can be passed into the
        REST API."""
        result = {
            "concreteType": concrete_types.COLUMN_MODEL,
            "name": self.name,
            "columnType": self.column_type.value if self.column_type else None,
            "facetType": self.facet_type.value if self.facet_type else None,
            "defaultValue": self.default_value,
            "maximumSize": self.maximum_size,
            "maximumListLength": self.maximum_list_length,
            "enumValues": self.enum_values,
            "jsonSubColumns": [
                sub_column.to_synapse_request() for sub_column in self.json_sub_columns
            ]
            if self.json_sub_columns
            else None,
        }
        delete_none_keys(result)
        return result


class SchemaStorageStrategy(str, Enum):
    """Enum used to determine how to store the schema of a table in Synapse."""

    INFER_FROM_DATA = "INFER_FROM_DATA"
    """
    (Default)
    Allow the data to define which columns are created on the Synapse table
    automatically. The limitation with this behavior is that the columns created may
    only be of the following types:

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
    follow the examples shown in the [Table][synapseclient.models.Table] class.


    The columns created as a result of this strategy will be appended to the end of the
    existing columns if the table already exists.
    """


class ColumnExpansionStrategy(str, Enum):
    """
    Determines how to automate the expansion of columns based on the data
    that is being stored. The options given allow cells with a limit on the length of
    content (Such as strings) to be expanded to a larger size if the data being stored
    exceeds the limit. A limit to list length is also enforced in Synapse by automatic
    expansion for lists is not yet supported through this interface.
    """

    # To be supported at a later time
    # AUTO_EXPAND_CONTENT_AND_LIST_LENGTH = "AUTO_EXPAND_CONTENT_AND_LIST_LENGTH"
    # """
    # (Default)
    # Automatically expand both the content length and list length of columns if the data
    # being stored exceeds the limit.
    # """

    AUTO_EXPAND_CONTENT_LENGTH = "AUTO_EXPAND_CONTENT_LENGTH"
    """
    (Default)
    Automatically expand the content length of columns if the data being stored exceeds
    the limit.
    """

    # To be supported at a later time
    # AUTO_EXPAND_LIST_LENGTH = "AUTO_EXPAND_LIST_LENGTH"
    # """
    # Automatically expand the list length of columns if the data being stored exceeds
    # the limit.
    # """


# TODO: Determine if Datasets, and other table type things have all this functionality
@async_to_sync
class TableRowOperator(TableRowOperatorSynchronousProtocol):
    id: str = None
    name: str = None
    parent_id: str = None
    activity: None = None
    columns: OrderedDict = None
    _last_persistent_instance: None = None
    _columns_to_delete: None = None

    def _set_last_persistent_instance(self) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    def to_synapse_request(self) -> Dict:
        """Used to satisfy the usage in this mixin from the parent class."""

    def fill_from_dict(self, *args, **kwargs) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    def get_async(self, *args, **kwargs) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    def _generate_schema_change_request(
        self, *args, **kwargs
    ) -> TableSchemaChangeRequest:
        """Used to satisfy the usage in this mixin from the parent class."""

    def query_async(self, *args, **kwargs) -> None:
        """Used to satisfy the usage in this mixin from the parent class."""

    @property
    def has_changed(self) -> bool:
        """Used to satisfy the usage in this mixin from the parent class."""

    @property
    def has_columns_changed(self) -> bool:
        """Used to satisfy the usage in this mixin from the parent class."""

    async def upsert_rows_async(
        self,
        values: DATA_FRAME_TYPE,
        primary_keys: List[str],
        dry_run: bool = False,
        *,
        rows_per_query: int = 50000,
        update_size_byte: int = 1.9 * MB,
        insert_size_byte: int = 900 * MB,
        synapse_client: Optional[Synapse] = None,
        **kwargs,
    ) -> None:
        """
        This method allows you to perform an `upsert` (Update and Insert) for row(s).
        This means that you may update a row with only the data that you want to change.
        When supplied with a row that does not match the given `primary_keys` a new
        row will be inserted.


        Using the `primary_keys` argument you may specify which columns to use to
        determine if a row already exists. If a row exists with the same values in the
        columns specified in this list the row will be updated. If a row does not exist
        it will be inserted.


        Limitations:

        - The request to update, and the request to insert data does not occur in a
            single transaction. This means that the update of data may succeed, but the
            insert of data may fail. Additionally, as noted in the limitation below, if
            data is chunked up into multiple requests you may find that a portion of
            your data is updated, but another portion is not.
        - The number of rows that may be upserted in a single call should be
            kept to a minimum (< 50,000). There is significant overhead in the request
            to Synapse for each row that is upserted. If you are upserting a large
            number of rows a better approach may be to query for the data you want
            to update, update the data, then use the [store_rows_async][synapseclient.models.mixins.table_operator.TableRowOperator.store_rows_async] method to
            update the data in Synapse. Any rows you want to insert may be added
            to the DataFrame that is passed to the [store_rows_async][synapseclient.models.mixins.table_operator.TableRowOperator.store_rows_async] method.
        - When upserting mnay rows the requests to Synapse will be chunked into smaller
            requests. The limit is 2MB per request. This chunking will happen
            automatically and should not be a concern for most users. If you are
            having issues with the request being too large you may lower the
            number of rows you are trying to upsert, or note the above limitation.
        - The `primary_keys` argument must contain at least one column.
        - The `primary_keys` argument cannot contain columns that are a LIST type.
        - The `primary_keys` argument cannot contain columns that are a JSON type.
        - The values used as the `primary_keys` must be unique in the table. If there
            are multiple rows with the same values in the `primary_keys` the behavior
            is that an exception will be raised.
        - The columns used in `primary_keys` cannot contain updated values. Since
            the values in these columns are used to determine if a row exists, they
            cannot be updated in the same transaction.


        Arguments:
            values: Supports storing data from the following sources:

                - A string holding the path to a CSV file. Tthe data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_operator.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
                - A dictionary where the key is the column name and the value is one or more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). You may pass in additional arguments to the `pd.DataFrame` function by passing them in as keyword arguments to this function. Read about the available arguments in the [Pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) documentation.
                - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

            primary_keys: The columns to use to determine if a row already exists. If
                a row exists with the same values in the columns specified in this list
                the row will be updated. If a row does not exist it will be inserted.

            dry_run: If set to True the data will not be updated in Synapse. A message
                will be printed to the console with the number of rows that would have
                been updated and inserted. If you would like to see the data that would
                be updated and inserted you may set the `dry_run` argument to True and
                set the log level to DEBUG by setting the debug flag when creating
                your Synapse class instance like: `syn = Synapse(debug=True)`.

            rows_per_query: The number of rows that will be queries from Synapse per
                request. Since we need to query for the data that is being updated
                this will determine the number of rows that are queried at a time.
                The default is 50,000 rows.

            update_size_byte: The maximum size of the request that will be sent to Synapse
                when updating rows of data. The default is 1.9MB.

            insert_size_byte: The maximum size of the request that will be sent to Synapse
                when inserting rows of data. The default is 900MB.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor


        Example: Updating 2 rows and inserting 1 row
            In this given example we have a table with the following data:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |

            The following code will update the first row's `col2` to `22`, update the
            second row's `col3` to `33`, and insert a new row:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`
            import pandas as pd

            syn = Synapse()
            syn.login()


            async def main():
                table = await Table(id="syn123").get_async(include_columns=True)

                df = {
                    'col1': ['A', 'B', 'C'],
                    'col2': [22, 2, 3],
                    'col3': [1, 33, 3],
                }

                await table.upsert_rows_async(values=df, primary_keys=["col1"])

            asyncio.run(main())
            ```

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 22   | 1    |
            | B    | 2    | 33   |
            | C    | 3    | 3    |

        Example: Deleting data from a specific cell
            In this given example we have a table with the following data:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |

            The following code will update the first row's `col2` to `22`, update the
            second row's `col3` to `33`, and insert a new row:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()


            async def main():
                table = await Table(id="syn123").get_async(include_columns=True)

                df = {
                    'col1': ['A', 'B'],
                    'col2': [None, 2],
                    'col3': [1, None],
                }

                await table.upsert_rows_async(values=df, primary_keys=["col1"])

            asyncio.run(main())
            ```


            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    |      | 1    |
            | B    | 2    |      |

        """
        test_import_pandas()
        from pandas import DataFrame, isna

        if not self._last_persistent_instance:
            await self.get_async(include_columns=True, synapse_client=synapse_client)
        if not self.columns:
            raise ValueError(
                "There are no columns on this table. Unable to proceed with an upsert operation."
            )

        if isinstance(values, dict):
            values = DataFrame(values)
        elif isinstance(values, str):
            values = csv_to_pandas_df(filepath=values, **kwargs)
        elif isinstance(values, DataFrame):
            pass
        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

        client = Synapse.get_client(synapse_client=synapse_client)

        rows_to_update: List[PartialRow] = []
        chunk_list: List[DataFrame] = []
        for i in range(0, len(values), rows_per_query):
            chunk_list.append(values[i : i + rows_per_query])

        all_columns_from_df = [f'"{column}"' for column in values.columns]
        contains_etag = self.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG
        indexs_of_original_df_with_changes = []
        total_row_count_updated = 0

        with logging_redirect_tqdm(loggers=[client.logger]):
            progress_bar = tqdm(
                total=len(values),
                desc="Querying & Updating rows",
                unit_scale=True,
                smoothing=0,
            )
            for individual_chunk in chunk_list:
                select_statement = "SELECT ROW_ID, "

                if self.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG:
                    select_statement += "ROW_ETAG, "

                select_statement += (
                    f"{', '.join(all_columns_from_df)} FROM {self.id} WHERE "
                )
                where_statements = []
                for upsert_column in primary_keys:
                    column_model = self.columns[upsert_column]
                    if (
                        column_model.column_type
                        in (
                            ColumnType.STRING_LIST,
                            ColumnType.INTEGER_LIST,
                            ColumnType.BOOLEAN_LIST,
                            ColumnType.ENTITYID_LIST,
                            ColumnType.USERID_LIST,
                        )
                        or column_model.column_type == ColumnType.JSON
                    ):
                        raise ValueError(
                            f"Column type {column_model.column_type} is not supported for primary_keys"
                        )
                    elif column_model.column_type in (
                        ColumnType.STRING,
                        ColumnType.MEDIUMTEXT,
                        ColumnType.LARGETEXT,
                        ColumnType.LINK,
                        ColumnType.ENTITYID,
                    ):
                        values_for_where_statement = [
                            f"'{value}'"
                            for value in individual_chunk[upsert_column]
                            if value is not None
                        ]

                    elif column_model.column_type == ColumnType.BOOLEAN:
                        include_true = False
                        include_false = False
                        for value in individual_chunk[upsert_column]:
                            if value is None:
                                continue
                            if value:
                                include_true = True
                            else:
                                include_false = True
                            if include_true and include_false:
                                break
                        if include_true and include_false:
                            values_for_where_statement = ["'true'", "'false'"]
                        elif include_true:
                            values_for_where_statement = ["'true'"]
                        elif include_false:
                            values_for_where_statement = ["'false'"]
                    else:
                        values_for_where_statement = [
                            str(value)
                            for value in individual_chunk[upsert_column]
                            if value is not None
                        ]
                    if not values_for_where_statement:
                        continue
                    where_statements.append(
                        f"\"{upsert_column}\" IN ({', '.join(values_for_where_statement)})"
                    )

                where_statement = " AND ".join(where_statements)
                select_statement += where_statement

                results = await self.query_async(
                    query=select_statement, synapse_client=synapse_client
                )

                for row in results.itertuples(index=False):
                    row_etag = None

                    if contains_etag:
                        row_etag = row.ROW_ETAG

                    partial_change_values = {}

                    # Find the matching row in `values` that matches the row in `results` for the primary_keys
                    matching_conditions = individual_chunk[primary_keys[0]] == getattr(
                        row, primary_keys[0]
                    )
                    for col in primary_keys[1:]:
                        matching_conditions &= individual_chunk[col] == getattr(
                            row, col
                        )
                    matching_row = individual_chunk.loc[matching_conditions]

                    # Determines which cells need to be updated
                    for column in individual_chunk.columns:
                        if len(matching_row[column].values) > 1:
                            raise ValueError(
                                f"The values for the keys being upserted must be unique in the table: [{matching_row}]"
                            )

                        if len(matching_row[column].values) == 0:
                            continue
                        column_id = self.columns[column].id
                        column_type = self.columns[column].column_type
                        cell_value = matching_row[column].values[0]
                        if cell_value != getattr(row, column):
                            if (
                                isinstance(cell_value, list) and len(cell_value) > 0
                            ) or not isna(cell_value):
                                partial_change_values[
                                    column_id
                                ] = _convert_pandas_row_to_python_types(
                                    cell=cell_value, column_type=column_type
                                )
                            else:
                                partial_change_values[column_id] = None

                    if partial_change_values != {}:
                        total_row_count_updated += 1
                        partial_change = PartialRow(
                            row_id=row.ROW_ID,
                            etag=row_etag,
                            values=[
                                {
                                    "key": partial_change_key,
                                    "value": partial_change_value,
                                }
                                for partial_change_key, partial_change_value in partial_change_values.items()
                            ],
                        )
                        rows_to_update.append(partial_change)
                        indexs_of_original_df_with_changes.append(matching_row.index[0])

                if not dry_run and rows_to_update:
                    current_chunk_size = 0
                    chunk = []
                    for row in rows_to_update:
                        row_size = row.size()
                        if current_chunk_size + row_size > update_size_byte:
                            change = AppendableRowSetRequest(
                                entity_id=self.id,
                                to_append=PartialRowSet(
                                    table_id=self.id,
                                    rows=chunk,
                                ),
                            )

                            request = TableUpdateTransaction(
                                entity_id=self.id,
                                changes=[change],
                            )

                            await request.send_job_and_wait_async(synapse_client=client)
                            progress_bar.update(len(chunk))
                            chunk = []
                            current_chunk_size = 0
                        chunk.append(row)
                        current_chunk_size += row_size

                    if chunk:
                        change = AppendableRowSetRequest(
                            entity_id=self.id,
                            to_append=PartialRowSet(
                                table_id=self.id,
                                rows=chunk,
                            ),
                        )

                        await TableUpdateTransaction(
                            entity_id=self.id,
                            changes=[change],
                        ).send_job_and_wait_async(synapse_client=client)
                        progress_bar.update(len(chunk))
                elif dry_run:
                    progress_bar.update(len(rows_to_update))
                progress_bar.update(len(individual_chunk.index) - len(rows_to_update))

                rows_to_update: List[PartialRow] = []
            progress_bar.update(progress_bar.total - progress_bar.n)
            progress_bar.refresh()
            progress_bar.close()

        rows_to_insert_df = values.loc[
            ~values.index.isin(indexs_of_original_df_with_changes)
        ]

        client.logger.info(
            f"[{self.id}:{self.name}]: Found {total_row_count_updated}"
            f" rows to update and {len(rows_to_insert_df)} rows to insert"
        )

        if not dry_run and not rows_to_insert_df.empty:
            await self.store_rows_async(
                values=rows_to_insert_df,
                dry_run=dry_run,
                insert_size_byte=insert_size_byte,
                synapse_client=synapse_client,
            )

    async def store_rows_async(
        self,
        values: Union[str, Dict[str, Any], DATA_FRAME_TYPE],
        schema_storage_strategy: SchemaStorageStrategy = None,
        column_expansion_strategy: ColumnExpansionStrategy = None,
        dry_run: bool = False,
        additional_changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
        *,
        insert_size_byte: int = 900 * MB,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        read_csv_kwargs: Optional[Dict[str, Any]] = None,
        to_csv_kwargs: Optional[Dict[str, Any]] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Add or update rows in Synapse from the sources defined below. In most cases the
        result of this function call will append rows to the table. In the case of an
        update this method works on a full row replacement. What this means is
        that you may not do a partial update of a row. If you want to update a row
        you must pass in all the data for that row, or the data for the columns not
        provided will be set to null.

        If you'd like to update a row see the example `Updating rows in a table` below.

        If you'd like to perform an `upsert` or partial update of a row you may use
        the `.upsert_rows()` method. See that method for more information.


        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the contruction of your `Table` instance, ie.
            `Table(columns=[Column(name="foo")])`, they will be added at the begining
            of the columns list.
        - If you use the `store_rows` method and the `schema_storage_strategy` is set to
            `INFER_FROM_DATA` the columns will be added at the end of the columns list.



        Arguments:
            values: Supports storing data from the following sources:

                - A string holding the path to a CSV file. If the `schema_storage_strategy` is set to `None` the data will be uploaded as is. If `schema_storage_strategy` is set to `INFER_FROM_DATA` the data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_operator.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
                - A dictionary where the key is the column name and the value is one or more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). You may pass in additional arguments to the `pd.DataFrame` function by passing them in as keyword arguments to this function. Read about the available arguments in the [Pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) documentation.
                - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

            schema_storage_strategy: Determines how to automate the creation of columns
                based on the data that is being stored. If you want to have full
                control over the schema you may set this to `None` and create
                the columns manually.

                The limitation with this behavior is that the columns created may only
                be of the following types:

                - STRING
                - LARGETEXT
                - INTEGER
                - DOUBLE
                - BOOLEAN
                - DATE

                The determination is based on how this pandas function infers the
                data type: [infer_dtype](https://pandas.pydata.org/docs/reference/api/pandas.api.types.infer_dtype.html)

                This may also only set the `name`, `column_type`, and `maximum_size` of
                the column when the column is created. If this is used to update the
                column the `maxium_size` will only be updated depending on the
                value of `column_expansion_strategy`. The other attributes of the
                column will be set to the default values on create, or remain the same
                if the column already exists.


                The usage of this feature will never delete a column, shrink a column,
                or change the type of a column that already exists. If you need to
                change any of these attributes you must do so after getting the table
                via a `.get()` call, updating the columns as needed, then calling
                `.store()` on the table.

            column_expansion_strategy: Determines how to automate the expansion of
                columns based on the data that is being stored. The options given allow
                cells with a limit on the length of content (Such as strings) to be
                expanded to a larger size if the data being stored exceeds the limit.
                If you want to have full control over the schema you may set this to
                `None` and create the columns manually. String type columns are the only
                ones that support this feature.

            dry_run: Log the actions that would be taken, but do not actually perform
                the actions. This will not print out the data that would be stored or
                modified as a result of this action. It will print out the actions that
                would be taken, such as creating a new column, updating a column, or
                updating table metadata. This is useful for debugging and understanding
                what actions would be taken without actually performing them.

            additional_changes: Additional changes to the table that should execute
                within the same transaction as appending or updating rows. This is used
                as a part of the `upsert_rows` method call to allow for the updating of
                rows and the updating of the table schema in the same transaction. In
                most cases you will not need to use this argument.

            insert_size_byte: The maximum size of data that will be stored to Synapse
                within a single transaction. The API have a limit of 1GB, but the
                default is set to 900 MB to allow for some overhead in the request. The
                implication of this limit is that when you are storing a CSV that is
                larger than this limit the data will be chunked into smaller requests
                by writing a portion of the data to a temporary file that is cleaned up
                after upload. Due to this batching it also means that the entire
                upload is not atomic. Storing a dataframe is also subject to this limit
                and will be chunked into smaller requests if the size exceeds this
                limit. Dataframes are converted to CSV files before being uploaded to
                Synapse regardless of the size of the dataframe, but depending on the
                size of the dataframe the data may be chunked into smaller requests.

            csv_table_descriptor: When passing in a CSV file this will allow you to
                specify the format of the CSV file. This is only used when the `values`
                argument is a string holding the path to a CSV file. See
                [CsvTableDescriptor][synapseclient.models.CsvTableDescriptor]
                for more information.

            read_csv_kwargs: Additional arguments to pass to the `pd.read_csv` function
                when reading in a CSV file. This is only used when the `values` argument
                is a string holding the path to a CSV file and you have set the
                `schema_storage_strategy` to `INFER_FROM_DATA`. See
                <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>
                for complete list of supported arguments.

            to_csv_kwargs: Additional arguments to pass to the `pd.DataFrame.to_csv`
                function when writing the data to a CSV file. This is only used when
                the `values` argument is a Pandas DataFrame. See
                <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html>
                for complete list of supported arguments.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Inserting rows into a table that already has columns
            This example shows how you may insert rows into a table.

            Suppose we have a table with the following columns:

            | col1 | col2 | col3 |
            |------|------| -----|

            The following code will insert rows into the table:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                data_to_insert = {
                    'col1': ['A', 'B', 'C'],
                    'col2': [1, 2, 3],
                    'col3': [1, 2, 3],
                }

                await Table(id="syn1234").store_rows_async(values=data_to_insert)

            asyncio.run(main())
            ```

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |
            | C    | 3    | 3    |

        Example: Inserting rows into a table that does not have columns
            This example shows how you may insert rows into a table that does not have
            columns. The columns will be inferred from the data that is being stored.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table, SchemaStorageStrategy # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                data_to_insert = {
                    'col1': ['A', 'B', 'C'],
                    'col2': [1, 2, 3],
                    'col3': [1, 2, 3],
                }

                await Table(id="syn1234").store_rows_async(
                    values=data_to_insert,
                    schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA
                )

            asyncio.run(main())
            ```

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |
            | C    | 3    | 3    |

        Example: Using the dry_run option with a SchemaStorageStrategy of INFER_FROM_DATA
            This example shows how you may use the `dry_run` option with the
            `SchemaStorageStrategy` set to `INFER_FROM_DATA`. This will show you the
            actions that would be taken, but not actually perform the actions.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table, SchemaStorageStrategy # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                data_to_insert = {
                    'col1': ['A', 'B', 'C'],
                    'col2': [1, 2, 3],
                    'col3': [1, 2, 3],
                }

                await Table(id="syn1234").store_rows_async(
                    values=data_to_insert,
                    dry_run=True,
                    schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA
                )

            asyncio.run(main())
            ```

            The result of running this action will print to the console the actions that
            would be taken, but not actually perform the actions.

        Example: Updating rows in a table
            This example shows how you may query for data in a table, update the data,
            and then store the updated rows back in Synapse.

            Suppose we have a table that has the following data:


            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |
            | C    | 3    | 3    |

            Behind the scenese the tables also has `ROW_ID` and `ROW_VERSION` columns
            which are used to identify the row that is being updated. These columns
            are not shown in the table above, but is included in the data that is
            returned when querying the table. If you add data that does not have these
            columns the data will be treated as new rows to be inserted.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table, query_async # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                query_results = await query_async(query="select * from syn1234 where col1 in ('A', 'B')")

                # Update `col2` of the row where `col1` is `A` to `22`
                query_results.loc[query_results['col1'] == 'A', 'col2'] = 22

                # Update `col3` of the row where `col1` is `B` to `33`
                query_results.loc[query_results['col1'] == 'B', 'col3'] = 33

                await Table(id="syn1234").store_rows_async(values=query_results)

            asyncio.run(main())
            ```

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 22   | 1    |
            | B    | 2    | 33   |
            | C    | 3    | 3    |

        """
        test_import_pandas()
        from pandas import DataFrame

        original_values = values
        if isinstance(values, dict):
            values = DataFrame(values)
        elif (
            isinstance(values, str)
            and schema_storage_strategy == SchemaStorageStrategy.INFER_FROM_DATA
        ):
            values = csv_to_pandas_df(filepath=values, **(read_csv_kwargs or {}))
        elif isinstance(values, DataFrame) or isinstance(values, str):
            # We don't need to convert a DF, and CSVs will be uploaded as is
            pass
        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

        client = Synapse.get_client(synapse_client=synapse_client)

        if (
            (not self._last_persistent_instance)
            and (
                existing_id := await get_id(
                    entity=self, synapse_client=synapse_client, failure_strategy=None
                )
            )
            and (
                existing_entity := await self.__class__(id=existing_id).get_async(
                    include_columns=True, synapse_client=synapse_client
                )
            )
        ):
            merge_dataclass_entities(
                source=existing_entity,
                destination=self,
            )

        if dry_run:
            client.logger.info(
                f"[{self.id}:{self.name}]: Dry run enabled. No changes will be made."
            )

        schema_change_request = None

        if schema_storage_strategy == SchemaStorageStrategy.INFER_FROM_DATA:
            infered_columns = infer_column_type_from_data(values=values)

            modified_ordered_dict = OrderedDict()
            for column in self.columns.values():
                modified_ordered_dict[column.name] = column
            self.columns = modified_ordered_dict

            for infered_column in infered_columns:
                column_instance = self.columns.get(infered_column.name, None)
                if column_instance is None:
                    self.columns[infered_column.name] = infered_column
                else:
                    if (
                        column_expansion_strategy is not None
                        and (
                            column_expansion_strategy
                            == ColumnExpansionStrategy.AUTO_EXPAND_CONTENT_LENGTH
                        )
                        and (infered_column.maximum_size or 0)
                        > (column_instance.maximum_size or 1)
                    ):
                        column_instance.maximum_size = infered_column.maximum_size

            schema_change_request = await self._generate_schema_change_request(
                dry_run=dry_run, synapse_client=synapse_client
            )

        if dry_run:
            return

        if not self.id:
            raise ValueError(
                "The table must have an ID to store rows, or the table could not be found from the given name/parent_id."
            )

        if isinstance(original_values, str):
            await self._chunk_and_upload_csv(
                path_to_csv=original_values,
                insert_size_byte=insert_size_byte,
                csv_table_descriptor=csv_table_descriptor,
                schema_change_request=schema_change_request,
                client=client,
                additional_changes=additional_changes,
            )
        elif isinstance(values, DataFrame):
            # When creating this temporary file we are using the cache directory
            # as the staging location for the file upload. This is because it
            # will allow for the purge cache function to clean up files that
            # end up getting left here. It is also to account for the fact that
            # the temp directory may not have enough disk space to hold a file
            # we need to upload (As is the case on EC2 instances)
            temp_dir = client.cache.get_cache_dir(file_handle_id=111111111)
            os.makedirs(temp_dir, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="chunked_csv_for_synapse_store_rows",
                suffix=".csv",
                dir=temp_dir,
            ) as temp_file:
                try:
                    # TODO: This portion of the code should be updated to support uploading a file from memory using BytesIO (Ticket to be created)
                    # TODO: The way that the dataframe is split up is also needed. As of right now we are writing the CSV, and then letting the CSV
                    # TODO: upload process take over. However, if we are uploading from memory, then we don't have the ability to write to a file, and then
                    # TODO: upload that file. We need to be able to split the dataframe into chunks, and then upload those chunks to Synapse.
                    values.to_csv(
                        temp_file.name,
                        index=False,
                        float_format="%.12g",
                        **(to_csv_kwargs or {}),
                    )
                    # NOTE: reason for flat_format='%.12g':
                    # pandas automatically converts int columns into float64 columns when some cells in the column have no
                    # value. If we write the whole number back as a decimal (e.g. '3.0'), Synapse complains that we are writing
                    # a float into a INTEGER(synapse table type) column. Using the 'g' will strip off '.0' from whole number
                    # values. pandas by default (with no float_format parameter) seems to keep 12 values after decimal, so we
                    # use '%.12g'.c
                    # see SYNPY-267.

                    await self._chunk_and_upload_csv(
                        path_to_csv=temp_file.name,
                        insert_size_byte=insert_size_byte,
                        csv_table_descriptor=csv_table_descriptor,
                        schema_change_request=schema_change_request,
                        client=client,
                        additional_changes=additional_changes,
                    )
                finally:
                    os.remove(temp_file.name)

        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

    async def _chunk_and_upload_csv(
        self,
        path_to_csv: str,
        insert_size_byte: int,
        csv_table_descriptor: CsvTableDescriptor,
        schema_change_request: TableSchemaChangeRequest,
        client: Synapse,
        additional_changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
    ) -> None:
        # TODO: Add integration test around this portion of the code
        file_size = os.path.getsize(path_to_csv)
        if file_size > insert_size_byte:
            applied_additional_changes = False
            with open(file=path_to_csv, mode="r", encoding="utf-8") as f:
                header = f.readline().encode()
                chunk = [line.encode() for line in f.readlines(insert_size_byte)]
                file_path = None
                temp_dir = client.cache.get_cache_dir(file_handle_id=111111111)
                os.makedirs(temp_dir, exist_ok=True)
                while chunk:
                    with tempfile.NamedTemporaryFile(
                        delete=False,
                        prefix="chunked_csv_for_synapse_store_rows",
                        suffix=".csv",
                        dir=temp_dir,
                    ) as temp_file:
                        try:
                            file_path = temp_file.name
                            temp_file.write(header)
                            temp_file.writelines(chunk)
                            temp_file.close()

                            # TODO: This portion of the code should be updated to support uploading a file from memory using BytesIO (Ticket to be created)
                            file_handle_id = await multipart_upload_file_async(
                                syn=client,
                                file_path=file_path,
                                content_type="text/csv",
                            )
                        finally:
                            os.remove(file_path)

                    upload_request = UploadToTableRequest(
                        table_id=self.id,
                        upload_file_handle_id=file_handle_id,
                        update_etag=None,
                    )
                    if csv_table_descriptor:
                        upload_request.csv_table_descriptor = csv_table_descriptor
                    changes = []
                    if not applied_additional_changes:
                        applied_additional_changes = True
                        if schema_change_request:
                            changes.append(schema_change_request)
                        if additional_changes:
                            changes.extend(additional_changes)
                    changes.append(upload_request)

                    await TableUpdateTransaction(
                        entity_id=self.id, changes=changes
                    ).send_job_and_wait_async(synapse_client=client)

                    chunk = [line.encode() for line in f.readlines(insert_size_byte)]

        else:
            file_handle_id = await multipart_upload_file_async(
                syn=client, file_path=path_to_csv, content_type="text/csv"
            )
            upload_request = UploadToTableRequest(
                table_id=self.id,
                upload_file_handle_id=file_handle_id,
                update_etag=None,
            )
            if csv_table_descriptor:
                upload_request.csv_table_descriptor = csv_table_descriptor
            changes = []
            if schema_change_request:
                changes.append(schema_change_request)
            if additional_changes:
                changes.extend(additional_changes)
            changes.append(upload_request)

            await TableUpdateTransaction(
                entity_id=self.id, changes=changes
            ).send_job_and_wait_async(synapse_client=client)

    # TODO: Determine if it is possible to delete rows from a `Dataset` entity, or if it's only possible to delete the rows by setting the `items` attribute and storing the entity
    async def delete_rows_async(
        self, query: str, *, synapse_client: Optional[Synapse] = None
    ) -> DATA_FRAME_TYPE:
        """
        Delete rows from a table given a query to select rows. The query at a
        minimum must select the `ROW_ID` and `ROW_VERSION` columns. If you want to
        inspect the data that will be deleted ahead of time you may use the
        `.query` method to get the data.


        Arguments:
            query: The query to select the rows to delete. The query at a minimum
                must select the `ROW_ID` and `ROW_VERSION` columns. See this document
                that describes the expected syntax of the query:
                <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of your query for the rows that were deleted from the table.

        Example: Selecting a row to delete
            This example shows how you may select a row to delete from a table.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                await Table(id="syn1234").delete_rows_async(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo = 'asdf'")

            asyncio.run(main())
            ```

        Example: Selecting all rows that contain a null value
            This example shows how you may select a row to delete from a table where
            a column has a null value.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            async def main():
                await Table(id="syn1234").delete_rows_async(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo is null")

            asyncio.run(main())
            ```
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        results_from_query = await TableOperator.query_async(
            query=query, synapse_client=client
        )
        client.logger.info(
            f"Found {len(results_from_query)} rows to delete for given query: {query}"
        )

        if self.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG:
            filtered_columns = results_from_query[["ROW_ID", "ROW_VERSION", "ROW_ETAG"]]
        else:
            filtered_columns = results_from_query[["ROW_ID", "ROW_VERSION"]]

        filepath = f"{tempfile.mkdtemp()}/{self.id}_upload_{uuid.uuid4()}.csv"
        try:
            filtered_columns.to_csv(filepath, index=False)
            file_handle_id = await multipart_upload_file_async(
                syn=client, file_path=filepath, content_type="text/csv"
            )
        finally:
            os.remove(filepath)

        upload_request = UploadToTableRequest(
            table_id=self.id, upload_file_handle_id=file_handle_id, update_etag=None
        )

        await TableUpdateTransaction(
            entity_id=self.id, changes=[upload_request]
        ).send_job_and_wait_async(synapse_client=client)

        return results_from_query


def infer_column_type_from_data(values: DATA_FRAME_TYPE) -> List[Column]:
    """
    Return a list of Synapse table [Column][synapseclient.models.table.Column] objects
    that correspond to the columns in the given values.

    Arguments:
        values: An object that holds the content of the tables. It must be a
            [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

    Returns:
        A list of Synapse table [Column][synapseclient.table.Column] objects

    Example:

        ```python
        import pandas as pd

        df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
        cols = infer_column_type_from_data(df)
        ```
    """
    test_import_pandas()
    from pandas import DataFrame, isna
    from pandas.api.types import infer_dtype

    if isinstance(values, DataFrame):
        df = values
    else:
        raise ValueError(
            "Values of type %s is not supported. It must be a pandas DataFrame"
            % type(values)
        )

    cols = list()
    for col in df:
        if not col or col.upper() in RESERVED_COLUMN_NAMES:
            continue
        inferred_type = infer_dtype(df[col], skipna=True)
        if inferred_type == "floating":
            # Check if the column is integers, assuming that the row may be an integer or null
            if df[col].apply(lambda x: isna(x) or float(x).is_integer()).all():
                inferred_type = "integer"

        column_type = PANDAS_TABLE_TYPE.get(inferred_type, "STRING")
        if column_type == "STRING":
            maxStrLen = df[col].str.len().max()
            if maxStrLen > 1000:
                cols.append(
                    Column(
                        name=col, column_type=ColumnType["LARGETEXT"], default_value=""
                    )
                )
            else:
                size = int(
                    round(min(1000, max(50, maxStrLen * 1.5)))
                )  # Determine the length of the longest string
                cols.append(
                    Column(
                        name=col,
                        column_type=ColumnType[column_type],
                        maximum_size=size,
                    )
                )
        else:
            cols.append(Column(name=col, column_type=ColumnType[column_type]))
    return cols


def _convert_df_date_cols_to_datetime(
    df: DATA_FRAME_TYPE, date_columns: List
) -> DATA_FRAME_TYPE:
    """
    Convert date columns with epoch time to date time in UTC timezone

    Argumenets:
        df: a pandas dataframe
        date_columns: name of date columns

    Returns:
        A dataframe with epoch time converted to date time in UTC timezone
    """
    test_import_pandas()
    import numpy as np
    from pandas import to_datetime

    # find columns that are in date_columns list but not in dataframe
    diff_cols = list(set(date_columns) - set(df.columns))
    if diff_cols:
        raise ValueError("Please ensure that date columns are already in the dataframe")
    try:
        df[date_columns] = df[date_columns].astype(np.float64)
    except ValueError:
        raise ValueError(
            "Cannot convert epoch time to integer. Please make sure that the date columns that you specified contain valid epoch time value"
        )
    df[date_columns] = df[date_columns].apply(
        lambda x: to_datetime(x, unit="ms", utc=True)
    )
    return df


def _row_labels_from_id_and_version(rows):
    return ["_".join(map(str, row)) for row in rows]


def csv_to_pandas_df(
    filepath: Union[str, BytesIO],
    separator: str = DEFAULT_SEPARATOR,
    quote_char: str = DEFAULT_QUOTE_CHARACTER,
    escape_char: str = DEFAULT_ESCAPSE_CHAR,
    contain_headers: bool = True,
    lines_to_skip: int = 0,
    date_columns: Optional[List[str]] = None,
    list_columns: Optional[List[str]] = None,
    row_id_and_version_in_index: bool = True,
    dtype: Optional[Dict[str, Any]] = None,
    **kwargs,
):
    """
    Convert a csv file to a pandas dataframe

    Arguments:
        filepath: The path to the file.
        separator: The separator for the file, Defaults to `DEFAULT_SEPARATOR`.
                    Passed as `sep` to pandas. If `sep` is supplied as a `kwarg`
                    it will be used instead of this `separator` argument.
        quote_char: The quote character for the file,
                    Defaults to `DEFAULT_QUOTE_CHARACTER`.
                    Passed as `quotechar` to pandas. If `quotechar` is supplied as a `kwarg`
                    it will be used instead of this `quote_char` argument.
        escape_char: The escape character for the file,
                    Defaults to `DEFAULT_ESCAPSE_CHAR`.
        contain_headers: Whether the file contains headers,
                    Defaults to `True`.
        lines_to_skip: The number of lines to skip at the beginning of the file,
                        Defaults to `0`. Passed as `skiprows` to pandas.
                        If `skiprows` is supplied as a `kwarg`
                        it will be used instead of this `lines_to_skip` argument.
        date_columns: The names of the date columns in the file
        list_columns: The names of the list columns in the file
        row_id_and_version_in_index: Whether the file contains rowId and
                                version in the index, Defaults to `True`.
        dtype: The data type for the file, Defaults to `None`.
        **kwargs: Additional keyword arguments to pass to pandas.read_csv. See
                    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
                    for complete list of supported arguments.

    Returns:
        A pandas dataframe
    """
    test_import_pandas()
    from pandas import read_csv

    line_terminator = str(os.linesep)

    pandas_args = {
        "dtype": dtype,
        "sep": separator,
        "quotechar": quote_char,
        "escapechar": escape_char,
        "header": 0 if contain_headers else None,
        "skiprows": lines_to_skip,
    }
    pandas_args.update(kwargs)

    # assign line terminator only if for single character
    # line terminators (e.g. not '\r\n') 'cause pandas doesn't
    # longer line terminators. See: <https://github.com/pydata/pandas/issues/3501>
    # "ValueError: Only length-1 line terminators supported"
    df = read_csv(
        filepath,
        lineterminator=line_terminator if len(line_terminator) == 1 else None,
        **pandas_args,
    )

    # parse date columns if exists
    if date_columns:
        df = _convert_df_date_cols_to_datetime(df, date_columns)
    # Turn list columns into lists
    if list_columns:
        for col in list_columns:
            # Fill NA values with empty lists, it must be a string for json.loads to work
            df.fillna({col: "[]"}, inplace=True)
            df[col] = df[col].apply(json.loads)

    if (
        row_id_and_version_in_index
        and "ROW_ID" in df.columns
        and "ROW_VERSION" in df.columns
    ):
        # combine row-ids (in index) and row-versions (in column 0) to
        # make new row labels consisting of the row id and version
        # separated by a dash.
        zip_args = [df["ROW_ID"], df["ROW_VERSION"]]
        if "ROW_ETAG" in df.columns:
            zip_args.append(df["ROW_ETAG"])

        df.index = _row_labels_from_id_and_version(zip(*zip_args))
        del df["ROW_ID"]
        del df["ROW_VERSION"]
        if "ROW_ETAG" in df.columns:
            del df["ROW_ETAG"]

    return df


def _convert_pandas_row_to_python_types(
    cell: Union[SERIES_TYPE, str, List], column_type: ColumnType
) -> Union[List, datetime, float, int, bool, str]:
    """
    Handle the conversion of a cell item to a Python type based on the column type.

    Args:
        cell: The cell item to convert.

    Returns:
        The list of items to be used as annotations. Or a single instance if that is
            all that is present.
    """
    if column_type == ColumnType.STRING:
        return cell
    elif column_type == ColumnType.DOUBLE:
        return cell.item()
    elif column_type == ColumnType.INTEGER:
        return cell.astype(int).item()
    elif column_type == ColumnType.BOOLEAN:
        return cell.item()
    elif column_type == ColumnType.DATE:
        return cell.item()
    elif column_type == ColumnType.FILEHANDLEID:
        return cell.item()
    elif column_type == ColumnType.ENTITYID:
        return cell
    elif column_type == ColumnType.SUBMISSIONID:
        return cell.astype(int).item()
    elif column_type == ColumnType.EVALUATIONID:
        return cell.astype(int).item()
    elif column_type == ColumnType.LINK:
        return cell
    elif column_type == ColumnType.MEDIUMTEXT:
        return cell
    elif column_type == ColumnType.LARGETEXT:
        return cell
    elif column_type == ColumnType.USERID:
        return cell.astype(int).item()
    elif column_type == ColumnType.STRING_LIST:
        return cell
    elif column_type == ColumnType.INTEGER_LIST:
        return [x for x in cell]
    elif column_type == ColumnType.BOOLEAN_LIST:
        return cell
    elif column_type == ColumnType.DATE_LIST:
        return cell
    elif column_type == ColumnType.ENTITYID_LIST:
        return cell
    elif column_type == ColumnType.USERID_LIST:
        return cell
    elif column_type == ColumnType.JSON:
        return cell
    else:
        return cell
