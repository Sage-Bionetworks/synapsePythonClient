import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
from collections import OrderedDict

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from synapseclient import Synapse
from synapseclient.api import (
    ViewTypeMask,
    delete_entity,
    get_columns,
    get_default_columns,
    get_from_entity_factory,
    post_columns,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseTimeoutError
from synapseclient.core.utils import (
    MB,
    delete_none_keys,
    log_dataclass_diff,
    merge_dataclass_entities,
)
from synapseclient.models.services.search import get_id

from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)
from synapseclient.models.mixins.table_operator import (
    Column,
    ColumnType,
    TableSchemaChangeRequest,
    ColumnChange,
    TableUpdateTransaction,
    DATA_FRAME_TYPE,
    PartialRowSet,
    PartialRow,
    AppendableRowSetRequest,
    CLASSES_THAT_CONTAIN_ROW_ETAG,
    csv_to_pandas_df,
    test_import_pandas,
    _convert_pandas_row_to_python_types,
    QueryResultBundle,
    SumFileSizes,
    SERIES_TYPE,
)


@dataclass
class TableBase:
    """Mixin that extends the functionality of any `table` like entities in Synapse
    to perform a number of operations on the entity such as getting, deleting, or
    updating columns, querying for data, and more."""

    id: None = None
    name: None = None
    parent_id: None = None
    activity: None = None
    version_number: None = None
    _last_persistent_instance: None = None
    _columns_to_delete: Optional[Dict] = None

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


@dataclass
class ViewBase(TableBase):
    """A class that extends the TableOperator and TableRowOperator classes to add
    appropriately handle View-like Synapse entities.

    In the Synapse API, a View is a sub-category of the Table model which includes other Table-like
    entities including: SubmissionView, EntityView, and Dataset.
    """

    view_entity_type: Optional[str] = field(default=None, compare=False)
    """
    The type of view to create. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewTypeMask` enum.
    """

    view_type_mask: Optional[ViewTypeMask] = field(default=None, compare=False)
    """
    The type of view to create. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewTypeMask` enum.
    """

    include_default_columns: Optional[bool] = field(default=True, compare=False)
    """
    When creating a table or view, specifies if default columns should be included.
    Default columns are columns that are automatically added to the table or view. These
    columns are managed by Synapse and cannot be modified. If you attempt to create a
    column with the same name as a default column, you will receive a warning when you
    store the table.

    The column you are overriding will not behave the same as a default column. For
    example, suppose you create a column called `id` on a FileView. When using a
    default column, the `id` stores the Synapse ID of each of the entities included in
    the scope of the view. If you override the `id` column with a new column, the `id`
    column will no longer store the Synapse ID of the entities in the view. Instead, it
    will store the values you provide when you store the table. It will be stored as an
    annotation on the entity for the row you are modifying.
    """


class TableStoreMixin:

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
            # This conditional is to handle for cases where a Column object has not
            # been modified (ie: It's a default Column in Synapse), but it hasn't been
            # associated with this Table yet.
            elif (
                not self._last_persistent_instance
                or not self._last_persistent_instance.columns
                or column.name not in self._last_persistent_instance.columns
            ):
                client.logger.info(
                    f"[{self.id}:{self.name}:Column_{column.name} (Add)]: {column}"
                )
                if not dry_run:
                    columns_to_persist.append(column)
                    column_changes.append(ColumnChange(new_column_id=column.id))

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
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
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

            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when updating the table schema. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds

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
            ).send_job_and_wait_async(synapse_client=client, timeout=job_timeout)

            # Replace the columns after a schema change in case any column names were updated
            updated_columns = OrderedDict()
            for column in self.columns.values():
                updated_columns[column.name] = column
            self.columns = updated_columns
            await self.get_async(
                include_columns=False,
                synapse_client=synapse_client,
            )

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


class ViewStoreMixin(TableStoreMixin):

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store non-row information about a View-like entity
        including the columns and annotations.

        View-like entities often have default columns that are managed by Synapse.
        The default behavior of this function is to include these default columns in the
        table when it is stored. This means that with the default behavior, any columns that
        you have added to your View will be overwritten by the default columns if they have
        the same name. To avoid this behavior, set the `include_default_columns` attribute
        to `False`.

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
            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when updating the table schema. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.


        Returns:
            The View instance stored in synapse.
        """
        client = Synapse.get_client(synapse_client=synapse_client)

        if self.include_default_columns:
            default_columns = await get_default_columns(
                view_entity_type=(
                    self.view_entity_type if self.view_entity_type else None
                ),
                view_type_mask=(
                    self.view_type_mask.value if self.view_type_mask else None
                ),
                synapse_client=synapse_client,
            )
            for default_column in default_columns:
                if (
                    default_column.name in self.columns
                    and default_column != self.columns[default_column.name]
                ):
                    client.logger.warning(
                        f"Column '{default_column.name}' already exists in dataset. "
                        "Overwriting with default column."
                    )
                self.columns[default_column.name] = default_column
        # check that column names match this regex "^[a-zA-Z0-9,_.]+"
        for _, column in self.columns.items():
            if not re.match(r"^[a-zA-Z0-9,_.]+$", column.name):
                raise ValueError(
                    f"Column name '{column.name}' does not match the regex pattern '^[a-zA-Z0-9,_.]+$'"
                )
        return await super().store_async(
            dry_run=dry_run, job_timeout=job_timeout, synapse_client=synapse_client
        )


class DeleteMixin:

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


class GetMixin:

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


class ColumnMixin:

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
        elif isinstance(columns, dict) or isinstance(columns, OrderedDict):
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


class UpsertMixin:

    def _construct_select_statement_for_upsert(
        self,
        df: DATA_FRAME_TYPE,
        all_columns_from_df: List[str],
        primary_keys: List[str],
    ) -> str:
        """
        Create the select statement for a given DataFrame. This is used to select data
        from Synapse to determine if a row already exists in the table. This is used
        in the upsert method to determine if a row should be updated or inserted.

        Arguments:
            df: The DataFrame that contains the data to be upserted.
            all_columns_from_df: A list of all the columns in the DataFrame.
            primary_keys: A list of the columns that are used to determine if a row
                already exists in the table.

        Returns:
            The select statement that can be used to query Synapse to determine if a row
            already exists in the
        """

        select_statement = "SELECT ROW_ID, "

        if self.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG:
            select_statement += "ROW_ETAG, "

        select_statement += f"{', '.join(all_columns_from_df)} FROM {self.id} WHERE "
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
                values_for_where_statement = set(
                    [f"'{value}'" for value in df[upsert_column] if value is not None]
                )

            elif column_model.column_type == ColumnType.BOOLEAN:
                include_true = False
                include_false = False
                for value in df[upsert_column]:
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
                values_for_where_statement = set(
                    [str(value) for value in df[upsert_column] if value is not None]
                )
            if not values_for_where_statement:
                continue
            where_statements.append(
                f"\"{upsert_column}\" IN ({', '.join(values_for_where_statement)})"
            )

        where_statement = " AND ".join(where_statements)
        select_statement += where_statement
        return select_statement

    def _construct_partial_rows_for_upsert(
        self,
        results: DATA_FRAME_TYPE,
        chunk_to_check_for_upsert: DATA_FRAME_TYPE,
        primary_keys: List[str],
        contains_etag: bool,
    ) -> Tuple[List[PartialRow], List[int], List[int], List[str]]:
        """
        Handles the construction of the PartialRow objects that will be used to update
        rows in Synapse. This method is used in the upsert method to determine which
        rows need to be updated.

        Arguments:
            results: The DataFrame that contains the data that was queried from Synapse.
            chunk_to_check_for_upsert: The DataFrame that contains the data that is
                being upserted.
            primary_keys: A list of the columns that are used to determine if a row
                already exists in the table.

        Returns:
            A tuple containing a list of PartialRow objects that will be used to update
            rows in Synapse, a list of the indexs of the rows in the original
            DataFrame that have changes, a list of the indexes of the rows in the
            original DataFrame that do not have changes, and a list of the etags for
            the rows that have changes.
        """

        from pandas import isna

        rows_to_update: List[PartialRow] = []
        indexs_of_original_df_with_changes = []
        indexs_of_original_df_without_changes = []
        etags = []
        for row in results.itertuples(index=False):
            row_etag = None

            if contains_etag:
                row_etag = row.ROW_ETAG

            partial_change_values = {}

            # Find the matching row in `values` that matches the row in `results` for the primary_keys
            matching_conditions = chunk_to_check_for_upsert[primary_keys[0]] == getattr(
                row, primary_keys[0]
            )
            for col in primary_keys[1:]:
                matching_conditions &= chunk_to_check_for_upsert[col] == getattr(
                    row, col
                )
            matching_row = chunk_to_check_for_upsert.loc[matching_conditions]

            # Determines which cells need to be updated
            for column in chunk_to_check_for_upsert.columns:
                if len(matching_row[column].values) > 1:
                    raise ValueError(
                        f"The values for the keys being upserted must be unique in the table: [{matching_row}]"
                    )
                elif column not in self.columns:
                    continue
                if len(matching_row[column].values) == 0:
                    continue
                column_id = self.columns[column].id
                column_type = self.columns[column].column_type
                cell_value = matching_row[column].values[0]
                if not hasattr(row, column) or cell_value != getattr(row, column):
                    if (
                        isinstance(cell_value, list) and len(cell_value) > 0
                    ) or not isna(cell_value):
                        partial_change_values[column_id] = (
                            _convert_pandas_row_to_python_types(
                                cell=cell_value, column_type=column_type
                            )
                        )
                    else:
                        partial_change_values[column_id] = None

            if partial_change_values:
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
                if row_etag:
                    etags.append(row_etag)
            else:
                indexs_of_original_df_without_changes.append(matching_row.index[0])
        return (
            rows_to_update,
            indexs_of_original_df_with_changes,
            indexs_of_original_df_without_changes,
            etags,
        )

    async def _push_row_updates_to_synapse(
        self,
        rows_to_update: List[PartialRow],
        update_size_bytes: int,
        progress_bar: tqdm,
        job_timeout: int,
        client: Synapse,
    ) -> None:
        current_chunk_size = 0
        chunk = []
        for row in rows_to_update:
            row_size = row.size()
            if current_chunk_size + row_size > update_size_bytes:
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

                await request.send_job_and_wait_async(
                    synapse_client=client, timeout=job_timeout
                )
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
            ).send_job_and_wait_async(synapse_client=client, timeout=job_timeout)
            progress_bar.update(len(chunk))

    async def _wait_for_eventually_consistent_changes(
        self,
        original_etags_to_track: List[str],
        wait_for_eventually_consistent_view_timeout: int,
        synapse_client: Synapse,
    ) -> None:
        """
        Given that a change has been made to a view, this method will wait for the
        changes to be reflected in the view. This is done by querying the view for the
        etags that were changed. If the etags are found in the view then we know that
        the view has not yet been updated with the changes that were made. This method
        will wait for the changes to be reflected in the view.

        Arguments:
            original_etags_to_track: A list of the etags that were changed.
            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for the changes to be reflected in the view.
            synapse_client: The Synapse client to use to query the view.

        Raises:
            SynapseTimeoutError: If the changes are not reflected in the view within
                the timeout period.

        Returns:
            None
        """
        with logging_redirect_tqdm(loggers=[synapse_client.logger]):
            number_of_changes_to_wait_for = len(original_etags_to_track)
            progress_bar = tqdm(
                total=number_of_changes_to_wait_for,
                desc="Waiting for eventually-consistent changes to show up in the view",
                unit_scale=True,
                smoothing=0,
            )
            start_time = time.time()

            while (
                time.time() - start_time < wait_for_eventually_consistent_view_timeout
            ):
                quoted_etags = [f"'{etag}'" for etag in original_etags_to_track]
                wait_select_statement = f"select etag from {self.id} where etag IN ({','.join(quoted_etags)})"
                results = await self.query_async(
                    query=wait_select_statement,
                    synapse_client=synapse_client,
                    include_row_id_and_row_version=False,
                )
                for row in results.itertuples(index=False):
                    if row.etag in original_etags_to_track:
                        original_etags_to_track.remove(row.etag)
                        progress_bar.update(1)
                progress_bar.refresh()
                if not original_etags_to_track:
                    progress_bar.close()
                    break
                await asyncio.sleep(1)
            else:
                raise SynapseTimeoutError(
                    f"Timeout waiting for eventually consistent view: {time.time() - start_time} seconds"
                )

    async def upsert_rows_async(
        self,
        values: DATA_FRAME_TYPE,
        primary_keys: List[str],
        dry_run: bool = False,
        *,
        rows_per_query: int = 50000,
        update_size_bytes: int = 1.9 * MB,
        insert_size_bytes: int = 900 * MB,
        job_timeout: int = 600,
        wait_for_eventually_consistent_view: bool = False,
        wait_for_eventually_consistent_view_timeout: int = 600,
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

        The following is a Sequence Diagram that describces the upsert process at a
        high level:

        ```mermaid
        sequenceDiagram
            participant User
            participant Table
            participant Synapse

            User->>Table: upsert_rows()

            loop Query and Process Updates in Chunks (rows_per_query)
                Table->>Synapse: Query existing rows using primary keys
                Synapse-->>Table: Return matching rows
                Note Over Table: Create partial row updates

                loop For results from query
                    Note Over Table: Sum row/chunk size
                    alt Chunk size exceeds update_size_bytes
                        Table->>Synapse: Push update chunk
                        Synapse-->>Table: Acknowledge update
                    end
                    Table->>Table: Add row to chunk
                end

                alt Remaining updates exist
                    Table->>Synapse: Push final update chunk
                    Synapse-->>Table: Acknowledge update
                end
            end

            alt New rows exist
                Table->>Table: Identify new rows for insertion
                Table->>Table: Call `store_rows()` function
            end

            Table-->>User: Upsert complete
        ```

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

            update_size_bytes: The maximum size of the request that will be sent to Synapse
                when updating rows of data. The default is 1.9MB.

            insert_size_bytes: The maximum size of the request that will be sent to Synapse
                when inserting rows of data. The default is 900MB.

            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when inserting, and updating rows of data. Each individual
                request to Synapse will be sent as an independent job. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds

            wait_for_eventually_consistent_view: Only used if the table is a view. If
                set to True this will wait for the view to reflect any changes that
                you've made to the view. This is useful if you need to query the view
                after making changes to the data.

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.


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
        from pandas import DataFrame

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
        original_etags_to_track = []
        indexes_of_original_df_with_changes = []
        indexes_of_original_df_with_no_changes = []
        total_row_count_updated = 0

        with logging_redirect_tqdm(loggers=[client.logger]):
            progress_bar = tqdm(
                total=len(values),
                desc="Querying & Updating rows",
                unit_scale=True,
                smoothing=0,
            )
            for individual_chunk in chunk_list:
                select_statement = self._construct_select_statement_for_upsert(
                    df=individual_chunk,
                    all_columns_from_df=all_columns_from_df,
                    primary_keys=primary_keys,
                )

                results = await self.query_async(
                    query=select_statement, synapse_client=synapse_client
                )

                (
                    rows_to_update,
                    indexes_with_updates,
                    indexes_without_updates,
                    etags_to_track,
                ) = self._construct_partial_rows_for_upsert(
                    results=results,
                    chunk_to_check_for_upsert=individual_chunk,
                    primary_keys=primary_keys,
                    contains_etag=contains_etag,
                )
                total_row_count_updated += len(rows_to_update)
                indexes_of_original_df_with_changes.extend(indexes_with_updates)
                indexes_of_original_df_with_no_changes.extend(indexes_without_updates)
                if (
                    etags_to_track
                    and contains_etag
                    and wait_for_eventually_consistent_view
                ):
                    original_etags_to_track.extend(etags_to_track)

                if not dry_run and rows_to_update:
                    await self._push_row_updates_to_synapse(
                        rows_to_update=rows_to_update,
                        update_size_bytes=update_size_bytes,
                        progress_bar=progress_bar,
                        client=client,
                        job_timeout=job_timeout,
                    )
                elif dry_run:
                    progress_bar.update(len(rows_to_update))
                progress_bar.update(len(individual_chunk.index) - len(rows_to_update))

                rows_to_update: List[PartialRow] = []
            progress_bar.update(progress_bar.total - progress_bar.n)
            progress_bar.refresh()
            progress_bar.close()

        rows_to_insert_df = values.loc[
            ~values.index.isin(
                indexes_of_original_df_with_changes
                + indexes_of_original_df_with_no_changes
            )
        ]

        client.logger.info(
            f"[{self.id}:{self.name}]: Found {total_row_count_updated}"
            f" rows to update and {len(rows_to_insert_df)} rows to insert"
        )

        if wait_for_eventually_consistent_view and original_etags_to_track:
            await self._wait_for_eventually_consistent_changes(
                original_etags_to_track=original_etags_to_track,
                wait_for_eventually_consistent_view_timeout=wait_for_eventually_consistent_view_timeout,
                synapse_client=synapse_client,
            )
        # TODO: Replace this conditional with something that indicates it is a Table
        # View-like objects cannot insert rows this way.
        if False:
            if not dry_run and not rows_to_insert_df.empty:
                await self.store_rows_async(
                    values=rows_to_insert_df,
                    dry_run=dry_run,
                    insert_size_bytes=insert_size_bytes,
                    synapse_client=synapse_client,
                )


class QueryMixin:

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
            sum_file_sizes=(
                SumFileSizes(
                    sum_file_size_bytes=results.sumFileSizes.get(
                        "sumFileSizesBytes", None
                    ),
                    greater_than=results.sumFileSizes.get("greaterThan", None),
                )
                if results.sumFileSizes
                else None
            ),
            last_updated_on=results.lastUpdatedOn,
        )
