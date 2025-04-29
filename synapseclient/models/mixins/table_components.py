import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Protocol, Tuple, TypeVar, Union

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import (
    ViewEntityType,
    ViewTypeMask,
    delete_entity,
    get_columns,
    get_default_columns,
    get_from_entity_factory,
    post_columns,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.exceptions import SynapseTimeoutError
from synapseclient.core.upload.multipart_upload_async import (
    multipart_upload_dataframe_async,
    multipart_upload_file_async,
    multipart_upload_partial_file_async,
)
from synapseclient.core.utils import MB, log_dataclass_diff, merge_dataclass_entities
from synapseclient.models import Activity
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)
from synapseclient.models.table_components import (
    AppendableRowSetRequest,
    Column,
    ColumnChange,
    ColumnExpansionStrategy,
    ColumnType,
    CsvTableDescriptor,
    PartialRow,
    PartialRowSet,
    QueryResultBundle,
    SchemaStorageStrategy,
    SnapshotRequest,
    SumFileSizes,
    TableSchemaChangeRequest,
    TableUpdateTransaction,
    UploadToTableRequest,
)

CLASSES_THAT_CONTAIN_ROW_ETAG = [
    "Dataset",
    "EntityView",
    "DatasetCollection",
    "SubmissionView",
]
CLASSES_WITH_READ_ONLY_SCHEMA = ["MaterializedView", "VirtualTable"]

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
    """This function is called within other functions and methods to ensure that pandas is installed."""
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
class TableBase:
    """Base class for any `Table`-like entities in Synapse.
    Provides the minimum required attributes for any such entity.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableEntity.html>
    """

    id: None = None
    name: None = None
    parent_id: None = None
    activity: None = None
    version_number: None = None
    _last_persistent_instance: None = None
    _columns_to_delete: Optional[Dict] = None


@dataclass
class ViewBase(TableBase):
    """A class that extends TableBase for additional attributes specific to `View`-like objects.

    In the Synapse API, a `View` is a sub-category of the `Table` model interface which includes other `Table`-like
    entities including: `SubmissionView`, `EntityView`, and `Dataset`.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/View.html>
    """

    view_entity_type: Optional[ViewEntityType] = field(default=None, compare=False)
    """
    The type of view. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewEntityType` enum.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ViewEntityType.html>
    """

    view_type_mask: Optional[ViewTypeMask] = field(default=None, compare=False)
    """
    Bit mask representing the types to include in the view. This is used to determine
    the default columns that are added to the table. Must be defined as a `ViewTypeMask` enum.
    <https://rest-docs.synapse.org/rest/GET/column/tableview/defaults.html>
    """

    include_default_columns: Optional[bool] = field(default=True, compare=False)
    """
    When creating a entityview or view, specifies if default columns should be included.
    Default columns are columns that are automatically added to the entityview or view. These
    columns are managed by Synapse and cannot be modified. If you attempt to create a
    column with the same name as a default column, you will receive a warning when you
    store the entityview.

    **`include_default_columns` is only used if this is the first time that the view is
    being stored.** If you are updating an existing view this attribute will be ignored.
    If you want to add all default columns back to your view then you may use this code
    snippet to accomplish this:

    ```python
    import asyncio
    from synapseclient import Synapse
    from synapseclient.models import EntityView # May also use: Dataset

    syn = Synapse()
    syn.login()

    async def main():
        view = await EntityView(id="syn1234").get_async()
        await view._append_default_columns()
        await view.store_async()

    asyncio.run(main())
    ```

    The column you are overriding will not behave the same as a default column. For
    example, suppose you create a column called `id` on a EntityView. When using a
    default column, the `id` stores the Synapse ID of each of the entities included in
    the scope of the view. If you override the `id` column with a new column, the `id`
    column will no longer store the Synapse ID of the entities in the view. Instead, it
    will store the values you provide when you store the entityview. It will be stored as an
    annotation on the entity for the row you are modifying.
    """


@async_to_sync
class TableStoreMixin:
    """Mixin class providing methods for storing a `Table`-like entity."""

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
        if (
            self.__class__.__name__ in CLASSES_WITH_READ_ONLY_SCHEMA
            or not self.has_columns_changed
            or not self.columns
        ):
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

        if (not self._last_persistent_instance) and (
            hasattr(self, "_append_default_columns")
            and hasattr(self, "include_default_columns")
            and self.include_default_columns
        ):
            await self._append_default_columns(synapse_client=synapse_client)

        if (
            self.__class__.__name__ not in CLASSES_WITH_READ_ONLY_SCHEMA
            and self.columns
        ):
            # check that column names match this regex "^[a-zA-Z0-9 _\-\.\+\(\)']+$"
            for _, column in self.columns.items():
                if not re.match(r"^[a-zA-Z0-9 _\-\.\+\(\)']+$", column.name):
                    raise ValueError(
                        f"Column name '{column.name}' contains invalid characters. "
                        "Names may only contain: letters, numbers, spaces, underscores, "
                        "hyphens, periods, plus signs, apostrophes, and parentheses."
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


@async_to_sync
class ViewStoreMixin(TableStoreMixin):
    """Mixin class that extends `TableStoreMixin` providing methods for storing a `View`-like entity."""

    async def _append_default_columns(
        self, synapse_client: Optional[Synapse] = None
    ) -> None:
        """
        Append default columns to the table. This method will only append default
        columns if the `include_default_columns` attribute is set to `True`. This is
        called in the `super().store_async()` method because we need to respect the
        ordering of columns that are already present on the View.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            None
        """
        client = Synapse.get_client(synapse_client=synapse_client)

        if self.include_default_columns:
            view_type_mask = None
            if self.view_type_mask:
                if isinstance(self.view_type_mask, ViewTypeMask):
                    view_type_mask = self.view_type_mask.value
                else:
                    view_type_mask = self.view_type_mask

            default_columns = await get_default_columns(
                view_entity_type=(
                    self.view_entity_type if self.view_entity_type else None
                ),
                view_type_mask=view_type_mask,
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
        return await super().store_async(
            dry_run=dry_run, job_timeout=job_timeout, synapse_client=synapse_client
        )


@async_to_sync
class DeleteMixin:
    """Mixin class providing methods for deleting a `Table`-like entity."""

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
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            async def main():
                await Table(id="syn4567").delete_async()

            asyncio.run(main())
            ```
        """
        if not (self.id or (self.name and self.parent_id)):
            raise ValueError(
                "The table must have an id or a (name and `parent_id`) set."
            )

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await delete_entity(
            entity_id=entity_id,
            synapse_client=synapse_client,
        )


@async_to_sync
class GetMixin:
    """Mixin class providing methods for getting information about a `Table`-like entity."""

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
            from synapseclient.models import Table

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
            from synapseclient.models import Table

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
                "The table must have an id or a (name and `parent_id`) set."
            )

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await get_from_entity_factory(
            entity_to_update=self,
            version=self.version_number,
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
    """Mixin class providing methods for managing columns in a `Table`-like entity."""

    @property
    def has_columns_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance
            or (not self._last_persistent_instance.columns and self.columns)
            or self._last_persistent_instance.columns != self.columns
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
                insert_index = min(index, len(self.columns))
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
        ],
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

    """Mixin class providing methods for upserting data into a `Table`-like entity."""


def _construct_select_statement_for_upsert(
    entity: TableBase,
    df: DATA_FRAME_TYPE,
    all_columns_from_df: List[str],
    primary_keys: List[str],
    wait_for_eventually_consistent_view: bool,
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
        wait_for_eventually_consistent_view: If True, the select statement will
            include the ROW_ID, ROW_ETAG, and id columns. If False, the select
            statement will only include the ROW_ID column.

    Returns:
        The select statement that can be used to query Synapse to determine if a row
        already exists in the
    """

    if entity.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG:
        if wait_for_eventually_consistent_view:
            select_statement = "SELECT id, ROW_ID, ROW_ETAG, "

            if "id" in all_columns_from_df:
                all_columns_from_df.remove("id")
        else:
            select_statement = "SELECT ROW_ID, ROW_ETAG, "

    else:
        select_statement = "SELECT ROW_ID, "

    select_statement += f"{', '.join(all_columns_from_df)} FROM {entity.id} WHERE "
    where_statements = []
    for upsert_column in primary_keys:
        column_model = entity.columns[upsert_column]
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
    entity: TableBase,
    results: DATA_FRAME_TYPE,
    chunk_to_check_for_upsert: DATA_FRAME_TYPE,
    primary_keys: List[str],
    contains_etag: bool,
    wait_for_eventually_consistent_view: bool,
) -> Tuple[List[PartialRow], List[int], List[int], Dict[str, str]]:
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
        contains_etag: If True, the results DataFrame contains the ROW_ETAG column.
        wait_for_eventually_consistent_view: If True, the results DataFrame contains
            the id columns.

    Returns:
        A tuple containing a list of PartialRow objects that will be used to update
        rows in Synapse, a list of the indexs of the rows in the original
        DataFrame that have changes, a list of the indexes of the rows in the
        original DataFrame that do not have changes, and a dictionary of the synapse IDs
        for the key with the etag of the row that was changed.
    """

    from pandas import isna

    rows_to_update: List[PartialRow] = []
    indexs_of_original_df_with_changes = []
    indexs_of_original_df_without_changes = []
    syn_id_and_etags = {}
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
            matching_conditions &= chunk_to_check_for_upsert[col] == getattr(row, col)
        matching_row = chunk_to_check_for_upsert.loc[matching_conditions]

        # Determines which cells need to be updated
        for column in chunk_to_check_for_upsert.columns:
            if len(matching_row[column].values) > 1:
                raise ValueError(
                    f"The values for the keys being upserted must be unique in the table: [{matching_row}]"
                )
            elif column not in entity.columns:
                continue
            if len(matching_row[column].values) == 0:
                continue
            column_id = entity.columns[column].id
            column_type = entity.columns[column].column_type
            cell_value = matching_row[column].values[0]
            if not hasattr(row, column) or cell_value != getattr(row, column):
                if (isinstance(cell_value, list) and len(cell_value) > 0) or not isna(
                    cell_value
                ):
                    partial_change_values[
                        column_id
                    ] = _convert_pandas_row_to_python_types(
                        cell=cell_value, column_type=column_type
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
            if wait_for_eventually_consistent_view and row_etag and row.id:
                syn_id_and_etags[row.id] = row_etag
        else:
            indexs_of_original_df_without_changes.append(matching_row.index[0])
    return (
        rows_to_update,
        indexs_of_original_df_with_changes,
        indexs_of_original_df_without_changes,
        syn_id_and_etags,
    )


async def _push_row_updates_to_synapse(
    entity: TableBase,
    rows_to_update: List[PartialRow],
    update_size_bytes: int,
    progress_bar: tqdm,
    job_timeout: int,
    client: Synapse,
) -> List[TableUpdateTransaction]:
    results = []
    current_chunk_size = 0
    chunk = []
    for row in rows_to_update:
        row_size = row.size()
        if current_chunk_size + row_size > update_size_bytes:
            change = AppendableRowSetRequest(
                entity_id=entity.id,
                to_append=PartialRowSet(
                    table_id=entity.id,
                    rows=chunk,
                ),
            )

            request = TableUpdateTransaction(
                entity_id=entity.id,
                changes=[change],
            )

            result = await request.send_job_and_wait_async(
                synapse_client=client, timeout=job_timeout
            )
            results.append(result)
            progress_bar.update(len(chunk))
            chunk = []
            current_chunk_size = 0
        chunk.append(row)
        current_chunk_size += row_size

    if chunk:
        change = AppendableRowSetRequest(
            entity_id=entity.id,
            to_append=PartialRowSet(
                table_id=entity.id,
                rows=chunk,
            ),
        )

        result = await TableUpdateTransaction(
            entity_id=entity.id,
            changes=[change],
        ).send_job_and_wait_async(synapse_client=client, timeout=job_timeout)
        progress_bar.update(len(chunk))
        results.append(result)
    return results


async def _wait_for_eventually_consistent_changes(
    entity: TableBase,
    original_synids_and_etags_to_track: Dict[str, str],
    wait_for_eventually_consistent_view_timeout: int,
    row_update_results: List[TableUpdateTransaction],
    synapse_client: Synapse,
) -> None:
    """
    Given that a change has been made to a view, this method will wait for the
    changes to be reflected in the view. This is done by querying the view for the
    etags that were changed. If the etags are found in the view then we know that
    the view has not yet been updated with the changes that were made. This method
    will wait for the changes to be reflected in the view.

    Arguments:
        original_synids_and_etags_to_track: A dictionary of the synapse IDs for the
            key with the etag of the row that was changed.
        wait_for_eventually_consistent_view_timeout: The maximum amount of time to
            wait for the changes to be reflected in the view.
        row_update_results: The result of the row updates that were made. Used to
            determine which changes we should wait for, and which changes we should not
            wait for.
        synapse_client: The Synapse client to use to query the view.

    Raises:
        SynapseTimeoutError: If the changes are not reflected in the view within
            the timeout period.

    Returns:
        None
    """
    with logging_redirect_tqdm(loggers=[synapse_client.logger]):
        etags_to_track = []
        for row_update_result in row_update_results:
            if row_update_result.entities_with_changes_applied:
                for (
                    entity_with_change
                ) in row_update_result.entities_with_changes_applied:
                    etags_to_track.append(
                        original_synids_and_etags_to_track.get(entity_with_change)
                    )
        number_of_changes_to_wait_for = len(etags_to_track)
        progress_bar = tqdm(
            total=number_of_changes_to_wait_for,
            desc="Waiting for eventually-consistent changes to show up in the view",
            unit_scale=True,
            smoothing=0,
        )
        start_time = time.time()

        while time.time() - start_time < wait_for_eventually_consistent_view_timeout:
            quoted_etags = [f"'{etag}'" for etag in etags_to_track]
            wait_select_statement = (
                f"select etag from {entity.id} where etag IN ({','.join(quoted_etags)})"
            )
            results = await entity.query_async(
                query=wait_select_statement,
                synapse_client=synapse_client,
                include_row_id_and_row_version=False,
            )

            etags_in_results = results["etag"].values
            etags_to_remove = []
            for etag in etags_to_track:
                if etag not in etags_in_results:
                    etags_to_remove.append(etag)
            for etag in etags_to_remove:
                etags_to_track.remove(etag)
                progress_bar.update(1)

            progress_bar.refresh()
            if not etags_to_track:
                progress_bar.close()
                break
            await asyncio.sleep(1)
        else:
            raise SynapseTimeoutError(
                f"Timeout waiting for eventually consistent view: {time.time() - start_time} seconds"
            )


async def _upsert_rows_async(
    entity: Union[TableBase, ViewBase],
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
    This is used to internally wrap the upsert_rows_async method. This is used to
    allow for the method to be overridden in the ViewUpdateMixin class with another
    method name.
    """
    test_import_pandas()
    from pandas import DataFrame

    if not entity._last_persistent_instance:
        await entity.get_async(include_columns=True, synapse_client=synapse_client)

    if not entity.columns:
        raise ValueError(
            "There are no columns on this table. Unable to proceed with an upsert operation."
        )

    if wait_for_eventually_consistent_view and "id" not in entity.columns:
        raise ValueError(
            "The 'id' column is required to wait for eventually consistent views."
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
    contains_etag = entity.__class__.__name__ in CLASSES_THAT_CONTAIN_ROW_ETAG
    original_synids_and_etags_to_track = {}
    indexes_of_original_df_with_changes = []
    indexes_of_original_df_with_no_changes = []
    total_row_count_to_update = 0
    row_update_results = None

    with logging_redirect_tqdm(loggers=[client.logger]):
        progress_bar = tqdm(
            total=len(values),
            desc="Querying & Updating rows",
            unit_scale=True,
            smoothing=0,
        )
        for individual_chunk in chunk_list:
            select_statement = _construct_select_statement_for_upsert(
                entity=entity,
                df=individual_chunk,
                all_columns_from_df=all_columns_from_df,
                primary_keys=primary_keys,
                wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
            )

            results = await entity.query_async(
                query=select_statement, synapse_client=synapse_client
            )

            (
                rows_to_update,
                indexes_with_updates,
                indexes_without_updates,
                syn_id_and_etag_dict,
            ) = _construct_partial_rows_for_upsert(
                entity=entity,
                results=results,
                chunk_to_check_for_upsert=individual_chunk,
                primary_keys=primary_keys,
                contains_etag=contains_etag,
                wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
            )
            total_row_count_to_update += len(rows_to_update)
            indexes_of_original_df_with_changes.extend(indexes_with_updates)
            indexes_of_original_df_with_no_changes.extend(indexes_without_updates)
            if syn_id_and_etag_dict:
                original_synids_and_etags_to_track.update(syn_id_and_etag_dict)

            if not dry_run and rows_to_update:
                row_update_results = await _push_row_updates_to_synapse(
                    entity=entity,
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
            indexes_of_original_df_with_changes + indexes_of_original_df_with_no_changes
        )
    ]

    total_row_count_actually_updated = 0
    if row_update_results:
        for result in row_update_results:
            if result.entities_with_changes_applied:
                total_row_count_actually_updated += len(
                    result.entities_with_changes_applied
                )

    additional_message = ""
    if total_row_count_actually_updated < total_row_count_to_update:
        additional_message = f". {total_row_count_to_update - total_row_count_actually_updated} rows could not be updated."

    client.logger.info(
        f"[{entity.id}:{entity.name}]: Found {total_row_count_actually_updated or total_row_count_to_update}"
        f" rows to update and {len(rows_to_insert_df)} rows to insert"
        + additional_message
    )

    if wait_for_eventually_consistent_view and original_synids_and_etags_to_track:
        await _wait_for_eventually_consistent_changes(
            entity=entity,
            original_synids_and_etags_to_track=original_synids_and_etags_to_track,
            wait_for_eventually_consistent_view_timeout=wait_for_eventually_consistent_view_timeout,
            row_update_results=row_update_results,
            synapse_client=client,
        )

    # Only Tables can insert rows directly. Views and other table-like objects cannot.
    if not isinstance(entity, ViewBase):
        if not dry_run and not rows_to_insert_df.empty:
            await entity.store_rows_async(
                values=rows_to_insert_df,
                dry_run=dry_run,
                insert_size_bytes=insert_size_bytes,
                synapse_client=synapse_client,
            )


@async_to_sync
class TableUpsertMixin:
    async def upsert_rows_async(
        self,
        values: Union[str, Dict[str, Any], DATA_FRAME_TYPE],
        primary_keys: List[str],
        dry_run: bool = False,
        *,
        rows_per_query: int = 50000,
        update_size_bytes: int = 1.9 * MB,
        insert_size_bytes: int = 900 * MB,
        job_timeout: int = 600,
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
            to update, update the data, then use the [store_rows_async][synapseclient.models.mixins.table_components.TableStoreRowMixin.store_rows_async] method to
            update the data in Synapse. Any rows you want to insert may be added
            to the DataFrame that is passed to the [store_rows_async][synapseclient.models.mixins.table_components.TableStoreRowMixin.store_rows_async] method.
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

                - A string holding the path to a CSV file. The data will be read into a
                    [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe).
                    The code makes assumptions about the format of the columns in the
                    CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_components.csv_to_pandas_df]
                    function. You may pass in additional arguments to the `csv_to_pandas_df`
                    function by passing them in as keyword arguments to this function.
                - A dictionary where the key is the column name and the value is one or
                    more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). You may pass in additional arguments to the `pd.DataFrame` function by passing them in as keyword arguments to this function. Read about the available arguments in the [Pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) documentation.
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

                df = pd.DataFrame({
                    'col1': ['A', 'B', 'C'],
                    'col2': [22, 2, 3],
                    'col3': [1, 33, 3],
                })

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

                dictionary_of_data = {
                    'col1': ['A', 'B'],
                    'col2': [None, 2],
                    'col3': [1, None],
                }

                await table.upsert_rows_async(values=dictionary_of_data, primary_keys=["col1"])

            asyncio.run(main())
            ```


            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    |      | 1    |
            | B    | 2    |      |

        """
        return await _upsert_rows_async(
            entity=self,
            values=values,
            primary_keys=primary_keys,
            dry_run=dry_run,
            rows_per_query=rows_per_query,
            update_size_bytes=update_size_bytes,
            insert_size_bytes=insert_size_bytes,
            job_timeout=job_timeout,
            synapse_client=synapse_client,
            **kwargs,
        )


@async_to_sync
class ViewUpdateMixin:
    """Mixin class providing methods for updating rows in a `View`-like entity.
    Functionality for inserting rows is not supported for `View`-like entities. The update
    functionality will only work for values in custom columns within a `View`-like
    entity.
    """

    async def update_rows_async(
        self,
        values: Union[str, Dict[str, Any], DATA_FRAME_TYPE],
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
        """This method leverages the logic provided by [upsert_rows_async][synapseclient.models.Table.upsert_rows_async] to provide
        an interface for updating rows in a `View`-like entity. Update functionality will only work for
        values in custom columns within a `View`-like entity.

        Limitations:

        - When updating many rows the requests to Synapse will be chunked into smaller
            requests. The limit is 2MB per request. This chunking will happen
            automatically and should not be a concern for most users. If you are
            having issues with the request being too large you may lower the
            number of rows you are trying to update.
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

                - A string holding the path to a CSV file. The data will be read into a
                    [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe).
                    The code makes assumptions about the format of the columns in the
                    CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_components.csv_to_pandas_df]
                    function. You may pass in additional arguments to the `csv_to_pandas_df`
                    function by passing them in as keyword arguments to this function.
                - A dictionary where the key is the column name and the value is one or
                    more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). You may pass in additional arguments to the `pd.DataFrame` function by passing them in as keyword arguments to this function. Read about the available arguments in the [Pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) documentation.
                - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

            primary_keys: The columns to use to determine if a row already exists. If
                a row exists with the same values in the columns specified in this list
                the row will be updated. If a row does not exist nothing will be done.

            dry_run: If set to True the data will not be updated in Synapse. A message
                will be printed to the console with the number of rows that would have
                been updated and inserted. If you would like to see the data that would
                be updated and inserted you may set the `dry_run` argument to True and
                set the log level to DEBUG by setting the debug flag when creating
                your Synapse class instance like: `syn = Synapse(debug=True)`.

            rows_per_query: The number of rows that will be queried from Synapse per
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
                after making changes to the data. If you set this value to `True` your
                view must contain the default `id` column which is the Synapse ID of the
                row. If you do not have this column in your view you will need to add it
                to the view before you can use this feature. The default is False

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.
        """
        await _upsert_rows_async(
            entity=self,
            values=values,
            primary_keys=primary_keys,
            dry_run=dry_run,
            rows_per_query=rows_per_query,
            update_size_bytes=update_size_bytes,
            insert_size_bytes=insert_size_bytes,
            job_timeout=job_timeout,
            wait_for_eventually_consistent_view=wait_for_eventually_consistent_view,
            wait_for_eventually_consistent_view_timeout=wait_for_eventually_consistent_view_timeout,
            synapse_client=synapse_client,
            **kwargs,
        )


class QueryMixinSynchronousProtocol(Protocol):
    """Protocol for the synchronous query methods."""

    @staticmethod
    def query(
        query: str,
        include_row_id_and_row_version: bool = True,
        convert_to_datetime: bool = False,
        download_location=None,
        quote_character='"',
        escape_character="\\",
        line_end=str(os.linesep),
        separator=",",
        header=True,
        *,
        synapse_client: Optional[Synapse] = None,
        **kwargs,
    ) -> Union[DATA_FRAME_TYPE, str]:
        """Query for data on a table stored in Synapse. The results will always be
        returned as a Pandas DataFrame unless you specify a `download_location` in which
        case the results will be downloaded to that location. There are a number of
        arguments that you may pass to this function depending on if you are getting
        the results back as a DataFrame or downloading the results to a file.

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
            convert_to_datetime: (DataFrame only) If set to True, will convert all
                Synapse DATE columns from UNIX timestamp integers into UTC datetime
                objects

            download_location: (CSV Only) If set to a path the results will be
                downloaded to that directory. The results will be downloaded as a CSV
                file. A path to the downloaded file will be returned instead of a
                DataFrame.

            quote_character: (CSV Only) The character to use to quote fields. The
                default is a double quote.

            escape_character: (CSV Only) The character to use to escape special
                characters. The default is a backslash.

            line_end: (CSV Only) The character to use to end a line. The default is
                the system's line separator.

            separator: (CSV Only) The character to use to separate fields. The default
                is a comma.

            header: (CSV Only) If set to True the first row will be used as the header
                row. The default is True.

            **kwargs: (DataFrame only) Additional keyword arguments to pass to
                pandas.read_csv. See
                <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>
                for complete list of supported arguments. This is exposed as
                internally the query downloads a CSV from Synapse and then loads
                it into a dataframe.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame or a path to the downloaded
            query results if `download_location` is set.

        Example: Querying for data
            This example shows how you may query for data in a table and print out the
            results.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import query

            syn = Synapse()
            syn.login()

            results = query(query="SELECT * FROM syn1234")
            print(results)
            ```
        """
        # Replaced at runtime
        return ""

    @staticmethod
    def query_part_mask(
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
            from synapseclient import Synapse
            from synapseclient.models import query_part_mask

            syn = Synapse()
            syn.login()

            QUERY_RESULTS = 0x1
            QUERY_COUNT = 0x2
            LAST_UPDATED_ON = 0x80

            # Combine the part mask values using bitwise OR
            part_mask = QUERY_RESULTS | QUERY_COUNT | LAST_UPDATED_ON

            result = query_part_mask(query="SELECT * FROM syn1234", part_mask=part_mask)
            print(result)
            ```
        """
        # Replaced at runtime
        return QueryResultBundle(result=None)


@async_to_sync
class QueryMixin(QueryMixinSynchronousProtocol):
    """Mixin class providing methods for querying data from a `Table`-like entity."""

    @staticmethod
    async def query_async(
        query: str,
        include_row_id_and_row_version: bool = True,
        convert_to_datetime: bool = False,
        download_location=None,
        quote_character='"',
        escape_character="\\",
        line_end=str(os.linesep),
        separator=",",
        header=True,
        *,
        synapse_client: Optional[Synapse] = None,
        **kwargs,
    ) -> DATA_FRAME_TYPE:
        """Query for data on a table stored in Synapse. The results will always be
        returned as a Pandas DataFrame unless you specify a `download_location` in which
        case the results will be downloaded to that location. There are a number of
        arguments that you may pass to this function depending on if you are getting
        the results back as a DataFrame or downloading the results to a file.

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
            convert_to_datetime: (DataFrame only) If set to True, will convert all
                Synapse DATE columns from UNIX timestamp integers into UTC datetime
                objects

            download_location: (CSV Only) If set to a path the results will be
                downloaded to that directory. The results will be downloaded as a CSV
                file. A path to the downloaded file will be returned instead of a
                DataFrame.

            quote_character: (CSV Only) The character to use to quote fields. The
                default is a double quote.

            escape_character: (CSV Only) The character to use to escape special
                characters. The default is a backslash.

            line_end: (CSV Only) The character to use to end a line. The default is
                the system's line separator.

            separator: (CSV Only) The character to use to separate fields. The default
                is a comma.

            header: (CSV Only) If set to True the first row will be used as the header
                row. The default is True.

            **kwargs: (DataFrame only) Additional keyword arguments to pass to
                pandas.read_csv. See
                <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>
                for complete list of supported arguments. This is exposed as
                internally the query downloads a CSV from Synapse and then loads
                it into a dataframe.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame or a path to the downloaded
            query results if `download_location` is set.

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
                quoteCharacter=quote_character,
                escapeCharacter=escape_character,
                lineEnd=line_end,
                separator=separator,
                header=header,
                downloadLocation=download_location,
            ),
        )
        if download_location:
            return results.filepath
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


@async_to_sync
class ViewSnapshotMixin:
    """A mixin providing methods for creating a snapshot of a `View`-like entity."""

    async def snapshot_async(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the `View`-like entity.
        Synapse handles snapshot creation differently for `Table`- and `View`-like
        entities. `View` snapshots are created using the asyncronous job API.


        Making a snapshot of a view allows you to create an immutable version of the
        view at the time of the snapshot. This is useful to create checkpoints in time
        that you may go back and reference, or use in a publication. Snapshots are
        immutable and cannot be changed. They may only be deleted.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot. If this is not a
                unique label an exception will be raised when you store this to Synapse.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Table
                and calling the `store()` method on the Table instance. Adding an
                activity to a snapshot of a table is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the table. If False the activity will not be
                associated with the new version of the table. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Creating a snapshot of a view with an activity
            Create a snapshot of a view and include the activity. The activity must
            have been stored in Synapse by using the `activity` attribute on the EntityView
            and calling the `store_async()` method on the EntityView instance. Adding an activity
            to a snapshot of a entityview is meant to capture the provenance of the data at
            the time of the snapshot.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            async def main():
                view = EntityView(id="syn4567")
                snapshot = await view.snapshot_async(label="Q1 2025", comment="Results collected in Lab A", include_activity=True, associate_activity_to_new_version=True)
                print(snapshot)

            asyncio.run(main())
            ```

        Example: Creating a snapshot of a view without an activity
            Create a snapshot of a view without including the activity. This is used in
            cases where we do not have any Provenance to associate with the snapshot and
            we do not want to persist any activity that may be present on the view to
            the new version of the view.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            async def main():
                view = EntityView(id="syn4567")
                snapshot = await view.snapshot_async(label="Q1 2025", comment="Results collected in Lab A", include_activity=False, associate_activity_to_new_version=False)
                print(snapshot)

            asyncio.run(main())
            ```
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.info(
            f"[{self.id}:{self.name}]: Creating a snapshot of the {type(self)}."
        )

        await self.get_async(include_activity=True, synapse_client=client)

        result = await TableUpdateTransaction(
            entity_id=self.id,
            changes=None,
            create_snapshot=True,
            snapshot_options=SnapshotRequest(
                comment=comment,
                label=label,
                activity=(
                    self.activity.id if self.activity and include_activity else None
                ),
            ),
        ).send_job_and_wait_async(synapse_client=client)

        if not self.version_number:
            # set to latest drafting version if the first snapshot was just created
            self.version_number = 2
        else:
            # increment the version number for each subsequent snapshot
            self.version_number += 1

        if associate_activity_to_new_version and self.activity:
            self._last_persistent_instance.activity = None
            await self.store_async(synapse_client=synapse_client)
        else:
            await self.get_async(include_activity=True, synapse_client=synapse_client)

        return result


@async_to_sync
class TableStoreRowMixin:
    """Mixin class providing methods for storing rows in a `Table`-like entity."""

    id: Optional[str] = None
    name: Optional[str] = None
    parent_id: Optional[str] = None
    columns: Optional[OrderedDict] = None
    activity: None = None
    _last_persistent_instance: None = None
    _columns_to_delete: None = None

    def _infer_columns_from_data(
        self,
        values: DATA_FRAME_TYPE,
        column_expansion_strategy: ColumnExpansionStrategy,
    ) -> None:
        """
        Infer the columns from the data that is being stored. This method is used
        when the `schema_storage_strategy` is set to `INFER_FROM_DATA`.

        Arguments:
            values: The data that is being stored. This data will be used to infer the
                columns that are created in Synapse.
            column_expansion_strategy: Determines how to automate the expansion of
                columns based on the data that is being stored. The options given
                allow cells with a limit on the length of content (Such as strings)
                to be expanded to a larger size if the data being stored exceeds the
                limit. A limit to list length is also enforced in Synapse by automatic
                expansion for lists is not yet supported through this interface.

        Returns:
            None, but the columns on the table will be updated to reflect the inferred
            columns from the data that is being stored.
        """
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
        insert_size_bytes: int = 900 * MB,
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
        read_csv_kwargs: Optional[Dict[str, Any]] = None,
        to_csv_kwargs: Optional[Dict[str, Any]] = None,
        job_timeout: int = 600,
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


        **Limitations:**

        - Synapse limits the number of rows that may be stored in a single request to
            a CSV file that is 1GB. If you are storing a CSV file that is larger than
            this limit the data will be chunked into smaller requests. This process is
            done by reading the file once to determine what the row and byte boundries
            are and calculating the MD5 hash of that portion, then reading the file
            again to send the data to Synapse. This process is done to ensure that the
            data is not corrupted during the upload process, in addition Synapse
            requires the MD5 hash of the data to be sent in the request along with the
            number of bytes that are being sent.
        - The limit of 1GB is also enforced when storing a dictionary or a DataFrame.
            The data will be converted to a CSV format using the `.to_csv()` pandas
            function. If you are storing more than a 1GB file it is recommended that
            you store the data as a CSV and use the file path to upload the data. This
            is due to the fact that the DataFrame chunking process is slower than
            reading portions of a file on disk and calculating the MD5 hash of that
            portion.

        The following is a Sequence Daigram that describes the process noted in the
        limitation above. It shows how the data is chunked into smaller requests when
        the data exceeds the limit of 1GB, and how portions of the data are read from
        the CSV file on disk while being uploaded to Synapse.

        ```mermaid
        sequenceDiagram
            participant User
            participant Table
            participant FileSystem
            participant Synapse

            User->>Table: store_rows(values)

            alt CSV size > 1GB
                Table->>Synapse: Apply schema changes before uploading
                note over Table, FileSystem: Read CSV twice
                Table->>FileSystem: Read entire CSV (First Pass)
                FileSystem-->>Table: Compute chunk sizes & MD5 hashes

                loop Read and Upload CSV chunks (Second Pass)
                    Table->>FileSystem: Read next chunk from CSV
                    FileSystem-->>Table: Return bytes
                    Table->>Synapse: Upload CSV chunk
                    Synapse-->>Table: Return `file_handle_id`
                    Table->>Synapse: Send 'TableUpdateTransaction' to append/update rows
                    Synapse-->>Table: Transaction result
                end
            else
                Table->>Synapse: Upload CSV without splitting & Any additional schema changes
                Synapse-->>Table: Return `file_handle_id`
                Table->>Synapse: Send `TableUpdateTransaction' to append/update rows
                Synapse-->>Table: Transaction result
            end

            Table-->>User: Upload complete
        ```

        The following is a Sequence Daigram that describes the process noted in the
        limitation above for DataFrames. It shows how the data is chunked into smaller
        requests when the data exceeds the limit of 1GB, and how portions of the data
        are read from the DataFrame while being uploaded to Synapse.

        ```mermaid
        sequenceDiagram
            participant User
            participant Table
            participant MemoryBuffer
            participant Synapse

            User->>Table: store_rows(DataFrame)

            loop For all rows in DataFrame in 100 row increments
                Table->>MemoryBuffer: Convert DataFrame rows to CSV in-memory
                MemoryBuffer-->>Table: Compute chunk sizes & MD5 hashes
            end


            alt Multiple chunks detected
                Table->>Synapse: Apply schema changes before uploading
            end

            loop For all chunks found in first loop
                loop for all parts in chunk byte boundry
                    Table->>MemoryBuffer: Read small (< 8MB) part of the chunk
                    MemoryBuffer-->>Table: Return bytes (with correct offset)
                    Table->>Synapse: Upload part
                    Synapse-->>Table: Upload response
                end
                Table->>Synapse: Complete upload
                Synapse-->>Table: Return `file_handle_id`
                Table->>Synapse: Send 'TableUpdateTransaction' to append/update rows
                Synapse-->>Table: Transaction result
            end

            Table-->>User: Upload complete
        ```

        Arguments:
            values: Supports storing data from the following sources:

                - A string holding the path to a CSV file. If the `schema_storage_strategy` is set to `None` the data will be uploaded as is. If `schema_storage_strategy` is set to `INFER_FROM_DATA` the data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_components.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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

            insert_size_bytes: The maximum size of data that will be stored to Synapse
                within a single transaction. The API have a limit of 1GB, but the
                default is set to 900 MB to allow for some overhead in the request. The
                implication of this limit is that when you are storing a CSV that is
                larger than this limit the data will be chunked into smaller requests
                by reading the file once to determine what the row and byte boundries
                are and calculating the MD5 hash of that portion, then reading the file
                again to send the data to Synapse. This process is done to ensure that
                the data is not corrupted during the upload process, in addition Synapse
                requires the MD5 hash of the data to be sent in the request along with
                the number of bytes that are being sent. This argument is also used
                when storing a dictionary or a DataFrame. The data will be converted to
                a CSV format using the `.to_csv()` pandas function. When storing data
                as a DataFrame the minimum that it will be chunked to is 100 rows of
                data, regardless of if the data is larger than the limit.

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

            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when inserting, and updating rows of data. Each individual
                request to Synapse will be sent as an independent job. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds

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
            self._infer_columns_from_data(
                values=values, column_expansion_strategy=column_expansion_strategy
            )

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
            with logging_redirect_tqdm(loggers=[client.logger]):
                await self._chunk_and_upload_csv(
                    path_to_csv=original_values,
                    insert_size_bytes=insert_size_bytes,
                    csv_table_descriptor=csv_table_descriptor,
                    schema_change_request=schema_change_request,
                    client=client,
                    additional_changes=additional_changes,
                    job_timeout=job_timeout,
                )
        elif isinstance(values, DataFrame):
            with logging_redirect_tqdm(loggers=[client.logger]):
                await self._chunk_and_upload_df(
                    df=values,
                    insert_size_bytes=insert_size_bytes,
                    csv_table_descriptor=csv_table_descriptor,
                    schema_change_request=schema_change_request,
                    client=client,
                    additional_changes=additional_changes,
                    job_timeout=job_timeout,
                    to_csv_kwargs=to_csv_kwargs,
                )

        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

    async def _send_update(
        self,
        client: Synapse,
        table_descriptor: CsvTableDescriptor,
        job_timeout: int,
        file_handle_id: str = None,
        changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
    ) -> None:
        """
        Construct the request to send to Synapse to update the table with the
        given file handle ID.

        This will also send the schema change request, or any additional changes
        that are passed in to the method.

        Arguments:
            table_descriptor: The descriptor for the CSV file that is being uploaded.
            job_timeout: The maximum amount of time to wait for a job to complete.
            file_handle_id: The file handle ID that is being uploaded to Synapse.
            changes: Additional changes to the table that should
                execute within the same transaction as appending or updating rows.
        """
        all_changes = []
        if changes:
            all_changes.extend(changes)

        if file_handle_id:
            upload_request = UploadToTableRequest(
                table_id=self.id,
                upload_file_handle_id=file_handle_id,
                update_etag=None,
            )
            if table_descriptor:
                upload_request.csv_table_descriptor = table_descriptor
            all_changes.append(upload_request)

        if all_changes:
            await TableUpdateTransaction(
                entity_id=self.id, changes=all_changes
            ).send_job_and_wait_async(synapse_client=client, timeout=job_timeout)

    async def _stream_and_update_from_disk(
        self,
        client: Synapse,
        encoded_header: bytes,
        size_of_chunk: int,
        path_to_csv: str,
        byte_chunk_offset: int,
        md5: str,
        csv_table_descriptor: CsvTableDescriptor,
        job_timeout: int,
        progress_bar: tqdm,
        wait_for_update_semaphore: asyncio.Semaphore,
        file_suffix: str,
    ) -> None:
        """
        Handle the process of reading in parts of the CSV we are going to be uploading
        into Synapse. Since the Synapse REST API has a limit of 1GB as the maximum
        size of a file that can be appended to a table, we must upload files that are
        larger than that in multiple requests.

        After each chunk is uploaded we will send a `TableUpdateTransaction` to Synapse
        to append the rows to the table.

        Arguments:
            client: The Synapse client that is being used to interact with the API.
            encoded_header: The header of the CSV file that is being uploaded.
            size_of_chunk: The size of the chunk that we are uploading to Synapse.
            path_to_csv: The path to the CSV file that is being uploaded to Synapse.
            byte_chunk_offset: The byte offset that we are starting to read from the
                csv file for the current chunk. This is used to skip any parts of the
                csv file that we have already uploaded.
            md5: The MD5 hash of the current chunk that is being uploaded.
            csv_table_descriptor: The descriptor for the CSV file that is being uploaded.
            job_timeout: The maximum amount of time to wait for a job to complete.
            progress_bar: The progress bar that is being used to show the progress of
                the upload.
            wait_for_update_semaphore: The semaphore that is being used to wait for the
                update to complete before moving on to the next chunk.
            file_suffix: The suffix that is being used to name the CSV file that is
                being uploaded. Used in the progress bar message and the file name.

        Returns:
            None
        """
        file_handle_id = await multipart_upload_partial_file_async(
            syn=client,
            bytes_to_prepend=encoded_header,
            content_type="text/csv",
            dest_file_name=f"chunked_csv_for_synapse_store_rows_{file_suffix}.csv",
            partial_file_size_bytes=size_of_chunk,
            path_to_original_file=path_to_csv,
            bytes_to_skip=byte_chunk_offset,
            md5=md5,
        )
        # We are using a semaphore here because large tables can take a very long time
        # for the update to complete. This will allow us to wait for the update to
        # complete before moving on to the next chunk.
        async with wait_for_update_semaphore:
            await self._send_update(
                client=client,
                table_descriptor=csv_table_descriptor,
                file_handle_id=file_handle_id,
                job_timeout=job_timeout,
            )
            progress_bar.update(size_of_chunk)

    async def _stream_and_update_from_df(
        self,
        client: Synapse,
        df: DATA_FRAME_TYPE,
        header: bytes,
        line_start: int,
        line_end: int,
        size_of_chunk: int,
        byte_chunk_offset: int,
        md5: str,
        csv_table_descriptor: CsvTableDescriptor,
        job_timeout: int,
        progress_bar: tqdm,
        wait_for_update_semaphore: asyncio.Semaphore,
        file_suffix: str,
        changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
    ) -> None:
        """
        Organize the process of reading in and uploading parts of the DataFrame we are
        going to be uploading into Synapse. Once the portion of the DataFrame is read
        in we will send a `TableUpdateTransaction` to Synapse to append the rows to the
        table.

        Arguments:
            client: The Synapse client that is being used to interact with the API.
            df: The DataFrame that we are chunking up and is being uploaded to Synapse.
            header: The header of the CSV file that is being uploaded.
            line_start: The line number that we are starting to read from the DataFrame
                for the current chunk.
            line_end: The line number that we are ending to read from the DataFrame
                for the current chunk.
            size_of_chunk: The size of the chunk that we are uploading to Synapse.
            byte_chunk_offset: The byte offset that we are starting to read from the
                DataFrame for the current chunk. This is used to skip any parts of the
                DataFrame that we have already uploaded.
            md5: The MD5 hash of the current chunk that is being uploaded.
            csv_table_descriptor: The descriptor for the CSV file that is being uploaded.
            job_timeout: The maximum amount of time to wait for a job to complete.
            progress_bar: The progress bar that is being used to show the progress of
                the upload.
            wait_for_update_semaphore: The semaphore that is being used to wait for the
                update to complete before moving on to the next chunk.
            file_suffix: The suffix that is being used to name the CSV file that is
                being uploaded.
            changes: Additional changes to the table that should
                execute within this transaction.
        """
        file_handle_id = await multipart_upload_dataframe_async(
            syn=client,
            df=df,
            content_type="text/csv",
            dest_file_name=f"chunked_csv_for_synapse_store_rows_{file_suffix}.csv",
            partial_file_size_bytes=size_of_chunk,
            bytes_to_skip=byte_chunk_offset,
            md5=md5,
            line_start=line_start,
            line_end=line_end,
            bytes_to_prepend=header,
        )
        # We are using a semaphore here because large tables can take a very long time
        # for the update to complete. This will allow us to wait for the update to
        # complete before moving on to the next chunk.
        async with wait_for_update_semaphore:
            await self._send_update(
                client=client,
                table_descriptor=csv_table_descriptor,
                file_handle_id=file_handle_id,
                job_timeout=job_timeout,
                changes=changes,
            )
            progress_bar.update(size_of_chunk)

    async def _chunk_and_upload_csv(
        self,
        path_to_csv: str,
        insert_size_bytes: int,
        csv_table_descriptor: CsvTableDescriptor,
        schema_change_request: TableSchemaChangeRequest,
        client: Synapse,
        job_timeout: int,
        additional_changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
    ) -> None:
        """
        Determines if the file we are appending to the table is larger than the
        maximum size that Synapse allows for a single request. If the file is larger
        than the maximum size we will chunk the file into smaller requests and upload
        them to Synapse. If the file is smaller than the maximum size we will upload
        the file to Synapse as is.

        Arguments:
            path_to_csv: The path to the CSV file that is being uploaded to Synapse.
            insert_size_bytes: The maximum size of data that will be stored to Synapse
                within a single transaction. The API have a limit of 1GB.
            csv_table_descriptor: The descriptor for the CSV file that is being uploaded.
            schema_change_request: The schema change request that will be sent to
                Synapse to update the table.
            client: The Synapse client that is being used to interact with the API.
            job_timeout: The maximum amount of time to wait for a job to complete.
            additional_changes: Additional changes to the table that should execute
                within this transaction.
        """
        if (file_size := os.path.getsize(path_to_csv)) > insert_size_bytes:
            # Apply schema changes before breaking apart and uploading the file
            changes = []
            if schema_change_request:
                changes.append(schema_change_request)
            if additional_changes:
                changes.extend(additional_changes)

            await self._send_update(
                client=client,
                table_descriptor=csv_table_descriptor,
                changes=changes,
                job_timeout=job_timeout,
            )

            progress_bar = tqdm(
                total=file_size,
                desc="Splitting CSV and uploading chunks",
                unit_scale=True,
                smoothing=0,
                unit="B",
                leave=None,
            )
            # The original file is read twice, the reason is that on the first pass we
            # are calculating the size of the chunks that we will be uploading and the
            # MD5 hash of the file. On the second pass we are reading in the chunks
            # and uploading them to Synapse.
            with open(file=path_to_csv, mode="rb") as f:
                header_line = f.readline()
                size_of_header = len(header_line)
                file_size = size_of_header
                md5_hashlib = hashlib.new("md5", usedforsecurity=False)  # nosec
                md5_hashlib.update(header_line)
                chunks_to_upload = []
                size_of_chunk = 0
                previous_chunk_byte_offset = size_of_header
                while chunk := f.readlines(8 * MB):
                    for line in chunk:
                        md5_hashlib.update(line)
                        size_of_chunk += len(line)
                        file_size += size_of_chunk
                        if size_of_chunk >= insert_size_bytes:
                            chunks_to_upload.append(
                                (
                                    previous_chunk_byte_offset,
                                    size_of_chunk,
                                    md5_hashlib.hexdigest(),
                                )
                            )
                            previous_chunk_byte_offset += size_of_chunk
                            size_of_chunk = 0
                            md5_hashlib = hashlib.new(
                                "md5", usedforsecurity=False
                            )  # nosec
                            md5_hashlib.update(header_line)
                if size_of_chunk:
                    chunks_to_upload.append(
                        (
                            previous_chunk_byte_offset,
                            size_of_chunk,
                            md5_hashlib.hexdigest(),
                        )
                    )

                update_tasks = []
                wait_for_update_semaphore = asyncio.Semaphore(value=1)
                part = 0
                for byte_chunk_offset, size_of_chunk, md5 in chunks_to_upload:
                    update_tasks.append(
                        asyncio.create_task(
                            self._stream_and_update_from_disk(
                                client=client,
                                encoded_header=header_line,
                                size_of_chunk=size_of_chunk,
                                path_to_csv=path_to_csv,
                                byte_chunk_offset=byte_chunk_offset,
                                md5=md5,
                                csv_table_descriptor=csv_table_descriptor,
                                job_timeout=job_timeout,
                                progress_bar=progress_bar,
                                wait_for_update_semaphore=wait_for_update_semaphore,
                                file_suffix=f"{part}",
                            )
                        )
                    )
                    part += 1

                client.logger.info(
                    f"[{self.id}:{self.name}]: Found {len(chunks_to_upload)} chunks to upload into table"
                )
                await asyncio.gather(*update_tasks)

            progress_bar.update(progress_bar.total - progress_bar.n)
            progress_bar.refresh()
            progress_bar.close()
        else:
            file_handle_id = await multipart_upload_file_async(
                syn=client, file_path=path_to_csv, content_type="text/csv"
            )

            changes = []
            if schema_change_request:
                changes.append(schema_change_request)
            if additional_changes:
                changes.extend(additional_changes)

            await self._send_update(
                client=client,
                table_descriptor=csv_table_descriptor,
                file_handle_id=file_handle_id,
                changes=changes,
                job_timeout=job_timeout,
            )

    async def _chunk_and_upload_df(
        self,
        df: Union[str, DATA_FRAME_TYPE],
        insert_size_bytes: int,
        csv_table_descriptor: CsvTableDescriptor,
        schema_change_request: TableSchemaChangeRequest,
        client: Synapse,
        job_timeout: int,
        additional_changes: List[
            Union[
                "TableSchemaChangeRequest",
                "UploadToTableRequest",
                "AppendableRowSetRequest",
            ]
        ] = None,
        to_csv_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Determines the chunks that need to be used to upload the DataFrame to Synapse.
        The DataFrame will be chunked into smaller requests when the data exceeds the
        limit of 1GB. The data will be read in twice, the first pass will determine
        the size of the chunks that will be uploaded and the MD5 hash of the file. The
        second pass will read in the chunks and upload them to Synapse.

        Arguments:
            df: The DataFrame that we are chunking up and is being uploaded to Synapse.
            insert_size_bytes: The maximum size of data that will be stored to Synapse
                within a single transaction. The API have a limit of 1GB.
            csv_table_descriptor: The descriptor for the CSV file that is being uploaded.
            schema_change_request: The schema change request that will be sent to
                Synapse to update the table.
            client: The Synapse client that is being used to interact with the API.
            job_timeout: The maximum amount of time to wait for a job to complete.
            additional_changes: Additional changes to the table that should execute
                within this transaction. When there are multiple chunks to upload
                the changes will be applied right away to prevent going over service
                limits.
            to_csv_kwargs: Additional arguments to pass to the `pd.DataFrame.to_csv`
                function when writing the data to a CSV file.
        """
        # Loop over the rows of the DF to determine the size/boundries we'll be uploading

        chunks_to_upload = []
        size_of_chunk = 0
        buffer = BytesIO()
        total_df_bytes = 0
        header_line = None
        md5_hashlib = hashlib.new("md5", usedforsecurity=False)  # nosec
        line_start_index_for_chunk = 0
        line_end_index_for_chunk = 0
        for start in range(0, len(df), 100):
            end = start + 100
            line_end_index_for_chunk = end
            buffer.seek(0)
            buffer.truncate(0)
            df.iloc[start:end].to_csv(
                buffer,
                header=(start == 0),
                index=False,
                float_format="%.12g",
                **(to_csv_kwargs or {}),
            )
            total_df_bytes += buffer.tell()
            size_of_chunk += buffer.tell()

            if start == 0:
                buffer.seek(0)
                header_line = buffer.readline()
            md5_hashlib.update(buffer.getvalue())

            if size_of_chunk >= insert_size_bytes:
                chunks_to_upload.append(
                    (
                        size_of_chunk,
                        md5_hashlib.hexdigest(),
                        line_start_index_for_chunk,
                        line_end_index_for_chunk,
                    )
                )
                size_of_chunk = 0
                line_start_index_for_chunk = line_end_index_for_chunk
                md5_hashlib = hashlib.new("md5", usedforsecurity=False)  # nosec
        if size_of_chunk > 0:
            chunks_to_upload.append(
                (
                    size_of_chunk,
                    md5_hashlib.hexdigest(),
                    line_start_index_for_chunk,
                    line_end_index_for_chunk,
                )
            )

        client.logger.info(
            f"[{self.id}:{self.name}]: Found {len(chunks_to_upload)} chunks to upload into table"
        )
        progress_bar = tqdm(
            total=total_df_bytes,
            desc=(
                "Splitting DataFrame and uploading chunks"
                if len(chunks_to_upload) > 1
                else "Uploading DataFrame"
            ),
            unit_scale=True,
            smoothing=0,
            unit="B",
            leave=None,
        )

        changes = []
        if schema_change_request:
            changes.append(schema_change_request)
        if additional_changes:
            changes.extend(additional_changes)

        # Apply changes right away when there are multiple chunks. This is to prevent
        # going over service limits depending on the additiona changes.
        if len(chunks_to_upload) > 1:
            await self._send_update(
                client=client,
                table_descriptor=csv_table_descriptor,
                changes=changes,
                job_timeout=job_timeout,
            )
            changes = None

        update_tasks = []
        wait_for_update_semaphore = asyncio.Semaphore(value=1)
        part = 0
        for (
            size_of_chunk,
            md5,
            line_start,
            line_end,
        ) in chunks_to_upload:
            update_tasks.append(
                asyncio.create_task(
                    self._stream_and_update_from_df(
                        client=client,
                        size_of_chunk=size_of_chunk,
                        byte_chunk_offset=0,
                        md5=md5,
                        csv_table_descriptor=csv_table_descriptor,
                        job_timeout=job_timeout,
                        progress_bar=progress_bar,
                        wait_for_update_semaphore=wait_for_update_semaphore,
                        line_start=line_start,
                        line_end=line_end,
                        df=df,
                        header=header_line,
                        changes=changes,
                        file_suffix=f"{part}",
                    )
                )
            )
            part += 1

        await asyncio.gather(*update_tasks)
        progress_bar.update(progress_bar.total - progress_bar.n)
        progress_bar.refresh()
        progress_bar.close()


@async_to_sync
class TableDeleteRowMixin:
    """Mixin class providing methods for deleting rows from a `Table`-like entity."""

    async def delete_rows_async(
        self,
        query: str,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
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
            job_timeout: The amount of time to wait for table updates to complete
                before a `SynapseTimeoutError` is thrown. The default is 600 seconds.
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
        results_from_query = await self.query_async(query=query, synapse_client=client)
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
        ).send_job_and_wait_async(synapse_client=client, timeout=job_timeout)

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


def _row_labels_from_id_and_version(rows: List[Tuple[str, str]]) -> List[str]:
    """
    Create a list of row labels from a list of tuples containing row IDs and versions.

    Arguments:
        rows: A list of tuples containing row IDs and versions.

    Returns:
        A list of row labels.
    """
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
) -> DATA_FRAME_TYPE:
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
    try:
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
    except Exception:
        return cell
