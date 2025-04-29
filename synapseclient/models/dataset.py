import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api.table_services import ViewEntityType, ViewTypeMask
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import MB, delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
    ViewBase,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
)
from synapseclient.models.table_components import (
    DATA_FRAME_TYPE,
    Column,
    TableUpdateTransaction,
)

if TYPE_CHECKING:
    from synapseclient.models import File, Folder


@dataclass
class EntityRef:
    """
    Represents a reference to the id and version of an entity to be used in `Dataset` and
    `DatasetCollection` objects.

    Attributes:
        id: The Synapse ID of the entity.
        version: Indicates a specific version of the entity.
    """

    id: str
    """The Synapse ID of the entity."""

    version: int
    """Indicates a specific version of the entity."""

    def to_synapse_request(self):
        """Converts the attributes of an EntityRef instance to a
        request expected of the Synapse REST API."""

        return {
            "entityId": self.id,
            "versionNumber": self.version,
        }


class DatasetSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for Dataset operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store information about a Dataset including the columns and annotations.
        Storing an update to the Datatset items will alter the rows present in the Dataset.

        Datasets have default columns that are managed by Synapse. The default behavior of
        this function is to include these default columns in the dataset when it is stored.
        This means that with the default behavior, any columns that you have added to your
        Dataset will be overwritten by the default columns if they have the same name. To
        avoid this behavior, set the `include_default_columns` attribute to `False`.

        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the construction of your Dataset instance, ie.
            `Dataset(columns=[Column(name="foo")])`, they will be added at the beginning
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
            The Dataset instance stored in synapse.

        Example: Create a new dataset from a list of EntityRefs by storing it.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, EntityRef

            syn = Synapse()
            syn.login()

            my_entity_refs = [EntityRef(id="syn1234"), EntityRef(id="syn1235"), EntityRef(id="syn1236")]
            my_dataset = Dataset(parent_id="syn987", name="my-new-dataset", items=my_entity_refs)
            my_dataset.store()
            ```
        """
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the Dataset from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the Dataset
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Dataset instance stored in synapse.

        Example: Getting metadata about a Dataset using id
            Get a Dataset by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            dataset = Dataset(id="syn4567").get(include_activity=True)
            print(dataset)

            # Columns are retrieved by default
            print(dataset.columns)
            print(dataset.activity)
            ```

        Example: Getting metadata about a Dataset using name and parent_id
            Get a Dataset by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            dataset = Dataset(name="my_dataset", parent_id="syn1234").get(include_columns=True, include_activity=True)
            print(dataset)
            print(dataset.columns)
            print(dataset.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the dataset from synapse. This is not version specific. If you'd like
        to delete a specific version of the dataset you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a dataset
            Deleting a dataset is only supported by the ID of the dataset.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            Dataset(id="syn4567").delete()
            ```
        """
        return None

    def update_rows(
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
        """Update the values of rows in the dataset. This method can only
        be used to update values in custom columns. Default columns cannot be updated, but
        may be used as primary keys.

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
                after making changes to the data.

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.


        Example: Update custom column values in a dataset.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()

            # my_annotation must already exist in the dataset as a custom column
            modified_data = pd.DataFrame(
                {"id": ["syn1234"], "my_annotation": ["good data"]}
            )
            my_dataset.update_rows(values=modified_data, primary_keys=["id"], dry_run=False)
            ```
        """
        return None

    def snapshot(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the dataset. A snapshot is a saved, read-only version of the dataset
        at the time it was created. Dataset snapshots are created using the asyncronous job API.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Dataset
                and calling the `store()` method on the Dataset instance. Adding an
                activity to a snapshot of a dataset is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the dataset. If False the activity will not be
                associated with the new version of the dataset. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Save a snapshot of a dataset.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.snapshot(comment="My first snapshot", label="My first snapshot")
            ```
        """
        return TableUpdateTransaction


@dataclass
@async_to_sync
class Dataset(
    DatasetSynchronousProtocol,
    AccessControllable,
    ViewBase,
    ViewStoreMixin,
    DeleteMixin,
    ColumnMixin,
    GetMixin,
    QueryMixin,
    ViewUpdateMixin,
    ViewSnapshotMixin,
):
    """A `Dataset` object represents the metadata of a Synapse Dataset.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Dataset.html>

    Attributes:
        id: The unique immutable ID for this dataset. A new ID will be generated for new
            Datasets. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this dataset. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of the dataset. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: The date this dataset was created.
        modified_on: The date this dataset was last modified.
            In YYYY-MM-DD-Thh:mm:ss.sssZ format
        created_by: The ID of the user that created this dataset.
        modified_by: The ID of the user that last modified this dataset.
        parent_id: The ID of the Entity that is the parent of this dataset.
        columns: The columns of this dataset. This is an ordered dictionary where the key is the
            name of the column and the value is the Column object. When creating a new instance
            of a Dataset object you may pass any of the following types as the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the Column object
            - An OrderedDict where the key is the name of the column and the value is the Column object

            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your dataset class instance.

            You may modify the attributes of the Column object to change the column
            type, name, or other attributes. For example, suppose you'd like to change a
            column from a INTEGER to a DOUBLE. You can do so by changing the column type
            attribute of the Column object. The next time you store the dataset the column
            will be updated in Synapse with the new type.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Synapse
            from synapseclient.models import Column, ColumnType

            syn = Synapse()
            syn.login()

            dataset = Dataset(id="syn1234").get()
            dataset.columns["my_column"].column_type = ColumnType.DOUBLE
            dataset.store()
            ```

            Note that the keys in this dictionary should match the column names as they are in
            Synapse. However, know that the name attribute of the Column object is used for
            all interactions with the Synapse API. The OrderedDict key is purely for the usage
            of this interface. For example, if you wish to rename a column you may do so by
            changing the name attribute of the Column object. The key in the OrderedDict does
            not need to be changed. The next time you store the dataset the column will be updated
            in Synapse with the new name and the key in the OrderedDict will be updated.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this dataset.
        version_comment: The version comment for this dataset.
        is_latest_version: If this is the latest version of the object.
        is_search_enabled: When creating or updating a dataset or view specifies if full
            text search should be enabled. Note that enabling full text search might
            slow down the indexing of the dataset or view.
        items: The flat list of file entity references that define this dataset. This is effectively
        a list of the rows that are in/will be in the dataset after it is stored. The only way to add
        or remove rows is to add or remove items from this list.
        size: The cumulative size, in bytes, of all items (files) in the dataset. This is
            only correct after the dataset has been stored or newly read from Synapse.
        checksum: The checksum is computed over a sorted concatenation of the checksums
            of all items in the dataset. This is only correct after the dataset has been
            stored or newly read from Synapse.
        count: The number of items/files in the dataset. This is only correct after the
            dataset has been stored or newly read from Synapse.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analogous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance.
        annotations: Additional metadata associated with the dataset. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list.
        include_default_columns: When creating a dataset or view, specifies if default
            columns should be included. Default columns are columns that are
            automatically added to the dataset or view. These columns are managed by
            Synapse and cannot be modified. If you attempt to create a column with the
            same name as a default column, you will receive a warning when you store the
            dataset.

            **`include_default_columns` is only used if this is the first time that the
            view is being stored.** If you are updating an existing view this attribute
            will be ignored. If you want to add all default columns back to your view
            then you may use this code snippet to accomplish this:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            async def main():
                view = await Dataset(id="syn1234").get_async()
                await view._append_default_columns()
                await view.store_async()

            asyncio.run(main())
            ```

            The column you are overriding will not behave the same as a default column.
            For example, suppose you create a column called `id` on a Dataset. When
            using a default column, the `id` stores the Synapse ID of each of the
            entities included in the scope of the view. If you override the `id` column
            with a new column, the `id` column will no longer store the Synapse ID of
            the entities in the view. Instead, it will store the values you provide when
            you store the dataset. It will be stored as an annotation on the entity for
            the row you are modifying.

    Example: Create a new dataset from a list of EntityRefs.
        Dataset items consist of references to Synapse Files using an Entity Reference.
        If you are adding items to a Dataset directly, you must provide them in the form of
        an `EntityRef` class instance.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset, EntityRef

        syn = Synapse()
        syn.login()

        my_entity_refs = [EntityRef(id="syn1234"), EntityRef(id="syn1235"), EntityRef(id="syn1236")]
        my_dataset = Dataset(parent_id="syn987", name="my-new-dataset", items=my_entity_refs)
        my_dataset.store()
        ```

    Example: Add entities to an existing dataset.
        Using `add_item`, you can add Synapse entities that are Files, Folders, or EntityRefs that point to a Synapse entity.
        If the entity is a Folder (or an EntityRef that points to a folder), all of the child Files
        within the Folder will be added to the Dataset recursively.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset, File, Folder, EntityRef

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()

        # Add a file to the dataset
        my_dataset.add_item(File(id="syn1235"))

        # Add a folder to the dataset
        # All child files are recursively added to the dataset
        my_dataset.add_item(Folder(id="syn1236"))

        # Add an entity reference to the dataset
        my_dataset.add_item(EntityRef(id="syn1237", version=1))

        my_dataset.store()
        ```

    Example: Remove entities from a dataset.
        &nbsp;


        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset, File, Folder, EntityRef

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()

        # Remove a file from the dataset
        my_dataset.remove_item(File(id="syn1235"))

        # Remove a folder from the dataset
        # All child files are recursively removed from the dataset
        my_dataset.remove_item(Folder(id="syn1236"))

        # Remove an entity reference from the dataset
        my_dataset.remove_item(EntityRef(id="syn1237", version=1))

        my_dataset.store()
        ```

    Example: Query data from a dataset.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()
        row = my_dataset.query(query="SELECT * FROM syn1234 WHERE id = 'syn1235'")
        print(row)
        ```

    Example: Add a custom column to a dataset.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset, Column, ColumnType

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()
        my_dataset.add_column(Column(name="my_annotation", column_type=ColumnType.STRING))
        my_dataset.store()
        ```

    Example: Update custom column values in a dataset.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()
        # my_annotation must already exist in the dataset as a custom column
        modified_data = pd.DataFrame(
            {"id": ["syn1234"], "my_annotation": ["good data"]}
        )
        my_dataset.update_rows(values=modified_data, primary_keys=["id"], dry_run=False)
        ```

    Example: Save a snapshot of a dataset.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset

        syn = Synapse()
        syn.login()

        my_dataset = Dataset(id="syn1234").get()
        my_dataset.snapshot(comment="My first snapshot", label="My first snapshot")
        ```

    Example: Deleting a dataset
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import Dataset

        syn = Synapse()
        syn.login()

        Dataset(id="syn4567").delete()
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this dataset. A new ID will be generated for new
    datasets. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this dataset. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
    apostrophes, and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """The date this dataset was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this dataset was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ format"""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this dataset."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this dataset."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this dataset."""

    version_number: Optional[int] = field(default=None, compare=False)
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this dataset."""

    version_comment: Optional[str] = None
    """The version comment for this dataset."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """If this is the latest version of the object."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a dataset or view specifies if full text search
    should be enabled. Note that enabling full text search might slow down the
    indexing of the dataset or view."""

    items: Optional[List[EntityRef]] = field(default_factory=list, compare=False)
    """The flat list of file entity references that define this dataset."""

    size: Optional[int] = field(default=None, compare=False)
    """The cumulative size, in bytes, of all items(files) in the dataset.

    This is only correct after the dataset has been stored or newly read from Synapse.
    """

    checksum: Optional[str] = field(default=None, compare=False)
    """The checksum is computed over a sorted concatenation of the checksums of all
    items in the dataset.

    This is only correct after the dataset has been stored or newly read from Synapse.
    """

    count: Optional[int] = field(default=None, compare=False)
    """The number of items/files in the dataset.

    This is only correct after the dataset has been stored or newly read from Synapse.
    """

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict, compare=False)
    """
    The columns of this dataset. This is an ordered dictionary where the key is the
    name of the column and the value is the Column object. When creating a new instance
    of a Dataset object you may pass any of the following types as the `columns` argument:

    - A list of Column objects
    - A dictionary where the key is the name of the column and the value is the Column object
    - An OrderedDict where the key is the name of the column and the value is the Column object

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your dataset class instance.

    You may modify the attributes of the Column object to change the column
    type, name, or other attributes. For example, suppose you'd like to change a
    column from a INTEGER to a DOUBLE. You can do so by changing the column type
    attribute of the Column object. The next time you store the dataset the column
    will be updated in Synapse with the new type.

    ```python
    from synapseclient import Synapse
    from synapseclient.models import Table, Column, ColumnType

    syn = Synapse()
    syn.login()

    dataset = Dataset(id="syn1234").get()
    dataset.columns["my_column"].column_type = ColumnType.DOUBLE
    dataset.store()
    ```

    Note that the keys in this dictionary should match the column names as they are in
    Synapse. However, know that the name attribute of the Column object is used for
    all interactions with the Synapse API. The OrderedDict key is purely for the usage
    of this interface. For example, if you wish to rename a column you may do so by
    changing the name attribute of the Column object. The key in the OrderedDict does
    not need to be changed. The next time you store the dataset the column will be updated
    in Synapse with the new name and the key in the OrderedDict will be updated.
    """

    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)
    """
    Columns to delete when the dataset is stored. The key in this dict is the ID of the
    column to delete. The value is the Column object that represents the column to
    delete.
    """

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analogous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

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
    """Additional metadata associated with the dataset. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`"""

    _last_persistent_instance: Optional["Dataset"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    view_entity_type: ViewEntityType = ViewEntityType.DATASET
    """The API model string for the type of view. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewEntityType` enum.
    """

    view_type_mask: ViewTypeMask = ViewTypeMask.DATASET
    """The Bit Mask representing Dataset type.
    As defined in the Synapse REST API:
    <https://rest-docs.synapse.org/rest/GET/column/tableview/defaults.html>"""

    def __post_init__(self):
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance
            or self._last_persistent_instance != self
            or (not self._last_persistent_instance.items and self.items)
            or self._last_persistent_instance.items != self.items
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
        self._last_persistent_instance.items = (
            [dataclasses.replace(item) for item in self.items] if self.items else []
        )

    def fill_from_dict(self, entity, set_annotations: bool = True) -> "Self":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            synapse_table: The data coming from the Synapse API

        Returns:
            The Dataset object instance.
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
        self.size = entity.get("size", None)
        self.checksum = entity.get("checksum", None)
        self.count = entity.get("count", None)
        self.items = [
            EntityRef(id=item["entityId"], version=item["versionNumber"])
            for item in entity.get("items", [])
        ]

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
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": concrete_types.DATASET_ENTITY,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "columnIds": (
                [
                    column.id
                    for column in self._last_persistent_instance.columns.values()
                ]
                if self._last_persistent_instance
                and self._last_persistent_instance.columns
                else []
            ),
            "isSearchEnabled": self.is_search_enabled,
            "items": (
                [item.to_synapse_request() for item in self.items] if self.items else []
            ),
            "size": self.size,
            "checksum": self.checksum,
            "count": self.count,
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    def _append_entity_ref(self, entity_ref: EntityRef) -> None:
        """Helper function to add an EntityRef to the items list of the dataset.
        Will not add duplicates.

        Arguments:
            entity_ref: The EntityRef to add to the items list of the dataset.
        """
        if entity_ref not in self.items:
            self.items.append(entity_ref)

    def add_item(
        self,
        item: Union[EntityRef, "File", "Folder"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """Adds an item in the form of an EntityRef to the dataset.
        For Folders, children are added recursively. Effect is not seen
        until the dataset is stored.

        Arguments:
            item: Entity to add to the dataset. Must be an EntityRef, File, or Folder.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the item is not an EntityRef, File, or Folder

        Example: Add a file to a dataset.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, File

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.add_item(File(id="syn1235"))
            my_dataset.store()
            ```

        Example: Add a folder to a dataset.
            All child files are recursively added to the dataset.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, Folder

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.add_item(Folder(id="syn1236"))
            my_dataset.store()
            ```

        Example: Add an entity reference to a dataset.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, EntityRef

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.add_item(EntityRef(id="syn1237", version=1))
            my_dataset.store()
            ```
        """
        from synapseclient.models import File, Folder

        client = Synapse.get_client(synapse_client=synapse_client)

        if isinstance(item, EntityRef):
            self._append_entity_ref(entity_ref=item)
        elif isinstance(item, File):
            if not item.version_number:
                item = File(
                    id=item.id, version_number=item.version_number, download_file=False
                ).get()
            self._append_entity_ref(
                entity_ref=EntityRef(id=item.id, version=item.version_number)
            )
        elif isinstance(item, Folder):
            children = item._retrieve_children(follow_link=True)
            for child in children:
                if child["type"] == concrete_types.FILE_ENTITY:
                    self._append_entity_ref(
                        entity_ref=EntityRef(
                            id=child["id"], version=child["versionNumber"]
                        )
                    )
                else:
                    self.add_item(item=Folder(id=child["id"]), synapse_client=client)
        else:
            raise ValueError(
                f"item must be one of EntityRef, File, or Folder. {item} is a {type(item)}"
            )

    def _remove_entity_ref(self, entity_ref: EntityRef) -> None:
        """Helper function to remove an EntityRef from the items list of the dataset.

        Arguments:
            entity_ref: The EntityRef to remove from the items list of the dataset.
        """
        if entity_ref not in self.items:
            raise ValueError(f"Entity {entity_ref.id} not found in items list")
        self.items.remove(entity_ref)

    def remove_item(
        self,
        item: Union[EntityRef, "File", "Folder"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Removes an item from the dataset. For Folders, all
        children of the folder are removed recursively.
        Effect is not seen until the dataset is stored.

        Arguments:
            item: The Synapse ID or Entity to remove from the dataset
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If the item is not a valid type

        Example: Remove a file from a dataset.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, File

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.remove_item(File(id="syn1235"))
            my_dataset.store()
            ```

        Example: Remove a folder from a dataset.
            All child files are recursively removed from the dataset.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, Folder

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.remove_item(Folder(id="syn1236"))
            my_dataset.store()
            ```

        Example: Remove an entity reference from a dataset.
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset, EntityRef

            syn = Synapse()
            syn.login()

            my_dataset = Dataset(id="syn1234").get()
            my_dataset.remove_item(EntityRef(id="syn1237", version=1))
            my_dataset.store()
            ```
        """
        from synapseclient.models import File, Folder

        client = Synapse.get_client(synapse_client=synapse_client)

        if isinstance(item, EntityRef):
            self._remove_entity_ref(item)
        elif isinstance(item, File):
            if not item.version_number:
                item = File(
                    id=item.id, version_number=item.version_number, download_file=False
                ).get()
            self._remove_entity_ref(EntityRef(id=item.id, version=item.version_number))
        elif isinstance(item, Folder):
            children = item._retrieve_children(follow_link=True)
            for child in children:
                if child["type"] == concrete_types.FILE_ENTITY:
                    self._remove_entity_ref(
                        EntityRef(id=child["id"], version=child["versionNumber"])
                    )
                else:
                    self.remove_item(item=Folder(id=child["id"]), synapse_client=client)
        else:
            raise ValueError(
                f"item must be one of str, EntityRef, File, or Folder, {item} is a {type(item)}"
            )

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store information about a Dataset including the columns and annotations.
        Storing an update to the Dataset items will alter the rows present in the Dataset.
        Datasets have default columns that are managed by Synapse. The default behavior of
        this function is to include these default columns in the dataset when it is stored.
        This means that with the default behavior, any columns that you have added to your
        Dataset will be overwritten by the default columns if they have the same name. To
        avoid this behavior, set the `include_default_columns` attribute to `False`.

        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the construction of your Dataset instance, ie.
            `Dataset(columns=[Column(name="foo")])`, they will be added at the beginning
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
            The Dataset instance stored in synapse.

        Example: Create a new dataset from a list of EntityRefs by storing it.
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset, EntityRef

            syn = Synapse()
            syn.login()

            async def main():
                my_entity_refs = [EntityRef(id="syn1234"), EntityRef(id="syn1235"), EntityRef(id="syn1236")]
                my_dataset = Dataset(parent_id="syn987", name="my-new-dataset", items=my_entity_refs)
                await my_dataset.store_async()

            asyncio.run(main())
            ```
        """
        return await super().store_async(
            dry_run=dry_run,
            job_timeout=job_timeout,
            synapse_client=synapse_client,
        )

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the Dataset from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the Dataset
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Dataset instance stored in synapse.

        Example: Getting metadata about a Dataset using id
            Get a Dataset by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get_async` call, then you'll make the changes, and finally call the
            `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            async def main():
                dataset = await Dataset(id="syn4567").get_async(include_activity=True)
                print(dataset)

                # Columns are retrieved by default
                print(dataset.columns)
                print(dataset.activity)

            asyncio.run(main())
            ```

        Example: Getting metadata about a Dataset using name and parent_id
            Get a Dataset by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get_async` call, then you'll make the changes,
            and finally call the `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            async def main():
                dataset = await Dataset(
                    name="my_dataset",
                    parent_id="syn1234"
                ).get_async(
                    include_columns=True,
                    include_activity=True
                )
                print(dataset)
                print(dataset.columns)
                print(dataset.activity)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the dataset from synapse. This is not version specific. If you'd like
        to delete a specific version of the dataset you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a dataset
            Deleting a dataset is only supported by the ID of the dataset.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            async def main():
                await Dataset(id="syn4567").delete_async()

            asyncio.run(main())
            ```
        """
        await super().delete_async(synapse_client=synapse_client)

    async def update_rows_async(
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
        """Update the values of rows in the dataset. This method can only
        be used to update values in custom columns. Default columns cannot be updated, but
        may be used as primary keys.

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
                after making changes to the data.

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.


        Example: Update custom column values in a dataset.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset
            import pandas as pd

            syn = Synapse()
            syn.login()

            async def main():
                my_dataset = await Dataset(id="syn1234").get_async()

                # my_annotation must already exist in the dataset as a custom column
                modified_data = pd.DataFrame(
                    {"id": ["syn1234"], "my_annotation": ["good data"]}
                )
                await my_dataset.update_rows_async(values=modified_data, primary_keys=["id"], dry_run=False)

            asyncio.run(main())
            ```
        """
        await super().update_rows_async(
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

    async def snapshot_async(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the dataset. A snapshot is a saved, read-only version of the dataset
        at the time it was created. Dataset snapshots are created using the asyncronous job API.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Dataset
                and calling the `store()` method on the Dataset instance. Adding an
                activity to a snapshot of a dataset is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the dataset. If False the activity will not be
                associated with the new version of the dataset. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Save a snapshot of a dataset.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            async def main():
                my_dataset = await Dataset(id="syn1234").get_async()
                await my_dataset.snapshot_async(comment="My first snapshot", label="My first snapshot")

            asyncio.run(main())
            ```
        """
        return await super().snapshot_async(
            comment=comment,
            label=label,
            include_activity=include_activity,
            associate_activity_to_new_version=associate_activity_to_new_version,
            synapse_client=synapse_client,
        )


@dataclass
class DatasetCollectionSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for DatasetCollection operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store non-row information about a DatasetCollection including the columns and annotations.

        DatasetCollections have default columns that are managed by Synapse. The default behavior of
        this function is to include these default columns in the dataset collection when it is stored.
        This means that with the default behavior, any columns that you have added to your
        DatasetCollection will be overwritten by the default columns if they have the same name. To
        avoid this behavior, set the `include_default_columns` attribute to `False`.

        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the construction of your DatasetCollection instance, ie.
            `DatasetCollection(columns=[Column(name="foo")])`, they will be added at the beginning
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
            The DatasetCollection instance stored in synapse.

        Example: Create a new Dataset Collection from a list of Datasets by storing it.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Dataset

            syn = Synapse()
            syn.login()

            my_datasets = [Dataset(id="syn1234"), Dataset(id="syn1235"), Dataset(id="syn1236")]
            my_collection = DatasetCollection(parent_id="syn987", name="my-new-collection", items=my_datasets)
            my_collection.store()
            ```
        """
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the DatasetCollection from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the DatasetCollection
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The DatasetCollection instance stored in synapse.

        Example: Getting metadata about a Dataset Collection using id
            Get a Dataset Collection by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            collection = DatasetCollection(id="syn4567").get(include_activity=True)
            print(collection)

            # Columns are retrieved by default
            print(collection.columns)
            print(collection.activity)
            ```

        Example: Getting metadata about a Dataset Collection using name and parent_id
            Get a Dataset Collection by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            collection = DatasetCollection(name="my_collection", parent_id="syn1234").get(include_columns=True, include_activity=True)
            print(collection)
            print(collection.columns)
            print(collection.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the dataset collection from synapse. This is not version specific. If you'd like
        to delete a specific version of the dataset collection you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a Dataset Collection
            Deleting a Dataset Collection is only supported by the ID of the Dataset Collection.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            DatasetCollection(id="syn4567").delete()
            ```
        """
        return None

    def update_rows(
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
        """Update the values of rows in the dataset collection. This method can only
        be used to update values in custom columns. Default columns cannot be updated, but
        may be used as primary keys.

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
                after making changes to the data.

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.


        Example: Update custom column values in a Dataset Collection.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            my_collection = DatasetCollection(id="syn1234").get()

            # my_annotation must already exist in the dataset collection as a custom column
            modified_data = pd.DataFrame(
                {"id": ["syn1234"], "my_annotation": ["good data"]}
            )
            my_collection.update_rows(values=modified_data, primary_keys=["id"], dry_run=False)
            ```
        """
        return None

    def snapshot(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the dataset collection. A snapshot is a saved, read-only version of the dataset collection
        at the time it was created. Dataset collection snapshots are created using the asyncronous job API.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Dataset Collection
                and calling the `store()` method on the Dataset Collection instance. Adding an
                activity to a snapshot of a dataset collection is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the dataset collection. If False the activity will not be
                associated with the new version of the dataset collection. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Save a snapshot of a Dataset Collection.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            my_collection = DatasetCollection(id="syn1234").get()
            my_collection.snapshot(comment="My first snapshot", label="My first snapshot")
            ```
        """
        return TableUpdateTransaction


@dataclass
@async_to_sync
class DatasetCollection(
    DatasetCollectionSynchronousProtocol,
    AccessControllable,
    ViewBase,
    ViewStoreMixin,
    DeleteMixin,
    ColumnMixin,
    GetMixin,
    QueryMixin,
    ViewUpdateMixin,
    ViewSnapshotMixin,
):
    """A `DatasetCollection` object represents the metadata of a Synapse Dataset Collection.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DatasetCollection.html>

    A Dataset Collection is a type of view defined by a flat list of Datasets.

    Attributes:
        id: The unique immutable ID for this dataset collection. A new ID will be generated for new
            DatasetCollections. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this dataset collection. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of the dataset collection. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: The date this dataset collection was created.
        modified_on: The date this dataset collection was last modified.
            In YYYY-MM-DD-Thh:mm:ss.sssZ format
        created_by: The ID of the user that created this dataset collection.
        modified_by: The ID of the user that last modified this dataset collection.
        parent_id: The ID of the Entity that is the parent of this dataset collection.
        columns: The columns of this dataset collection. This is an ordered dictionary where the key is the
            name of the column and the value is the Column object. When creating a new instance
            of a DatasetCollection object you may pass any of the following types as the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the Column object
            - An OrderedDict where the key is the name of the column and the value is the Column object

            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your dataset collection class instance.

            You may modify the attributes of the Column object to change the column
            type, name, or other attributes. For example, suppose you'd like to change a
            column from a INTEGER to a DOUBLE. You can do so by changing the column type
            attribute of the Column object. The next time you store the dataset collection the column
            will be updated in Synapse with the new type.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Column, ColumnType

            syn = Synapse()
            syn.login()

            collection = DatasetCollection(id="syn1234").get()
            collection.columns["my_column"].column_type = ColumnType.DOUBLE
            collection.store()
            ```

            Note that the keys in this dictionary should match the column names as they are in
            Synapse. However, know that the name attribute of the Column object is used for
            all interactions with the Synapse API. The OrderedDict key is purely for the usage
            of this interface. For example, if you wish to rename a column you may do so by
            changing the name attribute of the Column object. The key in the OrderedDict does
            not need to be changed. The next time you store the dataset collection the column will be updated
            in Synapse with the new name and the key in the OrderedDict will be updated.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this dataset collection.
        version_comment: The version comment for this dataset collection.
        is_latest_version: If this is the latest version of the object.
        is_search_enabled: When creating or updating a dataset collection or view specifies if full
            text search should be enabled. Note that enabling full text search might
            slow down the indexing of the dataset collection or view.
        items: The flat list of datasets that define this collection. This is effectively
            a list of the rows that are in/will be in the collection after it is stored. The only way to add
            or remove rows is to add or remove items from this list.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analogous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance.
        annotations: Additional metadata associated with the dataset collection. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list.
        include_default_columns: When creating a dataset collection or view, specifies if default
            columns should be included. Default columns are columns that are
            automatically added to the dataset collection or view. These columns are managed by
            Synapse and cannot be modified. If you attempt to create a column with the
            same name as a default column, you will receive a warning when you store the
            dataset collection.

            **`include_default_columns` is only used if this is the first time that the
            view is being stored.** If you are updating an existing view this attribute
            will be ignored. If you want to add all default columns back to your view
            then you may use this code snippet to accomplish this:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                view = await DatasetCollection(id="syn1234").get_async()
                await view._append_default_columns()
                await view.store_async()

            asyncio.run(main())
            ```

            The column you are overriding will not behave the same as a default column.
            For example, suppose you create a column called `id` on a DatasetCollection. When
            using a default column, the `id` stores the Synapse ID of each of the
            entities included in the scope of the view. If you override the `id` column
            with a new column, the `id` column will no longer store the Synapse ID of
            the entities in the view. Instead, it will store the values you provide when
            you store the dataset collection. It will be stored as an annotation on the entity for
            the row you are modifying.

    Example: Create a new Dataset Collection from a list of Datasets.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection, Dataset

        syn = Synapse()
        syn.login()

        my_datasets = [Dataset(id="syn1234"), Dataset(id="syn1235"), Dataset(id="syn1236")]
        my_collection = DatasetCollection(parent_id="syn987", name="my-new-collection", items=my_datasets)
        my_collection.store()
        ```

    Example: Add Datasets to an existing Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection, Dataset

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()

        # Add a dataset to the collection
        my_collection.add_item(Dataset(id="syn1235"))
        my_collection.store()
        ```

    Example: Remove Datasets from a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection, Dataset

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()

        # Remove a dataset from the collection
        my_collection.remove_item(Dataset(id="syn1235"))
        my_collection.store()
        ```

    Example: Query data from a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()
        row = my_collection.query(query="SELECT * FROM syn1234 WHERE id = 'syn1235'")
        print(row)
        ```

    Example: Add a custom column to a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection, Column, ColumnType

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()
        my_collection.add_column(Column(name="my_annotation", column_type=ColumnType.STRING))
        my_collection.store()
        ```

    Example: Update custom column values in a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()
        # my_annotation must already exist in the dataset collection as a custom column
        modified_data = pd.DataFrame(
            {"id": ["syn1234"], "my_annotation": ["good data"]}
        )
        my_collection.update_rows(values=modified_data, primary_keys=["id"], dry_run=False)
        ```

    Example: Save a snapshot of a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection

        syn = Synapse()
        syn.login()

        my_collection = DatasetCollection(id="syn1234").get()
        my_collection.snapshot(comment="My first snapshot", label="My first snapshot")
        ```

    Example: Deleting a Dataset Collection.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import DatasetCollection

        syn = Synapse()
        syn.login()

        DatasetCollection(id="syn4567").delete()
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this dataset collection. A new ID will be generated for new
    dataset collections. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this dataset collection. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
    apostrophes, and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """The date this dataset collection was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this dataset collection was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ format"""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this dataset collection."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this dataset collection."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this dataset collection."""

    version_number: Optional[int] = field(default=None, compare=False)
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this dataset collection."""

    version_comment: Optional[str] = None
    """The version comment for this dataset collection."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """If this is the latest version of the object."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a dataset collection or view specifies if full text search
    should be enabled. Note that enabling full text search might slow down the
    indexing of the dataset collection or view."""

    items: Optional[List["EntityRef"]] = field(default_factory=list, compare=False)
    """The flat list of EntityRefs referring to the datasets that define this collection."""

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict, compare=False)
    """
    The columns of this dataset collection. This is an ordered dictionary where the key is the
    name of the column and the value is the Column object. When creating a new instance
    of a DatasetCollection object you may pass any of the following types as the `columns` argument:

    - A list of Column objects
    - A dictionary where the key is the name of the column and the value is the Column object
    - An OrderedDict where the key is the name of the column and the value is the Column object

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your dataset collection class instance.

    You may modify the attributes of the Column object to change the column
    type, name, or other attributes. For example, suppose you'd like to change a
    column from a INTEGER to a DOUBLE. You can do so by changing the column type
    attribute of the Column object. The next time you store the dataset collection the column
    will be updated in Synapse with the new type.

    ```python
    from synapseclient import Synapse
    from synapseclient.models import DatasetCollection, Column, ColumnType

    syn = Synapse()
    syn.login()

    collection = DatasetCollection(id="syn1234").get()
    collection.columns["my_column"].column_type = ColumnType.DOUBLE
    collection.store()
    ```

    Note that the keys in this dictionary should match the column names as they are in
    Synapse. However, know that the name attribute of the Column object is used for
    all interactions with the Synapse API. The OrderedDict key is purely for the usage
    of this interface. For example, if you wish to rename a column you may do so by
    changing the name attribute of the Column object. The key in the OrderedDict does
    not need to be changed. The next time you store the dataset collection the column will be updated
    in Synapse with the new name and the key in the OrderedDict will be updated.
    """

    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)
    """
    Columns to delete when the dataset collection is stored. The key in this dict is the ID of the
    column to delete. The value is the Column object that represents the column to
    delete.
    """

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analogous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

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
    """Additional metadata associated with the dataset collection. The key is the name of your
    desired annotations. The value is an object containing"""

    _last_persistent_instance: Optional["DatasetCollection"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    view_entity_type: ViewEntityType = ViewEntityType.DATASET_COLLECTION
    """The API model string for the type of view. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewEntityType` enum.
    """

    view_type_mask: ViewTypeMask = ViewTypeMask.DATASET_COLLECTION
    """The Bit Mask representing DatasetCollection type.
    As defined in the Synapse REST API:
    <https://rest-docs.synapse.org/rest/GET/column/tableview/defaults.html>"""

    def __post_init__(self):
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance
            or self._last_persistent_instance != self
            or (not self._last_persistent_instance.items and self.items)
            or self._last_persistent_instance.items != self.items
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
        self._last_persistent_instance.items = (
            [dataclasses.replace(item) for item in self.items] if self.items else []
        )

    def fill_from_dict(self, entity, set_annotations: bool = True) -> "Self":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            synapse_table: The data coming from the Synapse API

        Returns:
            The DatasetCollection object instance.
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
        self.items = [
            EntityRef(id=item["entityId"], version=item["versionNumber"])
            for item in entity.get("items", [])
        ]
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
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": concrete_types.DATASET_COLLECTION_ENTITY,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "columnIds": (
                [
                    column.id
                    for column in self._last_persistent_instance.columns.values()
                ]
                if self._last_persistent_instance
                and self._last_persistent_instance.columns
                else []
            ),
            "isSearchEnabled": self.is_search_enabled,
            "items": (
                [item.to_synapse_request() for item in self.items] if self.items else []
            ),
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    def add_item(
        self,
        item: Union["Dataset", "EntityRef"],
    ) -> None:
        """Adds a dataset to the dataset collection.
        Effect is not seen until the dataset collection is stored.

        Arguments:
            item: Dataset to add to the collection. Must be a Dataset.

        Raises:
            ValueError: If the item is not a Dataset

        Example: Add a Dataset to a Dataset Collection.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Dataset

            syn = Synapse()
            syn.login()

            my_collection = DatasetCollection(id="syn1234").get()
            my_collection.add_item(Dataset(id="syn1235"))
            my_collection.store()
            ```
        """
        if not isinstance(item, (Dataset, EntityRef)):
            raise ValueError(
                f"item must be a Dataset or EntityRef. {item} is a {type(item)}"
            )

        # EntityRef uses `version`, Dataset uses `version_number`
        version = item.version if isinstance(item, EntityRef) else item.version_number

        if not any(
            current_item.id == item.id and current_item.version == version
            for current_item in self.items
        ):
            self.items.append(EntityRef(id=item.id, version=version))

    def remove_item(
        self,
        item: Union["Dataset", "EntityRef"],
    ) -> None:
        """
        Removes an entity from the dataset collection. Must be a Dataset or EntityRef.
        Effect is not seen until the dataset collection is stored.
        Unless the version is specified, all entities with the same ID will be removed.

        Arguments:
            item: The Dataset to remove from the collection

        Returns:
            None

        Raises:
            ValueError: If the item is not a Dataset

        Example: Remove a Dataset from a Dataset Collection.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Dataset

            syn = Synapse()
            syn.login()

            my_collection = DatasetCollection(id="syn1234").get()
            my_collection.remove_item(Dataset(id="syn1235", version_number=1))
            my_collection.store()
            ```

        Example: Remove all versions of a Dataset from a Dataset Collection.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Dataset

            syn = Synapse()
            syn.login()

            my_collection = DatasetCollection(id="syn1234").get()
            my_collection.remove_item(Dataset(id="syn1235"))
            my_collection.store()
            ```
        """
        if not isinstance(item, (Dataset, EntityRef)):
            raise ValueError(
                f"item must be a Dataset or EntityRef. {item} is a {type(item)}"
            )

        version = item.version if isinstance(item, EntityRef) else item.version_number

        if version:
            self.items = [
                current_item
                for current_item in self.items
                if current_item != EntityRef(id=item.id, version=version)
            ]
        else:
            self.items = [
                current_item
                for current_item in self.items
                if current_item.id != item.id
            ]

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store information about a DatasetCollection including the columns and annotations. This includes updating
        the `item`s of the DatasetCollection which will update the rows of the visualization in Synapse.

        DatasetCollections have default columns that are managed by Synapse. The default behavior of
        this function is to include these default columns in the dataset collection when it is stored.
        This means that with the default behavior, any columns that you have added to your
        DatasetCollection will be overwritten by the default columns if they have the same name. To
        avoid this behavior, set the `include_default_columns` attribute to `False`.

        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the construction of your DatasetCollection instance, ie.
            `DatasetCollection(columns=[Column(name="foo")])`, they will be added at the beginning
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
            The DatasetCollection instance stored in synapse.

        Example: Create a new Dataset Collection from a list of Datasets by storing it.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection, Dataset

            syn = Synapse()
            syn.login()

            async def main():
                my_datasets = [Dataset(id="syn1234"), Dataset(id="syn1235"), Dataset(id="syn1236")]
                my_collection = DatasetCollection(parent_id="syn987", name="my-new-collection", items=my_datasets)
                await my_collection.store_async()

            asyncio.run(main())
            ```
        """
        return await super().store_async(
            dry_run=dry_run,
            job_timeout=job_timeout,
            synapse_client=synapse_client,
        )

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the DatasetCollection from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the DatasetCollection
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The DatasetCollection instance stored in synapse.

        Example: Getting metadata about a Dataset Collection using id
            Get a DatasetCollection by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get_async` call, then you'll make the changes, and finally call the
            `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                collection = await DatasetCollection(id="syn4567").get_async(include_activity=True)
                print(collection)

                # Columns are retrieved by default
                print(collection.columns)
                print(collection.activity)

            asyncio.run(main())
            ```

        Example: Getting metadata about a Dataset Collection using name and parent_id
            Get a Dataset Collection by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get_async` call, then you'll make the changes,
            and finally call the `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                collection = await DatasetCollection(
                    name="my_collection",
                    parent_id="syn1234"
                ).get_async(
                    include_columns=True,
                    include_activity=True
                )
                print(collection)
                print(collection.columns)
                print(collection.activity)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the dataset collection from synapse. This is not version specific. If you'd like
        to delete a specific version of the dataset collection you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a Dataset Collection
            Deleting a Dataset Collection is only supported by the ID of the Dataset Collection.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                await DatasetCollection(id="syn4567").delete_async()

            asyncio.run(main())
            ```
        """
        await super().delete_async(synapse_client=synapse_client)

    async def update_rows_async(
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
        """Update the values of rows in the dataset collection. This method can only
        be used to update values in custom columns. Default columns cannot be updated, but
        may be used as primary keys.

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
                after making changes to the data.

            wait_for_eventually_consistent_view_timeout: The maximum amount of time to
                wait for a view to be eventually consistent. The default is 600 seconds.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            **kwargs: Additional arguments that are passed to the `pd.DataFrame`
                function when the `values` argument is a path to a csv file.


        Example: Update custom column values in a Dataset Collection.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                my_collection = await DatasetCollection(id="syn1234").get_async()
                # my_annotation must already exist in the dataset collection as a custom column
                modified_data = pd.DataFrame(
                    {"id": ["syn1234"], "my_annotation": ["good data"]}
                )
                await my_collection.update_rows_async(values=modified_data, primary_keys=["id"], dry_run=False)

            asyncio.run(main())
            ```
        """
        await super().update_rows_async(
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

    async def snapshot_async(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the dataset collection. A snapshot is a saved, read-only version of the dataset collection
        at the time it was created. Dataset collection snapshots are created using the asyncronous job API.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the Dataset
                Collection and calling the `store()` method on the Dataset Collection
                instance. Adding an activity to a snapshot of a dataset collection is
                meant to capture the provenance of the data at the time of the snapshot.
                Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the dataset collection. If False the activity will
                not be associated with the new version of the dataset collection. Defaults
                to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Save a snapshot of a Dataset Collection.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import DatasetCollection

            syn = Synapse()
            syn.login()

            async def main():
                my_collection = await DatasetCollection(id="syn1234").get_async()
                await my_collection.snapshot_async(comment="My first snapshot", label="My first snapshot")

            asyncio.run(main())
            ```
        """
        return await super().snapshot_async(
            comment=comment,
            label=label,
            include_activity=include_activity,
            associate_activity_to_new_version=associate_activity_to_new_version,
            synapse_client=synapse_client,
        )
