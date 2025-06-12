import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Protocol, Set, TypeVar, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import ViewTypeMask
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import MB, delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins import AccessControllable, BaseJSONSchema
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
    TableUpdateTransaction,
    ViewBase,
    ViewSnapshotMixin,
    ViewStoreMixin,
    ViewUpdateMixin,
)
from synapseclient.models.table_components import Column

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


class EntityViewSynchronousProtocol(Protocol):
    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store non-row information about a view including the columns and annotations.

        Note the following behavior for the order of columns:

        - If a column is added via the `add_column` method it will be added at the
            index you specify, or at the end of the columns list.
        - If column(s) are added during the contruction of your `EntityView` instance, ie.
            `EntityView(columns=[Column(name="foo")])`, they will be added at the begining
            of the columns list.
        - If you use the `store_rows` method and the `schema_storage_strategy` is set to
            `INFER_FROM_DATA` the columns will be added at the end of the columns list.

        Arguments:
            dry_run: If True, will not actually store the entityview but will log to
                the console what would have been stored.

            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when updating the entityview schema. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The EntityView instance stored in synapse.
        """
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the entityview from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the file
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The EntityView instance stored in synapse.

        Example: Getting metadata about a entityview using id
            Get a entityview by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(id="syn4567").get(include_activity=True)
            print(entityview)

            # Columns are retrieved by default
            print(entityview.columns)
            print(entityview.activity)
            ```

        Example: Getting metadata about a entityview using name and parent_id
            Get a entityview by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(name="my_table", parent_id="syn1234").get(include_columns=True, include_activity=True)
            print(entityview)
            print(entityview.columns)
            print(entityview.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the entity from synapse. This is not version specific. If you'd like
        to delete a specific version of the entity you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a entityview
            Deleting a entityview is only supported by the ID of the entityview.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            EntityView(id="syn4567").delete()
            ```
        """
        return None

    def update_rows(
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
        - The values used as the `primary_keys` must be unique in the entityview. If there
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
            and calling the `store()` method on the EntityView instance. Adding an activity
            to a snapshot of a entityview is meant to capture the provenance of the data at
            the time of the snapshot.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            view = EntityView(id="syn4567")
            snapshot = view.snapshot(label="Q1 2025", comment="Results collected in Lab A", include_activity=True, associate_activity_to_new_version=True)
            print(snapshot)
            ```

        Example: Creating a snapshot of a view without an activity
            Create a snapshot of a view without including the activity. This is used in
            cases where we do not have any Provenance to associate with the snapshot and
            we do not want to persist any activity that may be present on the view to
            the new version of the view.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            view = EntityView(id="syn4567")
            snapshot = view.snapshot(label="Q1 2025", comment="Results collected in Lab A", include_activity=False, associate_activity_to_new_version=False)
            print(snapshot)
            ```
        """
        # Replaced at runtime
        return TableUpdateTransaction(entity_id=None)


@dataclass
@async_to_sync
class EntityView(
    AccessControllable,
    ViewBase,
    ViewStoreMixin,
    DeleteMixin,
    ColumnMixin,
    GetMixin,
    QueryMixin,
    ViewUpdateMixin,
    ViewSnapshotMixin,
    EntityViewSynchronousProtocol,
    BaseJSONSchema,
):
    """
    A view of Entities within a defined scope. The purpose of a `EntityView`, also known
    as an `FileView`, is to create a SQL-like view of entities within a
    defined scope. The scope is defined by the `scope_ids` attribute. The `scope_ids`
    attribute is a list of `syn` container ids that define where to search for rows to
    include in the view. Entities within the scope are included in the view if they
    match the criteria defined by the `view_type_mask` attribute. The `view_type_mask`
    attribute is a bit mask representing the types to include in the view. You may set
    this to a single value using the [ViewTypeMask][synapseclient.models.ViewTypeMask]
    enum or you may set this to multiple values using the bitwise OR operator.

    Attributes:
        id: The unique immutable ID for this dataset. A new ID will be generated for new
            Datasets. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this dataset. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of this entity. Must be 1000 characters or less.
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
        version_number: The version number issued to this version on the object.
        version_label: The version label for this dataset.
        version_comment: The version comment for this dataset.
        is_latest_version: If this is the latest version of the object.
        columns: The columns of this view. This is an ordered dictionary where the key
            is the name of the column and the value is the Column object. When creating
            a new instance of a View object you may pass any of the following types as
            the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the
              Column object
            - An OrderedDict where the key is the name of the column and the value is
              the Column object

            The order of the columns will be the order they are stored in Synapse. If
            you need to reorder the columns the recommended approach is to use the
            `.reorder_column()` method. Additionally, you may add, and delete columns
            using the `.add_column()`, and `.delete_column()` methods on your view
            class instance.

            You may modify the attributes of the Column object to change the column
            type, name, or other attributes. For example suppose I'd like to change a
            column from a INTEGER to a DOUBLE. I can do so by changing the column type
            attribute of the Column object. The next time you store the view the column
            will be updated in Synapse with the new type.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView, Column, ColumnType

            syn = Synapse()
            syn.login()

            view = EntityView(id="syn1234").get()
            view.columns["my_column"].column_type = ColumnType.DOUBLE
            view.store()
            ```

            Note that the keys in this dictionary should match the column names as they
            are in Synapse. However, know that the name attribute of the Column object
            is used for all interactions with the Synapse API. The OrderedDict key is
            purely for the usage of this interface. For example, if you wish to rename
            a column you may do so by changing the name attribute of the Column object.
            The key in the OrderedDict does not need to be changed. The next time you
            store the view the column will be updated in Synapse with the new name and
            the key in the OrderedDict will be updated.
        include_default_columns: When creating a entityview or view, specifies if default
            columns should be included. Default columns are columns that are
            automatically added to the entityview or view. These columns are managed by
            Synapse and cannot be modified. If you attempt to create a column with the
            same name as a default column, you will receive a warning when you store the
            entityview.

            **`include_default_columns` is only used if this is the first time that the
            view is being stored.** If you are updating an existing view this attribute
            will be ignored. If you want to add all default columns back to your view
            then you may use this code snippet to accomplish this:

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

            The column you are overriding will not behave the same as a default column.
            For example, suppose you create a column called `id` on a EntityView. When
            using a default column, the `id` stores the Synapse ID of each of the
            entities included in the scope of the view. If you override the `id` column
            with a new column, the `id` column will no longer store the Synapse ID of
            the entities in the view. Instead, it will store the values you provide when
            you store the entityview. It will be stored as an annotation on the entity for
            the row you are modifying.
        is_search_enabled: When creating or updating a dataset or view specifies if full
            text search should be enabled. Note that enabling full text search might
            slow down the indexing of the dataset or view.
        view_type_mask: Bit mask representing the types to include in the view. You may
            set this to a single value using the [ViewTypeMask][synapseclient.models.ViewTypeMask]
            enum or you may set this to multiple values using the bitwise OR operator.
            When this is returned after storing or reading from Synapse it will be
            returned as an integer.

            The following are the possible types (type=):

            - File=0x01
            - Project=0x02
            - Table=0x04
            - Folder=0x08
            - View=0x10
            - Docker=0x20
            - SubmissionView=0x40
            - Dataset=0x80
            - DatasetCollection=0x100
            - MaterializedView=0x200

            To include multiple types in the view you will be using the bitwise OR
            operator to combine the types. For example, if you want to include both
            Files and Folders in the view you would use the following code:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView, ViewTypeMask

            syn = Synapse()
            syn.login()

            view = EntityView(name="My EntityView", parent_id="syn1234",
                            scope_ids=["syn1234"],
                            view_type_mask=ViewTypeMask.FILE | ViewTypeMask.FOLDER).store()
            ```
        scope_ids: The list of container ids that define the scope of this view. This
            may be a single container or multiple containers. A container in this
            context may refer to a Project or Folder which contains zero or more
            entities. The entities in the container(s) will be included in the view if
            they match the criteria defined by the `view_type_mask` attribute.
        activity: The Activity model represents the main record of Provenance in Synapse.
            It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance. Activity
            cannot be removed during a store operation by setting it to None. You must
            use: [synapseclient.models.Activity.delete_async][] or
            [synapseclient.models.Activity.disassociate_from_entity_async][].
        annotations: Additional metadata associated with the entityview. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list. To remove all annotations set this
            to an empty dict `{}` or None and store the entity.
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new
    Entities. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less. Names may only
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
    """The date this entity was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this entity was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ format"""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this entity."""

    version_number: Optional[int] = field(default=None, compare=False)
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this entity."""

    version_comment: Optional[str] = None
    """The version comment for this entity."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """If this is the latest version of the object."""

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict, compare=False)
    """
    The columns of this view. This is an ordered dictionary where the key is the
    name of the column and the value is the Column object. When creating a new instance
    of a View object you may pass any of the following types as the `columns` argument:

    - A list of Column objects
    - A dictionary where the key is the name of the column and the value is the Column object
    - An OrderedDict where the key is the name of the column and the value is the Column object

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your view class instance.

    You may modify the attributes of the Column object to change the column
    type, name, or other attributes. For example suppose I'd like to change a
    column from a INTEGER to a DOUBLE. I can do so by changing the column type
    attribute of the Column object. The next time you store the view the column
    will be updated in Synapse with the new type.

    ```python
    from synapseclient import Synapse
    from synapseclient.models import EntityView, Column, ColumnType

    syn = Synapse()
    syn.login()

    view = EntityView(id="syn1234").get()
    view.columns["my_column"].column_type = ColumnType.DOUBLE
    view.store()
    ```

    Note that the keys in this dictionary should match the column names as they are in
    Synapse. However, know that the name attribute of the Column object is used for
    all interactions with the Synapse API. The OrderedDict key is purely for the usage
    of this interface. For example, if you wish to rename a column you may do so by
    changing the name attribute of the Column object. The key in the OrderedDict does
    not need to be changed. The next time you store the view the column will be updated
    in Synapse with the new name and the key in the OrderedDict will be updated.
    """

    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)
    """
    Columns to delete when the entityview is stored. The key in this dict is the ID of the
    column to delete. The value is the Column object that represents the column to
    delete.
    """

    is_search_enabled: Optional[bool] = None
    """
    When creating or updating a entityview or view specifies if full text search
    should be enabled. Note that enabling full text search might slow down the
    indexing of the entityview or view.
    """

    view_type_mask: Optional[Union[int, ViewTypeMask]] = None
    """
    Bit mask representing the types to include in the view. You may set this to a
    single value using the [ViewTypeMask][synapseclient.models.ViewTypeMask] enum or
    you may set this to multiple values using the bitwise OR operator. When this is
    returned after storing or reading from Synapse it will be returned as an integer.

    The following are the possible types (type=):

    - File=0x01
    - Project=0x02
    - Table=0x04
    - Folder=0x08
    - View=0x10
    - Docker=0x20
    - SubmissionView=0x40
    - Dataset=0x80
    - DatasetCollection=0x100
    - MaterializedView=0x200

    To include multiple types in the view you will be using the bitwise OR operator
    to combine the types. For example, if you want to include both Files and Folders
    in the view you would use the following code:

    ```python
    from synapseclient import Synapse
    from synapseclient.models import EntityView, ViewTypeMask

    syn = Synapse()
    syn.login()


    view = EntityView(name="My EntityView", parent_id="syn1234",
                    scope_ids=["syn1234"],
                    view_type_mask=ViewTypeMask.FILE | ViewTypeMask.FOLDER).store()
    ```
    """

    scope_ids: Optional[Set[str]] = field(default_factory=set)
    """
    The list of container ids that define the scope of this view. This may be a
    single container or multiple containers. A container in this context may refer to
    a Project or Folder which contains zero or more entities. The entities in the
    container(s) will be included in the view if they match the criteria defined by the
    `view_type_mask` attribute.
    """

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
    """Additional metadata associated with the entityview. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`
    or None and store the entity."""

    _last_persistent_instance: Optional["EntityView"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def __post_init__(self):
        """Post initialization of the EntityView object."""
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)
        if isinstance(self.scope_ids, list):
            self.scope_ids = set(self.scope_ids)

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
        self._last_persistent_instance = dataclasses.replace(self)
        self._last_persistent_instance.activity = (
            dataclasses.replace(self.activity)
            if self.activity and self.activity.id
            else None
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
        self._last_persistent_instance.scope_ids = (
            deepcopy(self.scope_ids) if self.scope_ids else set()
        )

    def fill_from_dict(
        self, entity: Dict, set_annotations: bool = True
    ) -> "EntityView":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse API

        Returns:
            The EntityView object instance.
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
        self.view_type_mask = entity.get("viewTypeMask", None)
        self.scope_ids = set(f"syn{id}" for id in entity.get("scopeIds", []))

        if set_annotations:
            self.annotations = Annotations.from_dict(entity.get("annotations", {}))
        return self

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        scopes_without_syn = (
            {str(id).replace("syn", "") for id in self.scope_ids}
            if self.scope_ids
            else None
        )
        entity = {
            "name": self.name,
            "description": self.description,
            "id": self.id,
            "etag": self.etag,
            "parentId": self.parent_id,
            "concreteType": concrete_types.ENTITY_VIEW,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "columnIds": [
                column.id for column in self._last_persistent_instance.columns.values()
            ]
            if self._last_persistent_instance and self._last_persistent_instance.columns
            else [],
            "isSearchEnabled": self.is_search_enabled,
            "viewTypeMask": self.view_type_mask.value
            if isinstance(self.view_type_mask, ViewTypeMask)
            else self.view_type_mask,
            "scopeIds": list(scopes_without_syn) if scopes_without_syn else None,
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Get the metadata about the entityview from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the file
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The EntityView instance stored in synapse.

        Example: Getting metadata about a entityview using id
            Get a entityview by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get_async` call, then you'll make the changes, and finally call the
            `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            async def main():
                my_view = await EntityView(id="syn4567").get_async(include_activity=True)
                print(my_view)

                # Columns are retrieved by default
                print(my_view.columns)
                print(my_view.activity)

            asyncio.run(main())
            ```

        Example: Getting metadata about a entityview using name and parent_id
            Get a entityview by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get_async` call, then you'll make the changes,
            and finally call the `.store_async()` method.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            async def main():
                my_view = await EntityView(name="my_fv", parent_id="syn1234").get_async(include_columns=True, include_activity=True)
                print(my_view)
                print(my_view.columns)
                print(my_view.activity)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    def add_column(
        self, column: Union["Column", List["Column"]], index: int = None
    ) -> None:
        """Add column(s) to the entityview. Note that this does not store the column(s) in
        Synapse. You must call the `.store()` function on this entityview class instance to
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
            This example shows how you may add a single column to a entityview and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(
                id="syn1234"
            ).get(include_columns=True)

            entityview.add_column(
                Column(name="my_column", column_type=ColumnType.STRING)
            )
            entityview.store()
            ```


        Example: Adding multiple columns
            This example shows how you may add multiple columns to a entityview and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(
                id="syn1234"
            ).get(include_columns=True)

            entityview.add_column([
                Column(name="my_column", column_type=ColumnType.STRING),
                Column(name="my_column2", column_type=ColumnType.INTEGER),
            ])
            entityview.store()
            ```

        Example: Adding a column at a specific index
            This example shows how you may add a column at a specific index to a entityview
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(
                id="syn1234"
            ).get(include_columns=True)

            entityview.add_column(
                Column(name="my_column", column_type=ColumnType.STRING),
                # Add the column at the beginning of the list
                index=0
            )
            entityview.store()
            ```

        Example: Adding a single column (async)
            This example shows how you may add a single column to a entityview and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            async def main():
                entityview = await EntityView(
                    id="syn1234"
                ).get_async(include_columns=True)

                entityview.add_column(
                    Column(name="my_column", column_type=ColumnType.STRING)
                )
                await entityview.store_async()

            asyncio.run(main())
            ```

        Example: Adding multiple columns (async)
            This example shows how you may add multiple columns to a entityview and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            async def main():
                entityview = await EntityView(
                    id="syn1234"
                ).get_async(include_columns=True)

                entityview.add_column([
                    Column(name="my_column", column_type=ColumnType.STRING),
                    Column(name="my_column2", column_type=ColumnType.INTEGER),
                ])
                await entityview.store_async()

            asyncio.run(main())
            ```

        Example: Adding a column at a specific index (async)
            This example shows how you may add a column at a specific index to a entityview
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            async def main():
                entityview = await EntityView(
                    id="syn1234"
                ).get_async(include_columns=True)

                entityview.add_column(
                    Column(name="my_column", column_type=ColumnType.STRING),
                    # Add the column at the beginning of the list
                    index=0
                )
                await entityview.store_async()

            asyncio.run(main())
            ```
        """
        return super().add_column(column=column, index=index)

    def reorder_column(self, name: str, index: int) -> None:
        """Reorder a column in the entityview. Note that this does not store the column in
        Synapse. You must call the `.store()` function on this entityview class instance to
        store the column in Synapse. This is a convenience function to eliminate
        the need to manually reorder the `.columns` attribute dictionary.

        You must ensure that the index is within the bounds of the number of columns in
        the entityview. If you pass in an index that is out of bounds the column will be
        added to the end of the list.

        Arguments:
            name: The name of the column to reorder.
            index: The index to move the column to starting with 0.

        Returns:
            None

        Example: Reordering a column
            This example shows how you may reorder a column in a entityview and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(
                id="syn1234"
            ).get(include_columns=True)

            # Move the column to the beginning of the list
            entityview.reorder_column(name="my_column", index=0)
            entityview.store()
            ```


        Example: Reordering a column (async)
            This example shows how you may reorder a column in a entityview and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, EntityView

            syn = Synapse()
            syn.login()

            async def main():
                entityview = await EntityView(
                    id="syn1234"
                ).get_async(include_columns=True)

                # Move the column to the beginning of the list
                entityview.reorder_column(name="my_column", index=0)
                entityview.store_async()

            asyncio.run(main())
            ```
        """
        return super().reorder_column(name=name, index=index)

    def delete_column(self, name: str) -> None:
        """
        Mark a column for deletion. Note that this does not delete the column from
        Synapse. You must call the `.store()` function on this entityview class instance to
        delete the column from Synapse. This is a convenience function to eliminate
        the need to manually delete the column from the dictionary and add it to the
        `._columns_to_delete` attribute.

        Arguments:
            name: The name of the column to delete.

        Returns:
            None

        Example: Deleting a column
            This example shows how you may delete a column from a entityview and then store
            the change back in Synapse.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            entityview = EntityView(
                id="syn1234"
            ).get(include_columns=True)

            entityview.delete_column(name="my_column")
            entityview.store()
            ```

        Example: Deleting a column (async)
            This example shows how you may delete a column from a entityview and then store
            the change back in Synapse.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import EntityView

            syn = Synapse()
            syn.login()

            async def main():
                entityview = await EntityView(
                    id="syn1234"
                ).get_async(include_columns=True)

                entityview.delete_column(name="my_column")
                await entityview.store_async()

            asyncio.run(main())
            ```
        """
        return super().delete_column(name=name)
