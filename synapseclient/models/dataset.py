import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

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
    Represents a reference to the id and version of an entity to be used in collections.

    Attributes:
        id: The 'syn' identifier of the entity.
        version: Indicates a specific version of a the entity.
    """

    id: str
    """The 'syn' identifier of the entity."""

    version: int
    """Indicates a specific version of a the entity."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "entityId": self.id,
            "versionNumber": self.version,
        }


@dataclass
@async_to_sync
class Dataset(
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
    """A Dataset object represents the metadata of a Synapse Dataset.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Dataset.html>

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
            not need to be changed. The next time you store the dataset the column will be updated
            in Synapse with the new name and the key in the OrderedDict will be updated.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this dataset.
        version_comment: The version comment for this dataset.
        is_latest_version: If this is the latest version of the object.
        is_search_enabled: When creating or updating a dataset or view specifies if full
            text search should be enabled. Note that enabling full text search might
            slow down the indexing of the dataset or view.
        items: The flat list of file entity references that define this dataset.
        size: The cumulative size, in bytes, of all items(files) in the dataset. This is
            only correct after the dataset has been stored or newly read from Synapse.
        checksum: The checksum is computed over a sorted concatenation of the checksums
            of all items in the dataset. This is only correct after the dataset has been
            stored or newly read from Synapse.
        count: The number of items/files in the dataset. This is only correct after the
            dataset has been stored or newly read from Synapse.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance.
        annotations: Additional metadata associated with the dataset. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list.
    """

    id: Optional[str] = None
    """The unique immutable ID for this dataset. A new ID will be generated for new
    Datasets. Once issued, this ID is guaranteed to never change or be re-issued"""

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
    analygous to the Activity defined in the
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
            "columnIds": [column.id for column in self.columns.values()],
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

    async def add_item_async(
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
            synapse_client: The Synapse client to use. Defaults to None.

        Raises:
            ValueError: If the item is not an EntityRef, File, or Folder

        #TODO: Add Examples
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
                    await self.add_item_async(
                        item=Folder(id=child["id"]), synapse_client=client
                    )
        else:
            raise ValueError(
                f"item must be one of EntityRef, File, or Folder. {item} is a {type(item)}"
            )

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
            synapse_client: The Synapse client to use. Defaults to None.

        Raises:
            ValueError: If the item is not an EntityRef, File, or Folder

        #TODO: Add Examples
        """
        return None

    def _remove_entity_ref(self, entity_ref: EntityRef) -> None:
        """Helper function to remove an EntityRef from the items list of the dataset.

        Arguments:
            entity_ref: The EntityRef to remove from the items list of the dataset.
        """
        if entity_ref not in self.items:
            raise ValueError(f"Entity {entity_ref.id} not found in items list")
        self.items.remove(entity_ref)

    async def remove_item_async(
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
            synapse_client: The Synapse client to use. Defaults to None.

        Returns:
            Dataset: The dataset with the item removed

        Raises:
            ValueError: If the item is not a valid type

        #TODO: Add Examples
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
                    await self.remove_item_async(
                        item=Folder(id=child["id"]), synapse_client=client
                    )
        else:
            raise ValueError(
                f"item must be one of str, EntityRef, File, or Folder, {item} is a {type(item)}"
            )

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
            synapse_client: The Synapse client to use. Defaults to None.

        Returns:
            None

        Raises:
            ValueError: If the item is not a valid type

        #TODO: Add Examples
        """
        return None

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store non-row information about a Dataset including the columns and annotations.

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
        """
        return await super().store_async(
            dry_run=dry_run,
            job_timeout=job_timeout,
            synapse_client=synapse_client,
        )

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Store non-row information about a Dataset including the columns and annotations.

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

        Example:
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Dataset

            syn = Synapse()
            syn.login()

            dataset = Dataset(id="syn4567").get()
            dataset.store()
            ```
        """
        return Self

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
                dataset = await Dataset(name="my_dataset", parent_id="syn1234").get_async(include_columns=True, include_activity=True)
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
        return Self

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
        """Placeholder Docstring specific to the Dataset class' implementation
        of the update_rows_async method."""
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
        """Placeholder Docstring specific to the Dataset class' implementation
        of the update_rows method."""
        return None

    async def snapshot_async(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        activity: Optional[Activity] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Create a snapshot of the view."""
        return await super().snapshot_async(
            comment=comment,
            label=label,
            activity=activity,
            synapse_client=synapse_client,
        )

    def snapshot(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        activity: Optional[Activity] = None,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Create a snapshot of the view."""
        return TableUpdateTransaction

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
        return await QueryMixin.query_async(
            query=query,
            include_row_id_and_row_version=include_row_id_and_row_version,
            convert_to_datetime=convert_to_datetime,
            synapse_client=synapse_client,
            **kwargs,
        )

    @staticmethod
    def query(
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
            from synapseclient import Synapse
            from synapseclient.models import query

            syn = Synapse()
            syn.login()

            results = query(query="SELECT * FROM syn1234")
            print(results)
            ```
        """
        return DATA_FRAME_TYPE
