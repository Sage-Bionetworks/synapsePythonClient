import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

from typing_extensions import Self

from synapseclient import Synapse
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


@dataclass()
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


# TODO: Create a DatasetSynchronousProtocol
# class Dataset(DatasetSynchronousProtocol, AccessControllable, TableOperator):


@dataclass()
@async_to_sync
class Dataset(AccessControllable, TableOperator, TableRowOperator):
    """A Dataset represents the metadata of a dataset.

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

            After the Dataset object is created the columns attribute will be an OrderedDict. If
            you wish to replace the columns after the Dataset is constructed you may do so by
            calling the `.set_columns()` method. For example:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Dataset

            syn = Synapse()
            syn.login()

            # Initialize the dataset with a list of columns
            dataset = Dataset(name="my_dataset", columns=[Column(name="my_column", column_type=ColumnType.STRING)])

            # Replace the columns with a different list of columns
            dataset.set_columns(columns=[Column(name="my_new_column", column_type=ColumnType.STRING)])
            dataset.store()
            ```


            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your dataset class instance.


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

    After the Dataset object is created the columns attribute will be an OrderedDict. If
    you wish to replace the columns after the Dataset is constructed you may do so by
    calling the `.set_columns()` method. For example:

    ```python
    from synapseclient import Synapse
    from synapseclient.models import Column, ColumnType, Dataset

    syn = Synapse()
    syn.login()

    # Initialize the dataset with a list of columns
    dataset = Dataset(name="my_dataset", columns=[Column(name="my_column", column_type=ColumnType.STRING)])

    # Replace the columns with a different list of columns
    dataset.set_columns(columns=[Column(name="my_new_column", column_type=ColumnType.STRING)])
    dataset.store()
    ```


    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your dataset class instance.


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
            "columnIds": [
                column.id for column in self._last_persistent_instance.columns.values()
            ]
            if self._last_persistent_instance and self._last_persistent_instance.columns
            else [],
            "isSearchEnabled": self.is_search_enabled,
            "items": [item.to_synapse_request() for item in self.items]
            if self.items
            else [],
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

    # TODO: Implement this method
    @staticmethod
    async def snapshot_async(
        self,
        comment: str = None,
        label: str = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Any]:
        pass
