import asyncio
import dataclasses
import json
import logging
import os
import tempfile
import uuid
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pandas.api.types import infer_dtype

from synapseclient import Column as Synapse_Column
from synapseclient import Synapse
from synapseclient import Table as Synapse_Table
from synapseclient.api import (
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
    delete_none_keys,
    log_dataclass_diff,
    merge_dataclass_entities,
)
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.protocols.table_protocol import (
    ColumnSynchronousProtocol,
    TableSynchronousProtocol,
)
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    FailureStrategy,
    store_entity_components,
)
from synapseclient.table import delete_rows

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
    Switching between types (using a transaction with TableUpdateTransactionRequest
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
    Switching between types (using a transaction with TableUpdateTransactionRequest
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
    Switching between types (using a transaction with TableUpdateTransactionRequest
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
        self._last_persistent_instance = dataclasses.replace(self)
        self._last_persistent_instance.json_sub_columns = (
            dataclasses.replace(self.json_sub_columns)
            if self.json_sub_columns
            else None
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
    """Enum used"""

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


@dataclass()
@async_to_sync
class Table(TableSynchronousProtocol, AccessControllable):
    """A Table represents the metadata of a table.

    Attributes:
        id: The unique immutable ID for this table. A new ID will be generated for new
            Tables. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this table. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        parent_id: The ID of the Entity that is the parent of this table.
        columns: The columns of this table. This is an ordered dictionary where the key is the
            name of the column and the value is the Column object. When creating a new instance
            of a Table object you may pass any of the following types as the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the Column object
            - An OrderedDict where the key is the name of the column and the value is the Column object

            After the Table object is created the columns attribute will be an OrderedDict. If
            you wish to replace the columns after the Table is constructed you may do so by
            calling the `.set_columns()` method. For example:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Column, ColumnType, Table

            syn = Synapse()
            syn.login()

            # Initialize the table with a list of columns
            table = Table(name="my_table", columns=[Column(name="my_column", column_type=ColumnType.STRING)])

            # Replace the columns with a different list of columns
            table.set_columns(columns=[Column(name="my_new_column", column_type=ColumnType.STRING)])
            table.store()
            ```


            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your table class instance.


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

    Example: Create a table with columns
        This example shows how you may create a new table with a list of columns.

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

    Example: Rename an existing column
        This examples shows how you may retrieve a table from synapse, rename a column,
        and then store the table back in synapse.

            from synapseclient import Synapse
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            table = Table(
                name="my_table",
                parent_id="syn1234",
            ).get(include_columns=True)

            # You may also get the table by id:
            table = Table(
                id="syn4567"
            ).get(include_columns=True)

            table.columns["my_old_column"].name = "my_new_column"

            # Before the data is stored in synapse you'll still be able to use the old key to access the column entry
            print(table.columns["my_old_column"])

            table.store()

            # After the data is stored in synapse you'll be able to use the new key to access the column entry
            print(table.columns["my_new_column"])

    Example: Create a table with a list of columns
        A list of columns may be passed in when creating a new table. The order of the
        columns in the list will be the order they are stored in Synapse. If the table
        already exists and you create the Table instance in this way the columns will
        be appended to the end of the existing columns.

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


    Example: Creating a table with a dictionary of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

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

    Example: Creating a table with an OrderedDict of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

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
    """

    id: Optional[str] = None
    """The unique immutable ID for this table. A new ID will be generated for new
    Tables. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this table. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
    apostrophes, and parentheses"""

    description: Optional[str] = None

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

    After the Table object is created the columns attribute will be an OrderedDict. If
    you wish to replace the columns after the Table is constructed you may do so by
    calling the `.set_columns()` method. For example:

    ```python
    from synapseclient import Synapse
    from synapseclient.models import Column, ColumnType, Table

    syn = Synapse()
    syn.login()

    # Initialize the table with a list of columns
    table = Table(name="my_table", columns=[Column(name="my_column", column_type=ColumnType.STRING)])

    # Replace the columns with a different list of columns
    table.set_columns(columns=[Column(name="my_new_column", column_type=ColumnType.STRING)])
    table.store()
    ```


    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your table class instance.


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

    @staticmethod
    def _convert_columns_to_ordered_dict(
        columns: Union[List[Column], OrderedDict[str, Column], Dict[str, Column], None]
    ) -> OrderedDict[str, Column]:
        """Converts the columns attribute to an OrderedDict if it is a list or dict."""
        if not columns:
            return OrderedDict()

        if isinstance(columns, list):
            return OrderedDict((column.name, column) for column in columns)
        elif isinstance(columns, dict):
            results = OrderedDict()
            for key, column in columns.items():
                if column.name:
                    results[column.name] = column
                else:
                    column.name = key
                    results[key] = column
            return results
        elif isinstance(columns, OrderedDict):
            results = OrderedDict()
            for key, column in columns.items():
                if column.name:
                    results[column.name] = column
                else:
                    column.name = key
                    results[key] = column
            return results

        else:
            raise ValueError("columns must be a list, dict, or OrderedDict")

    def __post_init__(self):
        """Post initialization of the Table object. This is used to set the columns
        attribute to an OrderedDict if it is a list or dict."""
        self.columns = Table._convert_columns_to_ordered_dict(columns=self.columns)

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
        self, synapse_table: Synapse_Table, set_annotations: bool = True
    ) -> "Table":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            synapse_table: The data coming from the Synapse API

        Returns:
            The Table object instance.
        """
        self.id = synapse_table.get("id", None)
        self.name = synapse_table.get("name", None)
        self.description = synapse_table.get("description", None)
        self.parent_id = synapse_table.get("parentId", None)
        self.etag = synapse_table.get("etag", None)
        self.created_on = synapse_table.get("createdOn", None)
        self.created_by = synapse_table.get("createdBy", None)
        self.modified_on = synapse_table.get("modifiedOn", None)
        self.modified_by = synapse_table.get("modifiedBy", None)
        self.version_number = synapse_table.get("versionNumber", None)
        self.version_label = synapse_table.get("versionLabel", None)
        self.version_comment = synapse_table.get("versionComment", None)
        self.is_latest_version = synapse_table.get("isLatestVersion", None)
        self.is_search_enabled = synapse_table.get("isSearchEnabled", False)

        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_table.get("annotations", {})
            )
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

    async def upsert_rows_async(
        self,
        values: pd.DataFrame,
        primary_keys: List[str],
        dry_run: bool = False,
        *,
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

        - The number of rows that may be upserted in a single call should be
            limited. Additional work is planned to support batching
            the calls automatically for you.
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

                - A string holding the path to a CSV file. Tthe data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table

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


            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    |      | 1    |
            | B    | 2    |      |

        """
        if not self._last_persistent_instance:
            await self.get_async(include_columns=True, synapse_client=synapse_client)
        if not self.columns:
            raise ValueError(
                "There are no columns on this table. Unable to proceed with an upsert operation."
            )

        if isinstance(values, dict):
            values = pd.DataFrame(values)
        elif isinstance(values, str):
            values = csv_to_pandas_df(filepath=values, **kwargs)
        elif isinstance(values, pd.DataFrame) or isinstance(values, str):
            values = values.copy()
        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

        all_columns_from_df = [f'"{column}"' for column in values.columns]

        # TODO: Chunk this up into smaller queries. Running this when upserting 1 million rows causes an HTTP 403 to be raised from Synapse
        select_statement = (
            f"SELECT ROW_ID, {', '.join(all_columns_from_df)} FROM {self.id} WHERE "
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
                    f"'{value}'" for value in values[upsert_column] if value is not None
                ]

            elif column_model.column_type == ColumnType.BOOLEAN:
                include_true = False
                include_false = False
                for value in values[upsert_column]:
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
                    str(value) for value in values[upsert_column] if value is not None
                ]
            if not values_for_where_statement:
                continue
            where_statements.append(
                f"\"{upsert_column}\" IN ({', '.join(values_for_where_statement)})"
            )

        where_statement = " AND ".join(where_statements)
        select_statement += where_statement

        results = await Table.query_async(
            query=select_statement, synapse_client=synapse_client
        )
        partial_changes_objects_to_rename: List[PartialRow] = []
        indexs_of_original_df_with_changes = []
        for _, row in results.iterrows():
            row_id = row["ROW_ID"]
            # TODO: When upserting rows in table types that have an etag this will be required. In a normal "Table" this is not a column that can be selected, but this is applicable in something like a "FileView"
            # row_etag = row["ROW_ETAG"]
            partial_change_values = {}

            # Find the matching row in `values` that matches the row in `results` for the primary_keys
            matching_conditions = values[primary_keys[0]] == row[primary_keys[0]]
            for col in primary_keys[1:]:
                matching_conditions &= values[col] == row[col]
            matching_row = values.loc[matching_conditions]

            # Determines which cells need to be updated
            for column in values.columns:
                if len(matching_row[column].values) > 1:
                    raise ValueError(
                        f"The values for the keys being upserted must be unique in the table: [{matching_row}]"
                    )

                if len(matching_row[column].values) == 0:
                    continue
                column_id = self.columns[column].id
                column_type = self.columns[column].column_type
                cell_value = matching_row[column].values[0]
                if cell_value != row[column]:
                    if (
                        isinstance(cell_value, list) and len(cell_value) > 0
                    ) or not pd.isna(cell_value):
                        partial_change_values[
                            column_id
                        ] = _convert_pandas_row_to_python_types(
                            cell=cell_value, column_type=column_type
                        )
                    else:
                        partial_change_values[column_id] = None

            if partial_change_values != {}:
                # TODO: When upserting rows in table types that have an etag this will be required. In a normal "Table" this is not a column that can be selected, but this is applicable in something like a "FileView"
                # partial_chage = PartialRow(row_id=row_id, values=partial_change_values, etag=row_etag)
                partial_chage = PartialRow(
                    row_id=row_id,
                    values=[
                        {"key": partial_change_key, "value": partial_change_value}
                        for partial_change_key, partial_change_value in partial_change_values.items()
                    ],
                )
                partial_changes_objects_to_rename.append(partial_chage)
                indexs_of_original_df_with_changes.append(matching_row.index[0])

        rows_to_insert_df = values.loc[
            ~values.index.isin(indexs_of_original_df_with_changes)
        ]

        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.info(
            f"[{self.id}:{self.name}]: Found {len(partial_changes_objects_to_rename)}"
            f" rows to update and {len(rows_to_insert_df)} rows to insert"
        )

        if client.logger.isEnabledFor(logging.DEBUG):
            client.logger.debug(
                f"[{self.id}:{self.name}]: Rows to update: {partial_changes_objects_to_rename}"
            )
            client.logger.debug(
                f"[{self.id}:{self.name}]: Rows to insert: {rows_to_insert_df}"
            )

        if dry_run:
            return

        if partial_changes_objects_to_rename:
            partial_row_set = PartialRowSet(
                table_id=self.id, rows=partial_changes_objects_to_rename
            )
            appendable_rowset_request = AppendableRowSetRequest(
                entity_id=self.id, to_append=partial_row_set
            )
            # TODO: Convert this over to use the `AsynchronousCommunicator` mixin when available (https://github.com/Sage-Bionetworks/synapsePythonClient/pull/1152)
            # TODO: Look into making a change here that allows the `TableUpdateTransactionRequest` to also execute any inserts to the table within the same transaction. Currently data will be upserted first, and then inserted.
            uri = f"/entity/{self.id}/table/transaction/async"
            transaction_request = TableUpdateTransactionRequest(
                entity_id=self.id, changes=[appendable_rowset_request]
            )
            client._waitForAsync(
                uri=uri, request=transaction_request.to_synapse_request()
            )

        if not rows_to_insert_df.empty:
            await self.store_rows_async(
                values=rows_to_insert_df, synapse_client=synapse_client
            )

    # TODO: Polish docstring
    async def store_rows_async(
        self,
        values: Union[str, Dict[str, Any], pd.DataFrame],
        schema_storage_strategy: SchemaStorageStrategy = None,
        column_expansion_strategy: ColumnExpansionStrategy = None,
        dry_run: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        **kwargs,
    ) -> None:
        """
        Add or update rows in Synapse from the sources defined below. This method
        works on a full row replacement in the case of an update. What this means is
        that you may not do a partial update of a row. If you want to update a row
        you must pass in all the data for that row, or the data for the columns not
        provided will be set to null.

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

                - A string holding the path to a CSV file. If the `schema_storage_strategy` is set to `None` the data will be uploaded as is. If `schema_storage_strategy` is set to `INFER_FROM_DATA` the data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table

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

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 1    | 1    |
            | B    | 2    | 2    |
            | C    | 3    | 3    |

        Example: Inserting rows into a table that does not have columns
            This example shows how you may insert rows into a table that does not have
            columns. The columns will be inferred from the data that is being stored.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table, SchemaStorageStrategy

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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table, SchemaStorageStrategy

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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table, query_async

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

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 22   | 1    |
            | B    | 2    | 33   |
            | C    | 3    | 3    |

        """
        original_values = values
        if isinstance(values, dict):
            values = pd.DataFrame(values)
        elif (
            isinstance(values, str)
            and schema_storage_strategy == SchemaStorageStrategy.INFER_FROM_DATA
        ):
            values = csv_to_pandas_df(filepath=values, **kwargs)
        elif isinstance(values, pd.DataFrame) or isinstance(values, str):
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
                existing_entity := await Table(id=existing_id).get_async(
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

        # TODO: This portion of the code should be updated to support uploading a file from memory using BytesIO (Ticket to be created)
        if isinstance(original_values, str):
            file_handle_id = await multipart_upload_file_async(
                syn=client, file_path=original_values, content_type="text/csv"
            )
            upload_request = UploadToTableRequest(
                table_id=self.id, upload_file_handle_id=file_handle_id, update_etag=None
            )
            uri = f"/entity/{self.id}/table/transaction/async"
            changes = []
            if schema_change_request:
                changes.append(schema_change_request)
            changes.append(upload_request)
            # TODO: Convert this over to use the `AsynchronousCommunicator` mixin when available (https://github.com/Sage-Bionetworks/synapsePythonClient/pull/1152)
            transaction_request = TableUpdateTransactionRequest(
                entity_id=self.id, changes=changes
            )
            client._waitForAsync(
                uri=uri, request=transaction_request.to_synapse_request()
            )
        elif isinstance(values, pd.DataFrame):
            # TODO: Remove file after upload
            filepath = f"{tempfile.mkdtemp()}/{self.id}_upload_{uuid.uuid4()}.csv"
            # TODO: Support everything from `from_data_frame` to_csv call
            values.to_csv(filepath, index=False)
            file_handle_id = await multipart_upload_file_async(
                syn=client, file_path=filepath, content_type="text/csv"
            )
            upload_request = UploadToTableRequest(
                table_id=self.id, upload_file_handle_id=file_handle_id, update_etag=None
            )
            uri = f"/entity/{self.id}/table/transaction/async"
            # TODO: Convert this over to use the `AsynchronousCommunicator` mixin when available (https://github.com/Sage-Bionetworks/synapsePythonClient/pull/1152)
            changes = []
            if schema_change_request:
                changes.append(schema_change_request)
            changes.append(upload_request)
            transaction_request = TableUpdateTransactionRequest(
                entity_id=self.id, changes=changes
            )
            client._waitForAsync(
                uri=uri, request=transaction_request.to_synapse_request()
            )
        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

    async def delete_rows_async(
        self, query: str, *, synapse_client: Optional[Synapse] = None
    ) -> pd.DataFrame:
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

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                async def main():
                    await Table(id="syn1234").delete_rows_async(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo = 'asdf'")

                asyncio.run(main())
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        results_from_query = await Table.query_async(query=query, synapse_client=client)
        client.logger.info(
            f"Found {len(results_from_query)} rows to delete for given query: {query}"
        )

        rows_to_delete = []
        for row in results_from_query:
            rows_to_delete.append([row.row_id, row.version_number])
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: delete_rows(
                syn=Synapse.get_client(synapse_client=synapse_client),
                table_id=self.id,
                row_id_vers_list=rows_to_delete,
            ),
        )
        return results_from_query

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
        method_to_trace_name=lambda self, **kwargs: f"Table_Store: {self.name}"
    )
    async def store_async(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
    ) -> "Table":
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
                existing_entity := await Table(id=existing_id).get_async(
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
                        f"[{self.id}:{self.name}]: Dry run table update, expected changes:"
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
                    self.fill_from_dict(
                        synapse_table=entity["entity"], set_annotations=False
                    )
            else:
                if dry_run:
                    client.logger.info(
                        f"[{self.id}:{self.name}]: Dry run table update, expected changes:"
                    )
                    log_dataclass_diff(
                        logger=client.logger,
                        prefix=f"[{self.name}]: ",
                        obj1=Table(),
                        obj2=self,
                        fields_to_ignore=["columns", "_last_persistent_instance"],
                    )
                else:
                    entity = await post_entity_bundle2_create(
                        request=self.to_synapse_request(), synapse_client=synapse_client
                    )
                    self.fill_from_dict(
                        synapse_table=entity["entity"], set_annotations=False
                    )

        schema_change_request = await self._generate_schema_change_request(
            dry_run=dry_run, synapse_client=synapse_client
        )

        if dry_run:
            return self

        if schema_change_request:
            uri = f"/entity/{self.id}/table/transaction/async"
            transaction_request = TableUpdateTransactionRequest(
                entity_id=self.id, changes=[schema_change_request]
            )
            # TODO: Convert this over to use the `AsynchronousCommunicator` mixin when available (https://github.com/Sage-Bionetworks/synapsePythonClient/pull/1152)
            client._waitForAsync(
                uri=uri, request=transaction_request.to_synapse_request()
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

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Table_Get: {self.name}"
    )
    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Table":
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

        Example: Getting metadata about a table using name and parent_id
            Get a table by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get_async` call, then you'll make the changes,
            and finally call the `.store_async()` method.

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
        method_to_trace_name=lambda self, **kwargs: f"Table_Delete: {self.name}"
    )
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the table from synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a table
            Deleting a table is only supported by the ID of the table.

                import asyncio
                from synapseclient import Synapse

                syn = Synapse()
                syn.login()

                async def main():
                    await Table(id="syn4567").delete_async()

                asyncio.run(main())
        """
        if not (self.id or (self.name and self.parent_id)):
            raise ValueError(
                "The table must have an id or a " "(name and `parent_id`) set."
            )

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                obj=entity_id
            ),
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

                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                table = Table(
                    id="syn1234"
                ).get(include_columns=True)

                table.delete_column(name="my_column")
                table.store()

        Example: Deleting a column (async)
            This example shows how you may delete a column from a table and then store
            the change back in Synapse.

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
        self, column: Union[Column, List[Column]], index: int = None
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


        Example: Adding multiple columns
            This example shows how you may add multiple columns to a table and then store
            the change back in Synapse.

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

        Example: Adding a column at a specific index
            This example shows how you may add a column at a specific index to a table
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

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

        Example: Adding a single column (async)
            This example shows how you may add a single column to a table and then store
            the change back in Synapse.

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
                    table.store_async()

                asyncio.run(main())

        Example: Adding multiple columns (async)
            This example shows how you may add multiple columns to a table and then store
            the change back in Synapse.

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
                    table.store_async()

                asyncio.run(main())

        Example: Adding a column at a specific index (async)
            This example shows how you may add a column at a specific index to a table
            and then store the change back in Synapse. If the index is out of bounds the
            column will be added to the end of the list.

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
                    table.store_async()

                asyncio.run(main())
        """
        if not self._last_persistent_instance:
            raise ValueError(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            )
        if index is not None:
            raise NotImplementedError("Index is not yet implemented")

        if isinstance(column, list):
            for col in column:
                self.columns[col.name] = col
        else:
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


        Example: Reordering a column (async)
            This example shows how you may reorder a column in a table and then store
            the change back in Synapse.

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

    def set_columns(
        self, columns: Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ) -> None:
        """
        Helper function to set the columns attribute with a number of columns. This
        will convert the columns attribute to an OrderedDict if it is a list or dict. It
        is meant as a convenience function to eliminate the need to manually convert the
        columns attribute to an OrderedDict ahead of time.

        **Warning**: This will act as a destructive operation if your Table class
        instance has interacted with Synapse via a `.get()` or `.store()` operation.
        The columns you pass in will replace all columns in the table with the columns
        in the list.


        If you'd like to rename a column you should change the name attribute of the
        Column object rather than call this function. The next time you store the table
        the column will be updated in Synapse with the new name and the key in the
        `.columns` OrderedDict will be updated. See the example labeled
        `Rename an existing column` on the Table class for more information.

        Arguments:
            columns: The new columns to replace the existing columns with. This may be:

                - A list of Column objects
                - A dictionary where the key is the column name and the value is the Column object
                - An OrderedDict where the key is the column name and the value is the Column object.

        Returns:
            None

        Example: Replacing all columns with a list of columns
            .

                from synapseclient import Synapse
                from synapseclient.models import Column, ColumnType, Table

                syn = Synapse()
                syn.login()

                table = Table(
                    id="syn1234"
                ).get(include_columns=True)

                columns = [
                    Column(name="my_string_column", column_type=ColumnType.STRING),
                    Column(name="my_integer_column", column_type=ColumnType.INTEGER),
                    Column(name="my_double_column", column_type=ColumnType.DOUBLE),
                    Column(name="my_boolean_column", column_type=ColumnType.BOOLEAN),
                ]

                table.set_columns(columns=columns)
                table.store()

        Example: Replacing all columns with a dictionary of columns
            When specifying a number of columns via a dict setting the `name` attribute
            on the `Column` object is optional. When it is not specified it will be
            pulled from the key of the dict.

                from synapseclient import Synapse
                from synapseclient.models import Column, ColumnType, Table

                syn = Synapse()
                syn.login()

                table = Table(
                    id="syn1234"
                ).get(include_columns=True)

                columns = {
                    "my_string_column": Column(column_type=ColumnType.STRING),
                    "my_integer_column": Column(column_type=ColumnType.INTEGER),
                    "my_double_column": Column(column_type=ColumnType.DOUBLE),
                    "my_boolean_column": Column(column_type=ColumnType.BOOLEAN),
                }

                table.set_columns(columns=columns)
                table.store()

        Example: Replacing all columns with an OrderedDict of columns
            .

                from synapseclient import Synapse
                from synapseclient.models import Column, ColumnType, Table

                syn = Synapse()
                syn.login()

                table = Table(
                    id="syn1234"
                ).get(include_columns=True)

                columns = OrderedDict({
                    "my_string_column": Column(column_type=ColumnType.STRING),
                    "my_integer_column": Column(column_type=ColumnType.INTEGER),
                    "my_double_column": Column(column_type=ColumnType.DOUBLE),
                    "my_boolean_column": Column(column_type=ColumnType.BOOLEAN),
                })

                table.set_columns(columns=columns)
                table.store()
        """
        if not self._last_persistent_instance:
            raise ValueError(
                "This method is only supported after interacting with Synapse via a `.get()` or `.store()` operation"
            )

        self.columns = Table._convert_columns_to_ordered_dict(columns=columns)

    @staticmethod
    async def query_async(
        query: str,
        # TODO: Implement part_mask - Should this go into a different method because getting this information while downloading as a CSV is not supported?
        part_mask: str = None,
        include_row_id_and_row_version: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> pd.DataFrame:
        """Query for data on a table stored in Synapse. The results will always be
        returned as a Pandas DataFrame.

        Arguments:
            query: The query to run. The query must be valid syntax that Synapse can
                understand. See this document that describes the expected syntax of the
                query:
                <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html>
            part_mask: Still determining how this will be implemented - Ignore for now
            include_row_id_and_row_version: If True the `ROW_ID` and `ROW_VERSION`
                columns will be returned in the DataFrame. These columns are required
                if using the query results to update rows in the table. These columns
                are the primary keys used by Synapse to uniquely identify rows in the
                table.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.

        Example: Querying for data
            This example shows how you may query for data in a table and print out the
            results.

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                async def main():
                    results = await Table.query_async(query="SELECT * FROM syn1234")
                    print(results)

                asyncio.run(main())
        """
        # TODO: Implement similar logic to synapseclient/table.py::CsvFileTable::from_table_query and what it can handle
        # It should handle for any of the arguments available there in order to support correctly reading in the CSV
        # TODO: Additionally - the logic present in synapseclient/table.py::CsvFileTable::asDataFrame should be considered and implemented as well
        # TODO: Lastly - When a query is executed both single-threaded and multi-threaded downloads of the CSV result should not write a file to disk, instead write the bytes to a BytesIO object and then use that object instead of a filepath
        # TODO: Also support writing the CSV to disk if the user wants to do that
        loop = asyncio.get_event_loop()

        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.info(f"Running query: {query}")

        # TODO: Implementation should not download as CSV, but left as a placeholder for now
        results = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).tableQuery(
                query=query,
                includeRowIdAndRowVersion=include_row_id_and_row_version,
            ),
        )
        return results.asDataFrame(rowIdAndVersionInIndex=False)

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

        Example: Including the activity (Provenance) in the snapshot and not pulling it forward to the new `in-progress` version of the table.
            By default this method is set up to include the activity in the snapshot and
            then pull the activity forward to the new version. If you do not want to
            include the activity in the snapshot you can set `include_activity` to
            False. If you do not want to pull the activity forward to the new version
            you can set `associate_activity_to_new_version` to False.

            See the [activity][synapseclient.models.Activity] attribute on the Table
            class for more information on how to interact with the activity.

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
        assert (
            self.new_column_id is not None
        ), "Columns should not be removed unless explictly calling the `delete_column` method on the Table instance. If you are encountering this please report a bug to"
        return {
            "concreteType": self.concrete_type,
            "oldColumnId": self.old_column_id,
            "newColumnId": self.new_column_id,
        }


@dataclass
class TableSchemaChangeRequest:
    """
    A request to change the schema of a table. This is used to change the columns in a
    table. This request is used in the `TableUpdateTransactionRequest` to indicate what
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
class PartialRow:
    """
    A partial row to be added to a table. This is used in the `PartialRowSet` to
    indicate what rows should be updated in a table during the upsert operation.
    """

    row_id: str
    values: Dict[str, Any]
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
class AppendableRowSetRequest:
    """
    A request to append rows to a table. This is used to append rows to a table. This
    request is used in the `TableUpdateTransactionRequest` to indicate what rows should
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
    file into a table. This request is used in the `TableUpdateTransactionRequest`.
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
class TableUpdateTransactionRequest:
    """
    A request to update a table. This is used to update a table with a set of changes.
    """

    entity_id: str
    changes: List[Union[TableSchemaChangeRequest, UploadToTableRequest]]
    concrete_type: str = concrete_types.TABLE_UPDATE_TRANSACTION_REQUEST

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "changes": [change.to_synapse_request() for change in self.changes],
        }


def infer_column_type_from_data(values: pd.DataFrame) -> List[Column]:
    """
    Return a list of Synapse table [Column][synapseclient.models.table.Column] objects
    that correspond to the columns in the given values.

    Arguments:
        values: An object that holds the content of the tables. It must be a
            [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

    Returns:
        A list of Synapse table [Column][synapseclient.table.Column] objects

    Example:

        import pandas as pd

        df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
        cols = infer_column_type_from_data(df)
    """

    if isinstance(values, pd.DataFrame):
        df = values
    else:
        raise ValueError(
            "Values of type %s is not supported. It must be a pandas DataFrame"
            % type(values)
        )

    cols = list()
    for col in df:
        inferred_type = infer_dtype(df[col], skipna=True)
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
    df: pd.DataFrame, date_columns: List
) -> pd.DataFrame:
    """
    Convert date columns with epoch time to date time in UTC timezone

    Argumenets:
        df: a pandas dataframe
        date_columns: name of date columns

    Returns:
        A dataframe with epoch time converted to date time in UTC timezone
    """
    import numpy as np

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
        lambda x: pd.to_datetime(x, unit="ms", utc=True)
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
    df = pd.read_csv(
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
    cell: Union[pd.Series, str, List], column_type: ColumnType
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
