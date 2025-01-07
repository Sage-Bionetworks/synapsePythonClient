import asyncio
import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from opentelemetry import trace

from synapseclient import Column as Synapse_Column
from synapseclient import Schema as Synapse_Schema
from synapseclient import Synapse
from synapseclient import Table as Synapse_Table
from synapseclient.api import get_columns
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.utils import merge_dataclass_entities
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.protocols.table_protocol import (
    ColumnSynchronousProtocol,
    TableSynchronousProtocol,
)
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)
from synapseclient.table import delete_rows


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


@dataclass
class RowsetResultFormat:
    """Rowset result format options."""

    limit: Optional[int] = None
    """specify the maximum number of rows to be returned, defaults to None"""

    offset: Optional[int] = None

    def to_dict(self):
        """Converts the RowsetResultFormat into a dictionary that can be passed
        into the synapseclient."""

        return {
            "resultsAs": "rowset",
            "limit": self.limit,
            "offset": self.offset,
        }


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


@dataclass()
class Row:
    # TODO: We will want to restrict the typing here
    values: Optional[List[Any]] = None
    """The values for each column of this row."""

    row_id: Optional[int] = None
    """The immutable ID issued to a new row."""

    version_number: Optional[int] = None
    """The version number of this row. Each row version is immutable, so when a
    row is updated a new version is created."""


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

    def fill_from_dict(self, synapse_column: Synapse_Column) -> "Column":
        """Converts a response from the synapseclient into this dataclass."""
        self.id = synapse_column.get("id", None)
        self.name = synapse_column.get("name", None)
        self.column_type = synapse_column.get("columnType", None)
        self.facet_type = synapse_column.get("facetType", None)
        self.default_value = synapse_column.get("defaultValue", None)
        self.maximum_size = synapse_column.get("maximumSize", None)
        self.maximum_list_length = synapse_column.get("maximumListLength", None)
        self.enum_values = synapse_column.get("enumValues", None)
        self.json_sub_columns = synapse_column.get("jsonSubColumns", None)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Column_Store: {self.name}"
    )
    async def store_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "Column":
        """Persist the column to Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Column instance stored in synapse.
        """
        # TODO - Update the storage of columns to go through this API: https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableSchemaChangeRequest.html
        # TODO: As of right now since we are not using this API the columns do not maintain the data that is stored in them. This is a limitation of the current implementation.

        # Call synapse
        loop = asyncio.get_event_loop()
        entity = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).createColumn(
                name=self.name,
                columnType=self.column_type,
            ),
        )
        print(entity)
        self.fill_from_dict(entity)

        print(f"Stored column {self.name}, id: {self.id}")

        return self


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
        version_number: The version number issued to this version on the object.
        version_label: The version label for this table
        version_comment: The version comment for this table
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
        using the pandas function `infer_dtype`. If you need a more specific column
        type, or need to add options to the colums follow the examples below.

            import pandas as pd

            from synapseclient import Synapse
            from synapseclient.models import Table

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
            )

            # The call to `store_rows` will also call `.store()` on the table too,
            # meaning if the table does not exist it will be created.
            table.store_rows(values=my_data)

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

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this table."""

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict)
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

    # TODO: This is not used at the moment, but will be when swapping to use this api: <https://rest-docs.synapse.org/rest/POST/entity/id/table/transaction/async/start.html>
    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)

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
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this table"""

    version_comment: Optional[str] = None
    """The version comment for this table"""

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

    # TODO: Finish implementation
    async def store_rows_async(
        self,
        values: Union[str, List[Dict[str, Any]], Dict[str, Any], pd.DataFrame],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Takes in values from the sources defined below and stores the rows to Synapse.

        Arguments:
            values: Supports storing data from the following sources:

                - A string holding the path to a CSV file
                - A list of lists (or tuples) where each element is a row
                - A dictionary where the key is the column name and the value is one or more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe).
                - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        client.logger.info(
            f"Checking for changes to the schema of table: {self.name or self.id}"
        )
        await self.store_async(synapse_client=synapse_client)

        client.logger.info(f"Storing rows for table {self.name or self.id}")

        if isinstance(values, (list, tuple)):
            # Current functionality uses this to convert the values over
            # return CsvFileTable.from_list_of_rows(schema, values, **kwargs)
            client.logger.info(f"Storing rows for table {self.name or self.id} as list")
        elif isinstance(values, str):
            client.logger.info(
                f"Storing rows for table {self.name or self.id} as path to CSV"
            )
        elif isinstance(values, pd.DataFrame):
            # Current functionality uses this to convert the values over
            # return CsvFileTable.from_data_frame(schema, values, **kwargs)
            client.logger.info(
                f"Storing rows for table {self.name or self.id} as DataFrame"
            )

        # dict
        elif isinstance(values, dict):
            # Current functionality uses this to convert the values over
            # return CsvFileTable.from_data_frame(schema, pd.DataFrame(values), **kwargs)
            client.logger.info(f"Storing rows for table {self.name or self.id} as dict")

        else:
            raise ValueError(
                "Don't know how to make tables from values of type %s." % type(values)
            )

        raise NotImplementedError("This method is not yet implemented")

    # TODO: Refactor this function as it's really rough to work with in it's current implementation
    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Delete_rows: {self.name}"
    )
    async def delete_rows_async(
        self, rows: List[Row], *, synapse_client: Optional[Synapse] = None
    ) -> None:
        """Delete rows from a table.

        Arguments:
            rows: The rows to delete.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        # TODO: Add example of how to delete rows
        """
        rows_to_delete = []
        for row in rows:
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

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Table_Store: {self.name}"
    )
    async def store_async(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
    ) -> "Table":
        """Store non-row information about a table including the columns and annotations.

        Arguments:
            dry_run: If True, will not actually store the table but will return log to
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
            client.logger.info(f"Dry run for table {self.name or self.id}")
            # TODO: Implement and check what columns/attributes are present and differ from what is in Synapse

            raise NotImplementedError("This argument is not yet implemented")

        if not self.has_changed:
            client.logger.info(f"No changes detected for table {self.name or self.id}")

        if self.has_changed:
            # TODO: Swap the storage of the table to be done in a single transaction via: https://rest-docs.synapse.org/rest/POST/entity/id/table/transaction/async/start.html
            tasks = []
            if self.columns:
                tasks.extend(
                    column.store_async(synapse_client=synapse_client)
                    for column in self.columns.values()
                )
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # TODO: Proper exception handling. Similar behavior to the current implementation in File, Folder, Project should be followed
                    for result in results:
                        if isinstance(result, Column):
                            print(f"Stored {result.name}")
                        else:
                            if isinstance(result, BaseException):
                                raise result
                            raise ValueError(f"Unknown type: {type(result)}", result)
                except Exception as ex:
                    Synapse.get_client(synapse_client=synapse_client).logger.exception(
                        ex
                    )
                    print("I hit an exception")

            synapse_schema = Synapse_Schema(
                name=self.name,
                columns=self.columns.values(),
                parent=self.parent_id,
            )
            trace.get_current_span().set_attributes(
                {
                    "synapse.name": self.name or "",
                    "synapse.id": self.id or "",
                }
            )
            loop = asyncio.get_event_loop()
            entity = await loop.run_in_executor(
                None,
                lambda: client.store(obj=synapse_schema),
            )

            self.fill_from_dict(synapse_table=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self, synapse_client=synapse_client
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
        include_columns: bool = False,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Table":
        """Get the metadata about the table from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. When False, the columns will not be filled in.
            include_activity: If True the activity will be included in the file
                if it exists.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Table instance stored in synapse.
        """
        if not self.id and self.name and self.parent_id:
            raise NotImplementedError(
                "This method does not yet support getting by name and parent_id"
            )

        loop = asyncio.get_event_loop()
        entity = await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get(
                entity=self.id
            ),
        )
        self.fill_from_dict(synapse_table=entity, set_annotations=True)

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
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                obj=self.id
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
        """
        # TODO: _columns_to_delete is not used at the moment, it will be used when swapping to use this synapse API: <https://rest-docs.synapse.org/rest/POST/entity/id/table/transaction/async/start.html>
        self._columns_to_delete[name.name] = name
        self.columns.pop(name.name, None)

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
        """
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
        """

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
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> pd.DataFrame:
        """Query for data on a table stored in Synapse. The results will always be
        returned as a Pandas DataFrame.

        Arguments:
            query: The query to run.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.
        """
        # TODO: Implement similar logic to synapseclient/table.py::CsvFileTable::from_table_query and what it can handle
        # It should handle for any of the arguments available there in order to support correctly reading in the CSV
        # TODO: Additionally - the logic present in synapseclient/table.py::CsvFileTable::asDataFrame should be considered and implemented as well
        # TODO: Lastly - When a query is executed both single-threaded and multi-threaded downloads of the CSV result should not write a file to disk, instead write the bytes to a BytesIO object and then use that object instead of a filepath
        # TODO: Also support writing the CSV to disk if the user wants to do that
        raise NotImplementedError("This method is not yet implemented")
