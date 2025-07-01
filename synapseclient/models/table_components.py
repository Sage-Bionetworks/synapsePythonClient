import os
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Union

from typing_extensions import Self

from synapseclient import Column as Synapse_Column
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.protocols.table_protocol import ColumnSynchronousProtocol

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


@dataclass
class SumFileSizes:
    sum_file_size_bytes: int
    """The sum of the file size in bytes."""

    greater_than: bool
    """When true, the actual sum of the files sizes is greater than the value provided
    with 'sum_file_size_bytes'. When false, the actual sum of the files sizes is equals
    the value provided with 'sum_file_size_bytes'"""


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
    updated. Use mask = 0x80 to include in the bundle. This is returned in the
    ISO8601 format like `2000-01-01T00:00:00.000Z`."""


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
class SnapshotRequest:
    """A request that defines the options available when creating a snapshot of a table or view.
    Follows the model defined in
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SnapshotRequest.html>

    All attributes are optional.
    """

    comment: Optional[str] = None
    label: Optional[str] = None
    activity: Optional[str] = None

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        return {
            "snapshotComment": self.comment,
            "snapshotLabel": self.label,
            "snapshotActivityId": self.activity,
        }

    def fill_from_dict(self, synapse_response: Dict[str, str]) -> "Self":
        """Converts a response from the REST API into this dataclass."""
        self.comment = synapse_response.get("snapshotComment", None)
        self.label = synapse_response.get("snapshotLabel", None)
        self.activity = synapse_response.get("snapshotActivityId", None)
        return self


@dataclass
class TableUpdateTransaction(AsynchronousCommunicator):
    """
    A request to update a table. This is used to update a table with a set of changes.

    After calling the `send_job_and_wait_async` method the `results` attribute will be
    filled in based off <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateTransactionResponse.html>.
    """

    entity_id: str
    concrete_type: str = concrete_types.TABLE_UPDATE_TRANSACTION_REQUEST
    create_snapshot: bool = False
    changes: Optional[
        List[
            Union[
                TableSchemaChangeRequest, UploadToTableRequest, AppendableRowSetRequest
            ]
        ]
    ] = None
    snapshot_options: Optional[SnapshotRequest] = None
    results: Optional[List[Dict[str, Any]]] = None
    snapshot_version_number: Optional[int] = None
    entities_with_changes_applied: Optional[List[str]] = None

    """This will be an array of
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateResponse.html>."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        request = {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "createSnapshot": self.create_snapshot,
        }

        if self.changes:
            request["changes"] = [
                change.to_synapse_request() for change in self.changes
            ]
        if self.snapshot_options:
            request["snapshotOptions"] = self.snapshot_options.to_synapse_request()

        return request

    def fill_from_dict(self, synapse_response: Dict[str, str]) -> "Self":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response from the REST API that matches <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/TableUpdateTransactionResponse.html>

        Returns:
            An instance of this class.
        """
        self.results = synapse_response.get("results", None)
        self.snapshot_version_number = synapse_response.get(
            "snapshotVersionNumber", None
        )

        if "results" in synapse_response:
            successful_entities = []
            for result in synapse_response["results"]:
                if "updateResults" in result:
                    for update_result in result["updateResults"]:
                        failure_code = update_result.get("failureCode", None)
                        failure_message = update_result.get("failureMessage", None)
                        entity_id = update_result.get("entityId", None)
                        if not failure_code and not failure_message and entity_id:
                            successful_entities.append(entity_id)
            if successful_entities:
                self.entities_with_changes_applied = successful_entities
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
        if self.column_type and isinstance(self.column_type, str):
            self.column_type = ColumnType(self.column_type)

        if self.facet_type and isinstance(self.facet_type, str):
            self.facet_type = FacetType(self.facet_type)

        result = {
            "name": self.name,
            "columnType": self.column_type.value if self.column_type else None,
            "jsonPath": self.json_path,
            "facetType": self.facet_type.value if self.facet_type else None,
        }
        delete_none_keys(result)
        return result


@dataclass
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
        # TODO: This needs to be converted to its Dataclass. It also needs to be tested to verify conversion.
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
        if self.column_type and isinstance(self.column_type, str):
            self.column_type = ColumnType(self.column_type)

        if self.facet_type and isinstance(self.facet_type, str):
            self.facet_type = FacetType(self.facet_type)
        result = {
            "concreteType": concrete_types.COLUMN_MODEL,
            "name": self.name,
            "columnType": self.column_type.value if self.column_type else None,
            "facetType": self.facet_type.value if self.facet_type else None,
            "defaultValue": self.default_value,
            "maximumSize": self.maximum_size,
            "maximumListLength": self.maximum_list_length,
            "enumValues": self.enum_values,
            "jsonSubColumns": (
                [
                    sub_column.to_synapse_request()
                    for sub_column in self.json_sub_columns
                ]
                if self.json_sub_columns
                else None
            ),
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
