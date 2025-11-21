import json
import os
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import Self

from synapseclient import Column as Synapse_Column
from synapseclient.core.async_utils import async_to_sync, skip_async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.constants.concrete_types import (
    QUERY_BUNDLE_REQUEST,
    QUERY_RESULT,
    QUERY_TABLE_CSV_REQUEST,
    QUERY_TABLE_CSV_RESULT,
)
from synapseclient.core.utils import delete_none_keys, from_unix_epoch_time
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.protocols.table_protocol import ColumnSynchronousProtocol

if TYPE_CHECKING:
    from synapseclient import Synapse

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


@dataclass
class SumFileSizes:
    """
    A model for the sum of file sizes in a query result bundle.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SumFileSizes.html>
    """

    sum_file_size_bytes: int = None
    """The sum of the file size in bytes."""
    greater_than: bool = None
    """When true, the actual sum of the files sizes is greater than the value provided with 'sumFileSizesBytes'. When false, the actual sum of the files sizes is equals the value provided with 'sumFileSizesBytes'"""


@dataclass
class QueryResultOutput:
    """
    The result of querying Synapse with an included `part_mask`. This class contains a
    subnet of the available items that may be returned by specifying a `part_mask`.
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

    @classmethod
    def fill_from_dict(
        cls, result: "DATA_FRAME_TYPE", data: Dict[str, Any]
    ) -> "QueryResultOutput":
        """
        Create a QueryResultOutput from a result DataFrame and dictionary response.

        Arguments:
            result: The pandas DataFrame result from the query.
            data: The dictionary response from the REST API containing metadata.

        Returns:
            A QueryResultOutput instance.
        """
        sum_file_sizes = (
            SumFileSizes(
                sum_file_size_bytes=data["sum_file_sizes"].sum_file_size_bytes,
                greater_than=data["sum_file_sizes"].greater_than,
            )
            if data.get("sum_file_sizes")
            else None
        )

        return cls(
            result=result,
            count=data.get("count", None),
            sum_file_sizes=sum_file_sizes,
            last_updated_on=data.get("last_updated_on", None),
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

    is_first_line_header: bool = True
    """Is the first line a header? The default value of 'true' will be used if this is not provided by the caller."""

    def to_synapse_request(self):
        """Converts the request to a request expected of the Synapse REST API."""
        request = {
            "separator": self.separator,
            "quoteCharacter": self.quote_character,
            "escapeCharacter": self.escape_character,
            "lineEnd": self.line_end,
            "isFirstLineHeader": self.is_first_line_header,
        }
        delete_none_keys(request)
        return request

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        """Converts a response from the REST API into this dataclass."""
        self.separator = data.get("separator", self.separator)
        self.quote_character = data.get("quoteCharacter", self.quote_character)
        self.escape_character = data.get("escapeCharacter", self.escape_character)
        self.line_end = data.get("lineEnd", self.line_end)
        self.is_first_line_header = data.get(
            "isFirstLineHeader", self.is_first_line_header
        )
        return self


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
class Row:
    """
    Represents a single row of a TableEntity.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Row.html>
    """

    row_id: Optional[int] = None
    """The immutable ID issued to a new row."""

    version_number: Optional[int] = None
    """The version number of this row. Each row version is immutable, so when a row
    is updated a new version is created."""

    etag: Optional[str] = None
    """For queries against EntityViews with query.includeEntityEtag=true, this field
    will contain the etag of the entity. Will be null for all other cases."""

    values: Optional[List[str]] = None
    """The values for each column of this row. To delete a row, set this to an empty list: []"""

    def to_boolean(value):
        """
        Convert a string to boolean, case insensitively,
        where true values are: true, t, and 1 and false values are: false, f, 0.
        Raise a ValueError for all other values.
        """
        if value is None:
            raise ValueError("Can't convert None to boolean.")

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            lower_value = value.lower()
            if lower_value in ["true", "t", "1"]:
                return True
            if lower_value in ["false", "f", "0"]:
                return False

        raise ValueError(f"Can't convert {value} to boolean.")

    @staticmethod
    def cast_values(values, headers):
        """
        Convert a row of table query results from strings to the correct column type.

        See: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ColumnType.html>
        """
        if len(values) != len(headers):
            raise ValueError(
                f"The number of columns in the csv file does not match the given headers. {len(values)} fields, {len(headers)} headers"
            )

        result = []
        for header, field in zip(headers, values):  # noqa: F402
            columnType = header.get("columnType", "STRING")

            # convert field to column type
            if field is None or field == "":
                result.append(None)
            elif columnType in {
                "STRING",
                "ENTITYID",
                "FILEHANDLEID",
                "LARGETEXT",
                "USERID",
                "LINK",
            }:
                result.append(field)
            elif columnType == "DOUBLE":
                result.append(float(field))
            elif columnType == "INTEGER":
                result.append(int(field))
            elif columnType == "BOOLEAN":
                result.append(Row.to_boolean(field))
            elif columnType == "DATE":
                result.append(from_unix_epoch_time(field))
            elif columnType in {
                "STRING_LIST",
                "INTEGER_LIST",
                "BOOLEAN_LIST",
                "ENTITYID_LIST",
                "USERID_LIST",
            }:
                result.append(json.loads(field))
            elif columnType == "DATE_LIST":
                result.append(json.loads(field, parse_int=from_unix_epoch_time))
            else:
                # default to string for unknown column type
                result.append(field)

        return result

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "Row":
        """Create a Row from a dictionary response."""
        return cls(
            row_id=data.get("rowId"),
            version_number=data.get("versionNumber"),
            etag=data.get("etag"),
            values=data.get("values"),
        )


@dataclass
class ActionRequiredCount:
    """
    Represents a single action that the user will need to take in order to download one or more files.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/download/ActionRequiredCount.html>
    """

    action: Optional[Dict[str, Any]] = None
    """An action that the user must take in order to download a file."""

    count: Optional[int] = None
    """The number of files that require this action."""

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "ActionRequiredCount":
        """Create an ActionRequiredCount from a dictionary response."""
        return cls(
            action=data.get("action", None),
            count=data.get("count", None),
        )


@dataclass
class SelectColumn:
    """
    A column model contains the metadata of a single column of a TableEntity.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SelectColumn.html>
    """

    name: Optional[str] = None
    """The required display name of the column"""

    column_type: Optional[ColumnType] = None
    """The column type determines the type of data that can be stored in a column.
    Switching between types (using a transaction with TableUpdateTransactionRequest
    in the "changes" list) is generally allowed except for switching to "_LIST"
    suffixed types. In such cases, a new column must be created and data must be
    copied over manually"""

    id: Optional[str] = None
    """The optional ID of the select column, if this is a direct column selected"""

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "SelectColumn":
        """Create a SelectColumn from a dictionary response."""
        column_type = None
        column_type_value = data.get("columnType")
        if column_type_value:
            try:
                column_type = ColumnType(column_type_value)
            except ValueError:
                column_type = None
        return cls(
            name=data.get("name"),
            column_type=column_type,
            id=data.get("id"),
        )


@dataclass
class QueryNextPageToken:
    """
    Token for retrieving the next page of query results.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryNextPageToken.html>
    """

    concrete_type: Optional[str] = None
    """The concrete type of this object"""

    entity_id: Optional[str] = None
    """The ID of the entity (table/view) being queried"""

    token: Optional[str] = None
    """The token for the next page."""

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "QueryNextPageToken":
        """Create a QueryNextPageToken from a dictionary response."""
        return cls(
            concrete_type=data.get("concreteType"),
            entity_id=data.get("entityId"),
            token=data.get("token"),
        )


@dataclass
class RowSet:
    """
    Represents a set of row of a TableEntity.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/RowSet.html>
    """

    concrete_type: Optional[str] = None
    """The concrete type of this object"""

    table_id: Optional[str] = None
    """The ID of the TableEntity than owns these rows"""

    etag: Optional[str] = None
    """Any RowSet returned from Synapse will contain the current etag of the change set.
    To update any rows from a RowSet the etag must be provided with the POST."""

    headers: Optional[List[SelectColumn]] = None
    """The list of SelectColumns that describes the rows of this set."""

    rows: Optional[List[Row]] = field(default_factory=list)
    """The Rows of this set. The index of each row value aligns with the index of each header."""

    @classmethod
    def cast_row(
        cls, row: Dict[str, Any], headers: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Cast the values in a single row to their appropriate column types.

        This method takes a row dictionary containing string values from a table query
        response and converts them to the correct Python types based on the column
        headers. For example, converts string "123" to integer 123 for INTEGER columns,
        or string "true" to boolean True for BOOLEAN columns.

        Arguments:
            row: A dictionary representing a single table row with keys that need to be cast to proper types.
            headers: A list of header dictionaries, each containing column metadata
                including 'columnType' which determines how to cast the corresponding
                value in the row.

        Returns:
            The same row dictionary with the 'values' field updated to contain
            properly typed values instead of strings.
        """
        row["values"] = Row.cast_values(row["values"], headers)
        return row

    @classmethod
    def cast_row_set(cls, rows: List[Row], headers: List[Dict[str, Any]]) -> List[Row]:
        """
        Cast the values in multiple rows to their appropriate column types.

        This method takes a list of row dictionaries containing string values from a table query
        response and converts them to the correct Python types based on the column headers.
        It applies the same type casting logic as `cast_row` to each row in the collection.

        Arguments:
            rows: A list of row dictionaries, each representing a single table row with
                field contains a list of string values that need to be cast to proper types.
            headers: A list of header dictionaries, each containing column metadata
                including 'columnType' which determines how to cast the corresponding
                values in each row.

        Returns:
            A list of row dictionaries with the 'values' field in each row updated to
            contain properly typed values instead of strings.
        """
        rows = [cls.cast_row(row, headers) for row in rows]
        return rows

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "RowSet":
        """Create a RowSet from a dictionary response."""
        headers_data = data.get("headers")
        rows_data = data.get("rows")

        # Handle headers - convert to SelectColumn objects
        headers = None
        if headers_data and isinstance(headers_data, list):
            headers = [SelectColumn.fill_from_dict(header) for header in headers_data]

        # Handle rows - cast values and convert to Row objects
        rows = None
        if rows_data and isinstance(rows_data, list):
            # Cast row values based on header types if headers are available
            if headers_data and isinstance(headers_data, list):
                rows_data = cls.cast_row_set(rows_data, headers_data)
            # Convert to Row objects
            rows = [Row.fill_from_dict(row) for row in rows_data]

        return cls(
            concrete_type=data.get("concreteType"),
            table_id=data.get("tableId"),
            etag=data.get("etag"),
            headers=headers,
            rows=rows,
        )


@dataclass
class QueryResult:
    """
    A page of query result.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryResult.html>
    """

    query_results: RowSet
    """Represents a set of row of a TableEntity (RowSet)"""

    concrete_type: str = QUERY_RESULT
    """The concrete type of this object"""

    next_page_token: Optional[QueryNextPageToken] = None
    """Token for retrieving the next page of results, if available"""

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "QueryResult":
        """Create a QueryResult from a dictionary response."""
        next_page_token = None
        query_results = data.get("queryResults", None)

        if data.get("nextPageToken", None):
            next_page_token = QueryNextPageToken.fill_from_dict(data["nextPageToken"])

        if data.get("queryResults", None):
            query_results = RowSet.fill_from_dict(data["queryResults"])

        return cls(
            concrete_type=data.get("concreteType"),
            query_results=query_results,
            next_page_token=next_page_token,
        )


@dataclass
class QueryJob(AsynchronousCommunicator):
    """
    A query job that can be submitted to Synapse and return a DownloadFromTableResult.

    This class combines query request parameters with the ability to receive
    query results through the AsynchronousCommunicator pattern.

    Request modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DownloadFromTableRequest.html>

    Response modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DownloadFromTableResult.html>
    """

    # Request parameters
    entity_id: str
    """The ID of the entity (table/view) being queried"""

    concrete_type: str = QUERY_TABLE_CSV_REQUEST
    "The concrete type of the request (usually DownloadFromTableRequest)"

    write_header: Optional[bool] = True
    """Should the first line contain the columns names as a header in the resulting file? Set to 'true' to include the headers else, 'false'. The default value is 'true'."""

    include_row_id_and_row_version: Optional[bool] = True
    """Should the first two columns contain the row ID and row version? The default value is 'true'."""

    csv_table_descriptor: Optional[CsvTableDescriptor] = None
    """The description of a csv for upload or download."""

    file_name: Optional[str] = None
    """The optional name for the downloaded table."""

    sql: Optional[str] = None
    """The SQL query to execute"""

    additional_filters: Optional[List[Dict[str, Any]]] = None
    """Appends additional filters to the SQL query. These are applied before facets. Filters within the list have an AND relationship. If a WHERE clause already exists on the SQL query or facets are selected, it will also be ANDed with the query generated by these additional filters."""
    """TODO: create QueryFilter dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651"""

    selected_facets: Optional[List[Dict[str, Any]]] = None
    """The selected facet filters."""
    """TODO: create FacetColumnRequest dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651"""

    include_entity_etag: Optional[bool] = False
    """"Optional, default false. When true, a query results against views will include the Etag of each entity in the results. Note: The etag is necessary to update Entities in the view."""

    select_file_column: Optional[int] = None
    """The id of the column used to select file entities (e.g. to fetch the action required for download). The column needs to be an ENTITYID type column and be part of the schema of the underlying table/view."""

    select_file_version_column: Optional[int] = None
    """The id of the column used as the version for selecting file entities when required (e.g. to add a materialized view query to the download cart with version enabled). The column needs to be an INTEGER type column and be part of the schema of the underlying table/view."""

    offset: Optional[int] = None
    """The optional offset into the results"""

    limit: Optional[int] = None
    """The optional limit to the results"""

    sort: Optional[List[Dict[str, Any]]] = None
    """The sort order for the query results (ARRAY<SortItem>)"""
    """TODO: Add SortItem dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651"""

    # Response attributes (filled after job completion)
    job_id: Optional[str] = None
    """The job ID returned from the async job"""

    results_file_handle_id: Optional[str] = None
    """The file handle ID of the results CSV file"""

    table_id: Optional[str] = None
    """The ID of the table that was queried"""

    etag: Optional[str] = None
    """The etag of the table"""

    headers: Optional[List[SelectColumn]] = None
    """The column headers from the query result"""

    response_concrete_type: Optional[str] = QUERY_TABLE_CSV_RESULT
    """The concrete type of the response (usually DownloadFromTableResult)"""

    def to_synapse_request(self) -> Dict[str, Any]:
        """Convert to DownloadFromTableRequest format for async job submission."""

        csv_table_descriptor = None
        if self.csv_table_descriptor:
            csv_table_descriptor = self.csv_table_descriptor.to_synapse_request()

        synapse_request = {
            "concreteType": QUERY_TABLE_CSV_REQUEST,
            "entityId": self.entity_id,
            "csvTableDescriptor": csv_table_descriptor,
            "sql": self.sql,
            "writeHeader": self.write_header,
            "includeRowIdAndRowVersion": self.include_row_id_and_row_version,
            "includeEntityEtag": self.include_entity_etag,
            "fileName": self.file_name,
            "additionalFilters": self.additional_filters,
            "selectedFacet": self.selected_facets,
            "selectFileColumns": self.select_file_column,
            "selectFileVersionColumns": self.select_file_version_column,
            "offset": self.offset,
            "sort": self.sort,
        }
        delete_none_keys(synapse_request)
        return synapse_request

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "Self":
        """Fill the job results from Synapse response."""
        # Fill response attributes from DownloadFromTableResult
        headers = None
        headers_data = synapse_response.get("headers")
        if headers_data and isinstance(headers_data, list):
            headers = [SelectColumn.fill_from_dict(header) for header in headers_data]

        self.job_id = synapse_response.get("jobId")
        self.response_concrete_type = synapse_response.get("concreteType")
        self.results_file_handle_id = synapse_response.get("resultsFileHandleId")
        self.table_id = synapse_response.get("tableId")
        self.etag = synapse_response.get("etag")
        self.headers = headers

        return self


@dataclass
class Query:
    """
    Represents a SQL query with optional parameters.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Query.html>
    """

    sql: str
    """The SQL query string"""

    additional_filters: Optional[List[Dict[str, Any]]] = None
    """Appends additional filters to the SQL query. These are applied before facets.
    Filters within the list have an AND relationship. If a WHERE clause already exists
    on the SQL query or facets are selected, it will also be ANDed with the query
    generated by these additional filters."""
    """TODO: create QueryFilter dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651"""

    selected_facets: Optional[List[Dict[str, Any]]] = None
    """The selected facet filters"""
    """TODO: create FacetColumnRequest dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651"""

    include_entity_etag: Optional[bool] = False
    """Optional, default false. When true, a query results against views will include
    the Etag of each entity in the results. Note: The etag is necessary to update
    Entities in the view."""

    select_file_column: Optional[int] = None
    """The id of the column used to select file entities (e.g. to fetch the action
    required for download). The column needs to be an ENTITYID type column and be
    part of the schema of the underlying table/view."""

    select_file_version_column: Optional[int] = None
    """The id of the column used as the version for selecting file entities when required
    (e.g. to add a materialized view query to the download cart with version enabled).
    The column needs to be an INTEGER type column and be part of the schema of the
    underlying table/view."""

    offset: Optional[int] = None
    """The optional offset into the results"""

    limit: Optional[int] = None
    """The optional limit to the results"""

    sort: Optional[List[Dict[str, Any]]] = None
    """The sort order for the query results (ARRAY<SortItem>)"""
    """TODO: Add SortItem dataclass: https://sagebionetworks.jira.com/browse/SYNPY-1651 """

    def to_synapse_request(self) -> Dict[str, Any]:
        """Converts the Query object into a dictionary that can be passed into the REST API."""
        result = {
            "sql": self.sql,
            "additionalFilters": self.additional_filters,
            "selectedFacets": self.selected_facets,
            "includeEntityEtag": self.include_entity_etag,
            "selectFileColumn": self.select_file_column,
            "selectFileVersionColumn": self.select_file_version_column,
            "offset": self.offset,
            "limit": self.limit,
            "sort": self.sort,
        }
        delete_none_keys(result)
        return result


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

    @classmethod
    def fill_from_dict(cls, synapse_sub_column: Dict[str, Any]) -> "JsonSubColumn":
        """Converts a response from the synapseclient into this dataclass."""
        return cls(
            name=synapse_sub_column.get("name", ""),
            column_type=(
                ColumnType(synapse_sub_column.get("columnType", None))
                if synapse_sub_column.get("columnType", None)
                else ColumnType.STRING
            ),
            json_path=synapse_sub_column.get("jsonPath", ""),
            facet_type=(
                FacetType(synapse_sub_column.get("facetType", None))
                if synapse_sub_column.get("facetType", None)
                else None
            ),
        )

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

    async def get_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Column":
        """
        Get a column by its ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Column instance.

        Example: Getting a column by ID
            Getting a column by ID

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                async def get_column():
                    column = await Column(id="123").get_async()
                    print(column.name)

                asyncio.run(get_column())
        """
        from synapseclient.api import get_column

        if not self.id:
            raise ValueError("Column ID is required to get a column")

        result = await get_column(
            column_id=self.id,
            synapse_client=synapse_client,
        )

        self.fill_from_dict(result)
        return self

    @skip_async_to_sync
    @staticmethod
    async def list_async(
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> AsyncGenerator["Column", None]:
        """
        List columns with optional prefix filtering.

        Arguments:
            prefix: Optional prefix to filter columns by name.
            limit: Number of columns to retrieve per request to Synapse (pagination parameter).
                The function will continue retrieving results until all matching columns are returned.
            offset: The index of the first column to return (pagination parameter).
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Column instances.

        Example: Getting all columns
            Getting all columns

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                async def get_columns():
                    async for column in Column.list_async():
                        print(column.name)

                asyncio.run(get_columns())

        Example: Getting columns with a prefix
            Getting columns with a prefix

                import asyncio
                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                async def get_columns():
                    async for column in Column.list_async(prefix="my_prefix"):
                        print(column.name)

                asyncio.run(get_columns())
        """
        from synapseclient.api import list_columns

        async for column in list_columns(
            prefix=prefix,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        ):
            yield column

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

        json_sub_columns_data = synapse_column.get("jsonSubColumns", None)
        if json_sub_columns_data:
            self.json_sub_columns = [
                JsonSubColumn.fill_from_dict(sub_column_data)
                for sub_column_data in json_sub_columns_data
            ]
        else:
            self.json_sub_columns = None

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
            [replace(sub_col) for sub_col in self.json_sub_columns]
            if self.json_sub_columns
            else None
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


@dataclass
class QueryResultBundle:
    """
    A bundle of information about a query result.

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryResultBundle.html>
    """

    concrete_type: str = QUERY_TABLE_CSV_REQUEST
    """The concrete type of this object"""

    query_result: QueryResult = None
    """A page of query result"""

    query_count: Optional[int] = None
    """The total number of rows that match the query. Use mask = 0x2 to include in the
    bundle."""

    select_columns: Optional[List[SelectColumn]] = None
    """The list of SelectColumns from the select clause. Use mask = 0x4 to include in
    the bundle."""

    max_rows_per_page: Optional[int] = None
    """The maximum number of rows that can be retrieved in a single call. This is a
    function of the columns that are selected in the query. Use mask = 0x8 to include
    in the bundle."""

    column_models: Optional[List[Column]] = None
    """The list of ColumnModels for the table. Use mask = 0x10 to include in the bundle."""

    facets: Optional[List[Dict[str, Any]]] = None
    """TODO: create facets dataclass"""
    """The list of facets for the search results. Use mask = 0x20 to include in the bundle."""

    sum_file_sizes: Optional[SumFileSizes] = None
    """The sum of the file size for all files in the given view query. Use mask = 0x40
    to include in the bundle."""

    last_updated_on: Optional[str] = None
    """The date-time when this table/view was last updated. Note: Since views are
    eventually consistent a view might still be out-of-date even if it was recently
    updated. Use mask = 0x80 to include in the bundle. This is returned in the
    ISO8601 format like `2000-01-01T00:00:00.000Z`."""

    combined_sql: Optional[str] = None
    """The SQL that is combination of a the input SQL, FacetRequests, AdditionalFilters,
    Sorting, and Pagination. Use mask = 0x100 to include in the bundle."""

    actions_required: Optional[List[ActionRequiredCount]] = None
    """The first 50 actions required to download the files that are part of the query.
    Use mask = 0x200 to include them in the bundle."""

    @classmethod
    def fill_from_dict(cls, data: Dict[str, Any]) -> "QueryResultBundle":
        """Create a QueryResultBundle from a dictionary response."""
        # Handle sum_file_sizes
        sum_file_sizes = None
        sum_file_sizes_data = data.get("sumFileSizes")
        if sum_file_sizes_data:
            sum_file_sizes = SumFileSizes(
                sum_file_size_bytes=sum_file_sizes_data.get("sumFileSizesBytes"),
                greater_than=sum_file_sizes_data.get("greaterThan"),
            )

        # Handle query_result
        query_result = None
        query_result_data = data.get("queryResult")
        if query_result_data:
            query_result = QueryResult.fill_from_dict(query_result_data)

        # Handle select_columns
        select_columns = None
        select_columns_data = data.get("selectColumns")
        if select_columns_data and isinstance(select_columns_data, list):
            select_columns = [
                SelectColumn.fill_from_dict(col) for col in select_columns_data
            ]

        # Handle actions_required
        actions_required = None
        actions_required_data = data.get("actionsRequired")
        if actions_required_data and isinstance(actions_required_data, list):
            actions_required = [
                ActionRequiredCount.fill_from_dict(action)
                for action in actions_required_data
            ]

        # Handle column_models
        column_models = None
        column_models_data = data.get("columnModels")
        if column_models_data and isinstance(column_models_data, list):
            column_models = [Column().fill_from_dict(col) for col in column_models_data]

        return cls(
            concrete_type=data.get("concreteType"),
            query_result=query_result,
            query_count=data.get("queryCount"),
            select_columns=select_columns,
            max_rows_per_page=data.get("maxRowsPerPage"),
            column_models=column_models,
            facets=data.get("facets"),
            sum_file_sizes=sum_file_sizes,
            last_updated_on=data.get("lastUpdatedOn"),
            combined_sql=data.get("combinedSql"),
            actions_required=actions_required,
        )


@dataclass
class QueryBundleRequest(AsynchronousCommunicator):
    """
    A query bundle request that can be submitted to Synapse to retrieve query results with metadata.

    This class combines query request parameters with the ability to receive
    a QueryResultBundle through the AsynchronousCommunicator pattern.

    The partMask determines which parts of the result bundle are included:
    - Query Results (queryResults) = 0x1
    - Query Count (queryCount) = 0x2
    - Select Columns (selectColumns) = 0x4
    - Max Rows Per Page (maxRowsPerPage) = 0x8
    - The Table Columns (columnModels) = 0x10
    - Facet statistics for each faceted column (facetStatistics) = 0x20
    - The sum of the file sizes (sumFileSizesBytes) = 0x40
    - The last updated on date (lastUpdatedOn) = 0x80
    - The combined SQL query including additional filters (combinedSql) = 0x100
    - The list of actions required for any file in the query (actionsRequired) = 0x200

    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryBundleRequest.html>
    """

    # Request parameters
    entity_id: str
    """The ID of the entity (table/view) being queried"""

    query: Query
    """The SQL query with parameters"""

    concrete_type: str = QUERY_BUNDLE_REQUEST
    """The concrete type of this request"""

    part_mask: Optional[int] = None
    """Optional integer mask to request specific parts. Default includes all parts if not specified."""

    # Response attributes (filled after job completion from QueryResultBundle)
    query_result: Optional[QueryResult] = None
    """A page of query result"""

    query_count: Optional[int] = None
    """The total number of rows that match the query"""

    select_columns: Optional[List[SelectColumn]] = None
    """The list of SelectColumns from the select clause"""

    max_rows_per_page: Optional[int] = None
    """The maximum number of rows that can be retrieved in a single call"""

    column_models: Optional[List[Dict[str, Any]]] = None
    """The list of ColumnModels for the table"""

    facets: Optional[List[Dict[str, Any]]] = None
    """The list of facets for the search results"""

    sum_file_sizes: Optional[SumFileSizes] = None
    """The sum of the file size for all files in the given view query"""

    last_updated_on: Optional[str] = None
    """The date-time when this table/view was last updated"""

    combined_sql: Optional[str] = None
    """The SQL that is combination of a the input SQL, FacetRequests, AdditionalFilters, Sorting, and Pagination"""

    actions_required: Optional[List[ActionRequiredCount]] = None
    """The first 50 actions required to download the files that are part of the query"""

    def to_synapse_request(self) -> Dict[str, Any]:
        """Convert to QueryBundleRequest format for async job submission."""
        result = {
            "concreteType": self.concrete_type,
            "entityId": self.entity_id,
            "query": self.query,
        }

        if self.part_mask is not None:
            result["partMask"] = self.part_mask

        delete_none_keys(result)
        return result

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "Self":
        """Fill the request results from Synapse response (QueryResultBundle)."""
        # Use QueryResultBundle's fill_from_dict logic to populate response fields
        bundle = QueryResultBundle.fill_from_dict(synapse_response)

        # Copy all the result fields from the bundle
        self.query_result = bundle.query_result
        self.query_count = bundle.query_count
        self.select_columns = bundle.select_columns
        self.max_rows_per_page = bundle.max_rows_per_page
        self.column_models = bundle.column_models
        self.facets = bundle.facets
        self.sum_file_sizes = bundle.sum_file_sizes
        self.last_updated_on = bundle.last_updated_on
        self.combined_sql = bundle.combined_sql
        self.actions_required = bundle.actions_required

        return self


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
