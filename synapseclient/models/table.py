import asyncio
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from opentelemetry import context, trace

from synapseclient import Column as Synapse_Column
from synapseclient import Schema as Synapse_Schema
from synapseclient import Synapse
from synapseclient import Table as Synapse_Table
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.utils import run_and_attach_otel_context
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.protocols.table_protocol import (
    ColumnSynchronousProtocol,
    TableSynchronousProtocol,
)
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)
from synapseclient.table import CsvFileTable as Synapse_CsvFileTable
from synapseclient.table import TableQueryResult as Synaspe_TableQueryResult
from synapseclient.table import delete_rows

# TODO: Have a plug-and-play interface to plugin different dataframes,
# or perhaps stream a CSV back when querying for data and uploading data


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
class CsvResultFormat:
    """CSV result format options."""

    quote_character: Optional[str] = '"'
    """default double quote"""

    escape_character: Optional[str] = "\\"
    """default backslash"""

    line_end: Optional[str] = str(os.linesep)
    """defaults to os.linesep"""

    separator: Optional[str] = ","
    """defaults to comma"""

    header: Optional[bool] = True
    """True by default"""

    include_row_id_and_row_version: Optional[bool] = True
    """True by default"""

    download_location: Optional[str] = None
    """directory path to download the CSV file to"""

    def to_dict(self):
        """Converts the CsvResultFormat into a dictionary that can be passed into the synapseclient."""
        return {
            "resultsAs": "csv",
            "quoteCharacter": self.quote_character,
            "escapeCharacter": self.escape_character,
            "lineEnd": self.line_end,
            "separator": self.separator,
            "header": self.header,
            "includeRowIdAndRowVersion": self.include_row_id_and_row_version,
            "downloadLocation": self.download_location,
        }


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

    id: str
    """The immutable ID issued to new columns"""

    name: str
    """The display name of the column"""

    column_type: ColumnType
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
        self.id = synapse_column.id
        self.name = synapse_column.name
        self.column_type = synapse_column.columnType
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
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The Column instance stored in synapse.
        """
        # TODO - We need to add in some validation before the store to verify we have enough
        # information to store the data

        # Call synapse
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).createColumn(
                    name=self.name,
                    columnType=self.column_type,
                ),
                current_context,
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
        columns: The columns of this table.
        description: The description of this entity. Must be 1000 characters or less.
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
        is_latest_version: If this is the latest version of the object.
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

    columns: Optional[List[Column]] = None

    # TODO: Description doesn't seem to be returned from the API. Look into why.
    # description: Optional[str] = None
    # """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = None
    """The date this table was created."""

    created_by: Optional[str] = None
    """The ID of the user that created this table."""

    modified_on: Optional[str] = None
    """The date this table was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ format"""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this table."""

    version_number: Optional[int] = None
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this table"""

    version_comment: Optional[str] = None
    """The version comment for this table"""

    is_latest_version: Optional[bool] = None
    """If this is the latest version of the object."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a table or view specifies if full text search
    should be enabled. Note that enabling full text search might slow down the
    indexing of the table or view."""

    activity: Optional[Activity] = None
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
    ] = field(default_factory=dict)
    """Additional metadata associated with the table. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`
    or None and store the entity."""

    def fill_from_dict(
        self, synapse_table: Synapse_Table, set_annotations: bool = True
    ) -> "Table":
        """Converts the data coming from the Synapse API into this datamodel.

        :param synapse_table: The data coming from the Synapse API
        """
        self.id = synapse_table.get("id", None)
        self.name = synapse_table.get("name", None)
        self.parent_id = synapse_table.get("parentId", None)
        # TODO: Description doesn't seem to be returned from the API. Look into why.
        # self.description = synapse_table.description
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
        self.columns = [
            Column(id=columnId, name=None, column_type=None)
            for columnId in synapse_table.get("columnIds", [])
        ]
        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_table.get("annotations", {})
            )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda _, **kwargs: f"Store_rows_by_csv: {kwargs.get('csv_path', None)}"
    )
    async def store_rows_from_csv_async(
        self, csv_path: str, *, synapse_client: Optional[Synapse] = None
    ) -> str:
        """Takes in a path to a CSV and stores the rows to Synapse.

        Arguments:
            csv_path: The path to the CSV to store.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The path to the CSV that was stored.
        """
        synapse_table = Synapse_Table(schema=self.id, values=csv_path)
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).store(
                    obj=synapse_table
                ),
                current_context,
            ),
        )
        print(entity)
        # TODO: What should this return?
        return csv_path

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Delete_rows: {self.name}"
    )
    async def delete_rows_async(
        self, rows: List[Row], *, synapse_client: Optional[Synapse] = None
    ) -> None:
        """Delete rows from a table.

        Arguments:
            rows: The rows to delete.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            None
        """
        rows_to_delete = []
        for row in rows:
            rows_to_delete.append([row.row_id, row.version_number])
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: delete_rows(
                    syn=Synapse.get_client(synapse_client=synapse_client),
                    table_id=self.id,
                    row_id_vers_list=rows_to_delete,
                ),
                current_context,
            ),
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Table_Schema_Store: {self.name}"
    )
    async def store_schema_async(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> "Table":
        """Store non-row information about a table including the columns and annotations.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The Table instance stored in synapse.
        """
        tasks = []
        if self.columns:
            # TODO: When a table is retrieved via `.get()` we create Column objects but
            # TODO: We only have the ID attribute. THis is causing this if check to eval
            # TODO: To True, however, we aren't actually modifying the column.
            # TODO: Perhaps we should have a `has_changed` boolean on all dataclasses
            # TODO: That we can check to see if we need to store the data.
            tasks.extend(
                column.store_async(synapse_client=synapse_client)
                for column in self.columns
            )
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # TODO: Proper exception handling
                for result in results:
                    if isinstance(result, Column):
                        print(f"Stored {result.name}")
                    else:
                        if isinstance(result, BaseException):
                            raise result
                        raise ValueError(f"Unknown type: {type(result)}", result)
            except Exception as ex:
                Synapse.get_client(synapse_client=synapse_client).logger.exception(ex)
                print("I hit an exception")

        synapse_schema = Synapse_Schema(
            name=self.name,
            columns=self.columns,
            parent=self.parent_id,
        )
        trace.get_current_span().set_attributes(
            {
                "synapse.name": self.name or "",
                "synapse.id": self.id or "",
            }
        )
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).store(
                    obj=synapse_schema
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_table=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self, synapse_client=synapse_client
        )
        if re_read_required:
            await self.get_async(
                synapse_client=synapse_client,
            )

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Table_Get: {self.name}"
    )
    async def get_async(self, *, synapse_client: Optional[Synapse] = None) -> "Table":
        """Get the metadata about the table from synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The Table instance stored in synapse.
        """
        # TODO: How do we want to support retriving the table? Do we want to support by name, and parent?
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).get(
                    entity=self.id
                ),
                current_context,
            ),
        )
        self.fill_from_dict(synapse_table=entity, set_annotations=True)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Table_Delete: {self.name}"
    )
    # TODO: Synapse allows immediate deletion of entities, but the Synapse Client does not
    # TODO: Should we support immediate deletion?
    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the table from synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            None
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                    obj=self.id
                ),
                current_context,
            ),
        )

    @classmethod
    async def query_async(
        cls,
        query: str,
        result_format: Union[CsvResultFormat, RowsetResultFormat] = CsvResultFormat(),
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union[Synapse_CsvFileTable, Synaspe_TableQueryResult]:
        """Query for data on a table stored in Synapse.

        Arguments:
            query: The query to run.
            result_format: The format of the results. Defaults to CsvResultFormat().
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The results of the query.
        """
        loop = asyncio.get_event_loop()
        current_context = context.get_current()

        # TODO: Future Idea - We stream back a CSV, and let those reading this to handle the CSV however they want
        results = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).tableQuery(
                    query=query,
                    **result_format.to_dict(),
                ),
                current_context,
            ),
        )
        print(results)
        return results
