"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.utils import MB

if TYPE_CHECKING:
    from synapseclient.models import ColumnExpansionStrategy, SchemaStorageStrategy
    from synapseclient.models.mixins.table_operator import (
        AppendableRowSetRequest,
        CsvTableDescriptor,
        QueryResultBundle,
        TableSchemaChangeRequest,
        UploadToTableRequest,
    )

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


class TableOperatorSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Table instance stored in synapse.
        """
        return self

    def get(
        self,
        include_columns: bool = False,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
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

        Example: Getting metadata about a table using id
            Get a table by ID and print out the columns and activity. `include_columns`
            and `include_activity` are optional arguments that default to False. When
            you need to update existing columns or activity you will need to set these
            to True, make the changes, and then call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            table = Table(id="syn4567").get(include_columns=True, include_activity=True)
            print(table)
            print(table.columns)
            print(table.activity)
            ```


        Example: Getting metadata about a table using name and parent_id
            Get a table by name/parent_id and print out the columns and activity.
            `include_columns` and `include_activity` are optional arguments that
            default to False. When you need to update existing columns or activity you
            will need to set these to True, make the changes, and then call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            table = Table(name="my_table", parent_id="syn1234").get(include_columns=True, include_activity=True)
            print(table)
            print(table.columns)
            print(table.activity)
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

        Example: Deleting a table
            Deleting a table is only supported by the ID of the table.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            Table(id="syn4567").delete()
            ```
        """
        return None

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
        from pandas import DataFrame

        return DataFrame()

    @staticmethod
    def query_part_mask(
        query: str,
        part_mask: int,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "QueryResultBundle":
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
        return None


class TableRowOperatorSynchronousProtocol(Protocol):
    def upsert_rows(
        self,
        values: DATA_FRAME_TYPE,
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
            to update, update the data, then use the [store_rows][synapseclient.models.mixins.table_operator.TableRowOperator.store_rows] method to
            update the data in Synapse. Any rows you want to insert may be added
            to the DataFrame that is passed to the [store_rows][synapseclient.models.mixins.table_operator.TableRowOperator.store_rows] method.
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

                - A string holding the path to a CSV file. Tthe data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_operator.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`
            import pandas as pd

            syn = Synapse()
            syn.login()


            table = Table(id="syn123").get(include_columns=True)

            df = {
                'col1': ['A', 'B', 'C'],
                'col2': [22, 2, 3],
                'col3': [1, 33, 3],
            }

            table.upsert_rows(values=df, primary_keys=["col1"])
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
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()


            table = Table(id="syn123").get(include_columns=True)

            df = {
                'col1': ['A', 'B'],
                'col2': [None, 2],
                'col3': [1, None],
            }

            table.upsert_rows(values=df, primary_keys=["col1"])
            ```



            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    |      | 1    |
            | B    | 2    |      |

        """
        return None

    def store_rows(
        self,
        values: Union[str, Dict[str, Any], DATA_FRAME_TYPE],
        schema_storage_strategy: "SchemaStorageStrategy" = None,
        column_expansion_strategy: "ColumnExpansionStrategy" = None,
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
        csv_table_descriptor: Optional["CsvTableDescriptor"] = None,
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

                - A string holding the path to a CSV file. If the `schema_storage_strategy` is set to `None` the data will be uploaded as is. If `schema_storage_strategy` is set to `INFER_FROM_DATA` the data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_operator.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            data_to_insert = {
                'col1': ['A', 'B', 'C'],
                'col2': [1, 2, 3],
                'col3': [1, 2, 3],
            }

            Table(id="syn1234").store_rows(values=data_to_insert)
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
            from synapseclient import Synapse
            from synapseclient.models import Table, SchemaStorageStrategy # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            data_to_insert = {
                'col1': ['A', 'B', 'C'],
                'col2': [1, 2, 3],
                'col3': [1, 2, 3],
            }

            Table(id="syn1234").store_rows(
                values=data_to_insert,
                schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA
            )
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
            from synapseclient import Synapse
            from synapseclient.models import Table, SchemaStorageStrategy # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            data_to_insert = {
                'col1': ['A', 'B', 'C'],
                'col2': [1, 2, 3],
                'col3': [1, 2, 3],
            }

            Table(id="syn1234").store_rows(
                values=data_to_insert,
                dry_run=True,
                schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA
            )
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
            from synapseclient import Synapse
            from synapseclient.models import Table, query # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            query_results = query(query="select * from syn1234 where col1 in ('A', 'B')")

            # Update `col2` of the row where `col1` is `A` to `22`
            query_results.loc[query_results['col1'] == 'A', 'col2'] = 22

            # Update `col3` of the row where `col1` is `B` to `33`
            query_results.loc[query_results['col1'] == 'B', 'col3'] = 33

            Table(id="syn1234").store_rows(values=query_results)
            ```

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 22   | 1    |
            | B    | 2    | 33   |
            | C    | 3    | 3    |

        """
        return None

    def delete_rows(
        self, query: str, *, synapse_client: Optional[Synapse] = None
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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of your query for the rows that were deleted from the table.

        Example: Selecting a row to delete
            This example shows how you may select a row to delete from a table.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            Table(id="syn1234").delete_rows(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo = 'asdf'")
            ```

        Example: Selecting all rows that contain a null value
            This example shows how you may select a row to delete from a table where
            a column has a null value.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table # Also works with `Dataset`

            syn = Synapse()
            syn.login()

            Table(id="syn1234").delete_rows(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo is null")
            ```
        """
        from pandas import DataFrame

        return DataFrame()


class ViewOperatorSynchronousProtocol(Protocol):
    """A class that extends the TableOperator and TableRowOperator classes to add
    appropriately handle View-like Synapse entities.

    In the Synapse API, a View is a sub-category of the Table model which includes other Table-like
    entities including: SubmissionView, EntityView, and Dataset.
    """

    def store(
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
        return Self
