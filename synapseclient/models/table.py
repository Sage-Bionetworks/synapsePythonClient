import asyncio
import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient import Table as Synapse_Table
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import MB, delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins import AccessControllable, BaseJSONSchema
from synapseclient.models.mixins.table_components import (
    AppendableRowSetRequest,
    ColumnExpansionStrategy,
    ColumnMixin,
    CsvTableDescriptor,
    DeleteMixin,
    GetMixin,
    QueryMixin,
    SchemaStorageStrategy,
    TableBase,
    TableDeleteRowMixin,
    TableSchemaChangeRequest,
    TableStoreMixin,
    TableStoreRowMixin,
    TableUpsertMixin,
    UploadToTableRequest,
)
from synapseclient.models.table_components import Column

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


class TableSynchronousProtocol(Protocol):
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

            job_timeout: The maximum amount of time to wait for a job to complete.
                This is used when updating the table schema. If the timeout
                is reached a `SynapseTimeoutError` will be raised.
                The default is 600 seconds

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
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            table = Table(id="syn4567").get(include_activity=True)
            print(table)

            # Columns are retrieved by default
            print(table.columns)
            print(table.activity)
            ```

        Example: Getting metadata about a table using name and parent_id
            Get a table by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table

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

            syn = Synapse()
            syn.login()

            Table(id="syn4567").delete()
            ```
        """
        return None

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
        wait_for_eventually_consistent_view: bool = False,
        wait_for_eventually_consistent_view_timeout: int = 600,
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
            to update, update the data, then use the [store_rows][synapseclient.models.mixins.table_components.TableStoreRowMixin.store_row] method to
            update the data in Synapse. Any rows you want to insert may be added
            to the DataFrame that is passed to the [store_rows][synapseclient.models.mixins.table_components.TableStoreRowMixin.store_rows] method.
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

                - A string holding the path to a CSV file. Tthe data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_components.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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
            from synapseclient.models import Table
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
            from synapseclient.models import Table

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
        schema_storage_strategy: SchemaStorageStrategy = None,
        column_expansion_strategy: ColumnExpansionStrategy = None,
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
        csv_table_descriptor: Optional[CsvTableDescriptor] = None,
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

                - A string holding the path to a CSV file. If the `schema_storage_strategy` is set to `None` the data will be uploaded as is. If `schema_storage_strategy` is set to `INFER_FROM_DATA` the data will be read into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe). The code makes assumptions about the format of the columns in the CSV as detailed in the [csv_to_pandas_df][synapseclient.models.mixins.table_components.csv_to_pandas_df] function. You may pass in additional arguments to the `csv_to_pandas_df` function by passing them in as keyword arguments to this function.
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
            from synapseclient.models import Table

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
            from synapseclient.models import Table, SchemaStorageStrategy

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
            from synapseclient.models import Table, SchemaStorageStrategy

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
            from synapseclient.models import Table, query

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
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            Table(id="syn1234").delete_rows(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo = 'asdf'")
            ```

        Example: Selecting all rows that contain a null value
            This example shows how you may select a row to delete from a table where
            a column has a null value.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Table

            syn = Synapse()
            syn.login()

            Table(id="syn1234").delete_rows(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo is null")
            ```
        """
        from pandas import DataFrame

        return DataFrame()

    def snapshot(
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

                from synapseclient.models import Table
                from synapseclient import Synapse

                syn = Synapse()
                syn.login()

                my_table = Table(id="syn1234")
                my_table.snapshot(
                    comment="This is a new snapshot comment",
                    label="This is a unique label"
                )

        Example: Including the activity (Provenance) in the snapshot and not pulling it forward to the new `in-progress` version of the table.
            By default this method is set up to include the activity in the snapshot and
            then pull the activity forward to the new version. If you do not want to
            include the activity in the snapshot you can set `include_activity` to
            False. If you do not want to pull the activity forward to the new version
            you can set `associate_activity_to_new_version` to False.

            See the [activity][synapseclient.models.Activity] attribute on the Table
            class for more information on how to interact with the activity.

                from synapseclient.models import Table
                from synapseclient import Synapse

                syn = Synapse()
                syn.login()

                my_table = Table(id="syn1234")
                my_table.snapshot(
                    comment="This is a new snapshot comment",
                    label="This is a unique label",
                    include_activity=True,
                    associate_activity_to_new_version=False
                )

        Returns:
            A dictionary that matches: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SnapshotResponse.html>
        """
        return {}


@dataclass()
@async_to_sync
class Table(
    AccessControllable,
    TableBase,
    TableStoreRowMixin,
    TableDeleteRowMixin,
    DeleteMixin,
    ColumnMixin,
    GetMixin,
    QueryMixin,
    TableUpsertMixin,
    TableStoreMixin,
    TableSynchronousProtocol,
    BaseJSONSchema,
):
    """A Table represents the metadata of a table.

    Attributes:
        id: The unique immutable ID for this table. A new ID will be generated for new
            Tables. Once issued, this ID is guaranteed to never change or be re-issued
        name: The name of this table. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses
        description: The description of this entity. Must be 1000 characters or less.
        parent_id: The ID of the Entity that is the parent of this table.
        columns: The columns of this table. This is an ordered dictionary where the key is the
            name of the column and the value is the Column object. When creating a new instance
            of a Table object you may pass any of the following types as the `columns` argument:

            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the Column object
            - An OrderedDict where the key is the name of the column and the value is the Column object

            The order of the columns will be the order they are stored in Synapse. If you need
            to reorder the columns the recommended approach is to use the `.reorder_column()`
            method. Additionally, you may add, and delete columns using the `.add_column()`,
            and `.delete_column()` methods on your table class instance.

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

        ```python
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
        ```

    Example: Rename an existing column
        This examples shows how you may retrieve a table from synapse, rename a column,
        and then store the table back in synapse.

        ```python
        from synapseclient import Synapse
        from synapseclient.models import Table

        syn = Synapse()
        syn.login()

        table = Table(
            name="my_table",
            parent_id="syn1234",
        ).get()

        # You may also get the table by id:
        table = Table(
            id="syn4567"
        ).get()

        table.columns["my_old_column"].name = "my_new_column"

        # Before the data is stored in synapse you'll still be able to use the old key to access the column entry
        print(table.columns["my_old_column"])

        table.store()

        # After the data is stored in synapse you'll be able to use the new key to access the column entry
        print(table.columns["my_new_column"])
        ```

    Example: Create a table with a list of columns
        A list of columns may be passed in when creating a new table. The order of the
        columns in the list will be the order they are stored in Synapse. If the table
        already exists and you create the Table instance in this way the columns will
        be appended to the end of the existing columns.

        ```python
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
        ```


    Example: Creating a table with a dictionary of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

        ```python
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
        ```

    Example: Creating a table with an OrderedDict of columns
        When specifying a number of columns via a dict setting the `name` attribute
        on the `Column` object is optional. When it is not specified it will be
        pulled from the key of the dict.

        ```python
        from collections import OrderedDict
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
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this table. A new ID will be generated for new
    Tables. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: Optional[str] = None
    """The name of this table. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
    apostrophes, and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

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

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your table class instance.

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

    def __post_init__(self):
        """Post initialization of the Table object. This is used to set the columns
        attribute to an OrderedDict if it is a list or dict."""
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)

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
        self, entity: Synapse_Table, set_annotations: bool = True
    ) -> "Table":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse API

        Returns:
            The Table object instance.
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
            "parentId": self.parent_id,
            "concreteType": concrete_types.TABLE_ENTITY,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isSearchEnabled": self.is_search_enabled,
            # When saving other (non-column) fields to Synapse we still need to pass
            # in the list of columns, otherwise Synapse will wipe out the columns. We
            # are using the last known columns to ensure that we are not losing any
            "columnIds": (
                [
                    column.id
                    for column in self._last_persistent_instance.columns.values()
                ]
                if self._last_persistent_instance
                and self._last_persistent_instance.columns
                else []
            ),
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

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

            ```python
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
            ```

        Example: Including the activity (Provenance) in the snapshot and not pulling it forward to the new `in-progress` version of the table.
            By default this method is set up to include the activity in the snapshot and
            then pull the activity forward to the new version. If you do not want to
            include the activity in the snapshot you can set `include_activity` to
            False. If you do not want to pull the activity forward to the new version
            you can set `associate_activity_to_new_version` to False.

            See the [activity][synapseclient.models.Activity] attribute on the Table
            class for more information on how to interact with the activity.

            ```python
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
            ```

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
                activity=(
                    self.activity.id if self.activity and include_activity else None
                ),
            ),
        )

        if associate_activity_to_new_version and self.activity:
            self._last_persistent_instance.activity = None
            await self.store_async(synapse_client=synapse_client)
        else:
            await self.get_async(include_activity=True, synapse_client=synapse_client)

        return snapshot_response
