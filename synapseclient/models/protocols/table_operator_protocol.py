"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

import pandas as pd
from typing_extensions import Self

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import ColumnExpansionStrategy, SchemaStorageStrategy


class TableOperatorSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self, dry_run: bool = False, *, synapse_client: Optional[Synapse] = None
    ) -> Self:
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

                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                table = Table(id="syn4567").get(include_columns=True, include_activity=True)
                print(table)
                print(table.columns)
                print(table.activity)


        Example: Getting metadata about a table using name and parent_id
            Get a table by name/parent_id and print out the columns and activity.
            `include_columns` and `include_activity` are optional arguments that
            default to False. When you need to update existing columns or activity you
            will need to set these to True, make the changes, and then call the
            `.store()` method.

                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                table = Table(name="my_table", parent_id="syn1234").get(include_columns=True, include_activity=True)
                print(table)
                print(table.columns)
                print(table.activity)
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the table from synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Deleting a table
            Deleting a table is only supported by the ID of the table.

                from synapseclient import Synapse

                syn = Synapse()
                syn.login()

                Table(id="syn4567").delete()
        """
        return None

    @staticmethod
    def query(
        query: str,
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
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.

        Example: Querying for data
            This example shows how you may query for data in a table and print out the
            results.

                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                results = Table.query(query="SELECT * FROM syn1234")
                print(results)
        """
        return pd.DataFrame()


class TableRowOperatorSynchronousProtocol(Protocol):
    def upsert_rows(
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



            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    |      | 1    |
            | B    | 2    |      |

        """
        return None

    def store_rows(
        self,
        values: Union[str, List[Dict[str, Any]], Dict[str, Any], pd.DataFrame],
        schema_storage_strategy: "SchemaStorageStrategy" = None,
        column_expansion_strategy: "ColumnExpansionStrategy" = None,
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

            column_expansion_strategy: Determines how to automate the expansion of
                columns based on the data that is being stored. The options given allow
                cells with a limit on the length of content (Such as strings) or cells
                with a limit on the number of values (Such as lists) to be expanded to
                a larger size if the data being stored exceeds the limit. If you want to
                have full control over the schema you may set this to `None` and create
                the columns manually.
                TODO: When implementing this feature more verbose documentation on exactly what columns types may be expanded

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None
        """
        return None

    def delete_rows(
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

                from synapseclient import Synapse
                from synapseclient.models import Table

                syn = Synapse()
                syn.login()

                Table(id="syn1234").delete_rows(query="SELECT ROW_ID, ROW_VERSION FROM syn1234 WHERE foo = 'asdf'")
        """
        return pd.DataFrame()
