"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

import pandas as pd
from typing_extensions import Self

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models.table import (
        ColumnExpansionStrategy,
        SchemaStorageStrategy,
        Table,
    )


class ColumnSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(self, *, synapse_client: Optional[Synapse] = None) -> Self:
        """Persist the column to Synapse.

        :param synapse_client: If not passed in or None this will use the last client
            from the Synapse class constructor.
        :return: Column
        """
        return self


class TableSynchronousProtocol(Protocol):
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

    def upsert_rows(
        self,
        values: pd.DataFrame,
        upsert_columns: List[str],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        This method allows you to perform an `upsert` (Update and Insert) for a row.
        This means that you may update a row with only the data that you want to change.
        When supplied with a row that does not match the given `upsert_columns` a new
        row will be inserted. If you want to replace a row entirely you may use the
        `.store_rows()` method. See that method for more information.


        Using the `upsert_columns` argument you may specify which columns to use to
        determine if a row already exists. If a row exists with the same values in the
        columns specified in this list the row will be updated. If a row does not exist
        it will be inserted.


        Limitations:

        - The `upsert_columns` argument must contain at least one column.
        - The `upsert_columns` argument must contain columns that are not a LIST type.
        - The values used as the `upsert_columns` must be unique in the table. If there
            are multiple rows with the same values in the `upsert_columns` the behavior
            is that an exception will be raised.
        - The columns used in `upsert_columns` cannot contain updated values. Since
            the values in these columns are used to determine if a row exists, they
            cannot be updated in the same transaction.


        Arguments:
            values: Supports storing data from the following sources:

                - A string holding the path to a CSV file
                - A list of lists (or tuples) where each element is a row
                - A dictionary where the key is the column name and the value is one or more values. The values will be wrapped into a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe).
                - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

            upsert_columns: The columns to use to determine if a row already exists. If
                a row exists with the same values in the columns specified in this list
                the row will be updated. If a row does not exist it will be inserted.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor


        TODO: Add an example for deleting data out of a cell

        TODO: Add an example/support for skipping over cells in the table. Suppose I want to update row 1 `col2`, and row 2 `col3`, but I don't want to specify the data for row 1 `col3` and row 2 `col2`. Should this be supported?


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

                df = pd.DataFrame({
                    'col1': ['A', 'B', 'C'],
                    'col2': [22, 2, 3],
                    'col3': [1, 33, 3],
                })

                table.upsert_rows(values=df, upsert_columns=["col1"])

                main()

            The resulting table will look like this:

            | col1 | col2 | col3 |
            |------|------| -----|
            | A    | 22   | 1    |
            | B    | 2    | 33   |
            | C    | 3    | 3    |

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

    def get(
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
