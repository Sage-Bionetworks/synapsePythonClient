"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

import pandas as pd
from typing_extensions import Self

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models.table import Row, Table


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

    def store_rows(
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
        return None

    def delete_rows(
        self, rows: List["Row"], *, synapse_client: Optional[Synapse] = None
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
        return None

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
            query: The query to run.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The results of the query as a Pandas DataFrame.
        """
        return pd.DataFrame()
