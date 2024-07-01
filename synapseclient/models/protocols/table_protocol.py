"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.table import CsvFileTable as Synapse_CsvFileTable
from synapseclient.table import TableQueryResult as Synaspe_TableQueryResult

if TYPE_CHECKING:
    from synapseclient.models.table import (
        CsvResultFormat,
        Row,
        RowsetResultFormat,
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
            from the `.login()` method.
        :return: Column
        """
        return self


class TableSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store_rows_from_csv(
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
        return ""

    def delete_rows(
        self, rows: List["Row"], *, synapse_client: Optional[Synapse] = None
    ) -> None:
        """Delete rows from a table.

        Arguments:
            rows: The rows to delete.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            None
        """
        return None

    def store_schema(self, *, synapse_client: Optional[Synapse] = None) -> "Table":
        """Store non-row information about a table including the columns and annotations.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The Table instance stored in synapse.
        """
        return self

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Table":
        """Get the metadata about the table from synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The Table instance stored in synapse.
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the table from synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            None
        """
        return None

    @classmethod
    def query(
        cls,
        query: str,
        result_format: Union["CsvResultFormat", "RowsetResultFormat"] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union[Synapse_CsvFileTable, Synaspe_TableQueryResult, None]:
        """Query for data on a table stored in Synapse.

        Arguments:
            query: The query to run.
            result_format: The format of the results. Defaults to CsvResultFormat().
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The results of the query.
        """
        return None
