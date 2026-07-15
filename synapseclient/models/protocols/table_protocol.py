"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import Generator, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse


class ColumnSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(self, *, synapse_client: Optional[Synapse] = None) -> "Self":
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

                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                column = Column(id="123").get()
        """
        return self

    @staticmethod
    def list(
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["Self", None, None]:
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
            A generator that yields Column instances.

        Example: Getting all columns
            Getting all columns

                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                for column in Column.list():
                    print(column.name)

        Example: Getting columns with a prefix
            Getting columns with a prefix

                from synapseclient import Synapse
                from synapseclient.models import Column

                syn = Synapse()
                syn.login()

                for column in Column.list(prefix="my_prefix"):
                    print(column.name)
        """
        from synapseclient.api import list_columns_sync

        yield from list_columns_sync(
            prefix=prefix,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        )
