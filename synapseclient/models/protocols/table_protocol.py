"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import Any, Dict, Optional, Protocol

from typing_extensions import Self

from synapseclient import Synapse


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
