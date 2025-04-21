import dataclasses
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api.table_services import ViewEntityType
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    ColumnMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
    TableUpdateTransaction,
    ViewBase,
    ViewSnapshotMixin,
    ViewStoreMixin,
)
from synapseclient.models.table_components import Column

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


class SubmissionViewSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SubmissionView operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store information about a SubmissionView including the annotations, columns,
        and scope. Updates to the `scope_ids` attribute will be cause the rows of the
        view to be updated. `scope_ids` is a list of evaluation queues that the view is
        associated with.

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
            The SubmissionView instance stored in synapse.

        Example: Create a new submission view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(
                name="My Submission View",
                scope_ids=["syn9876543"],  # ID of an evaluation queue
                parent_id="syn1234",
            )
            submission_view = submission_view.store()
            print(f"Created Submission View with ID: {submission_view.id}")
            ```

        Example: Update the scope of an existing submission view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(id="syn1234").get()
            submission_view.scope_ids = ["syn9876543", "syn8765432"]  # Add a second evaluation queue
            submission_view = submission_view.store()
            print("Updated Submission View scope.")
            ```
        """
        return self

    def get(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Retrieve a SubmissionView from Synapse.

        Arguments:
            include_columns: Whether to include the columns in the returned view.
                Defaults to True. This is useful when updating the columns.
            include_activity: Whether to include the activity in the returned view.
                Defaults to False. Setting this to True will include the activity
                record associated with this view.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The SubmissionView instance retrieved from Synapse.

        Example: Retrieving a submission view by ID.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(id="syn1234").get()
            print(submission_view)
            ```

        Example: Getting a submission view with its columns and activity.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(id="syn1234").get(include_columns=True, include_activity=True)
            print(submission_view.columns)
            print(submission_view.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Delete a SubmissionView from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a submission view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(id="syn1234")
            submission_view.delete()
            print("Deleted Submission View.")
            ```
        """
        pass

    def snapshot(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the `SubmissionView` entity.
        Synapse handles snapshot creation differently for `Table`- and `View`-like
        entities. `View` snapshots are created using the asyncronous job API.

        Making a snapshot of a view allows you to create an immutable version of the
        view at the time of the snapshot. This is useful to create checkpoints in time
        that you may go back and reference, or use in a publication. Snapshots are
        immutable and cannot be changed. They may only be deleted.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot. If this is not a
                unique label an exception will be raised when you store this to Synapse.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the View
                and calling the `store()` method on the View instance. Adding an
                activity to a snapshot of a table is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the table. If False the activity will not be
                associated with the new version of the table. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Creating a snapshot of a view with an activity
            Create a snapshot of a view and include the activity. The activity must have been stored in
            Synapse by using the `activity` attribute on the SubmissionView and calling the `store()`
            method on the SubmissionView instance. Adding an activity to a snapshot of a view is meant
            to capture the provenance of the data at the time of the snapshot.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            view = SubmissionView(id="syn4567")
            snapshot = view.snapshot(
                label="Q1 2025",
                comment="Submissions reviewed in Lab A",
                include_activity=True,
                associate_activity_to_new_version=True
            )
            print(snapshot)
            ```

        Example: Creating a snapshot of a view without an activity
            Create a snapshot of a view without including the activity. This is used in
            cases where we do not have any Provenance to associate with the snapshot and
            we do not want to persist any activity that may be present on the view to
            the new version of the view.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            view = SubmissionView(id="syn4567")
            snapshot = view.snapshot(
                label="Q1 2025",
                comment="Submissions reviewed in Lab A",
                include_activity=False,
                associate_activity_to_new_version=False
            )
            print(snapshot)
            ```
        """
        # Replaced at runtime
        return TableUpdateTransaction(entity_id=None)


@dataclass
@async_to_sync
class SubmissionView(
    SubmissionViewSynchronousProtocol,
    AccessControllable,
    ColumnMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
    ViewBase,
    ViewStoreMixin,
    ViewSnapshotMixin,
):
    """A `SubmissionView` object represents the metadata of a Synapse Submission View.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/SubmissionView.html>

    Attributes:
        id: The unique immutable ID for this entity. A new ID will be generated for new
            Entities. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses.
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        parent_id: The ID of the Entity that is the parent of this Entity.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this entity.
        version_comment: The version comment for this entity.
        is_latest_version: If this is the latest version of the object.
        columns: The columns of this submission view. This is an ordered dictionary
            where the key is the name of the column and the value is the Column object.
            When creating a new instance of a SubmissionView object you may pass any of
            the following types as the `columns` argument:
            - A list of Column objects
            - A dictionary where the key is the name of the column and the value is the
                Column object
            - An OrderedDict where the key is the name of the column and the value is
                the Column object
            The order of the columns will be the order they are stored in Synapse. If
            you need to reorder the columns the recommended approach is to use the
            `.reorder_column()` method. Additionally, you may add, and delete columns
            using the `.add_column()`, and `.delete_column()` methods on your view class
            instance.

            You may modify the attributes of the Column object to change the column
            type, name, or other attributes. For example, suppose you'd like to change a
            column from a INTEGER to a DOUBLE. You can do so by changing the column type
            attribute of the Column object. The next time you store the view the column
            will be updated in Synapse with the new type.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView, Column, ColumnType

            syn = Synapse()
            syn.login()

            submission_view = SubmissionView(id="syn1234").get()
            submission_view.columns["my_column"].column_type = ColumnType.DOUBLE
            submission_view.store()
            ```
            Note that the keys in this dictionary should match the column names as they
            are in Synapse. However, know that the name attribute of the Column object is
            used for all interactions with the Synapse API. The OrderedDict key is purely
            for the usage of this interface. For example, if you wish to rename a column
            you may do so by changing the name attribute of the Column object. The key in
            the OrderedDict does not need to be changed. The next time you store the view
            the column will be updated in Synapse with the new name and the key in the
            OrderedDict will be updated.
        is_search_enabled: When creating or updating a table or view, specifies if full-text
            search should be enabled. Note that enabling full-text search might slow down
            the indexing of the table or view.
        scope_ids: The list of container IDs that define the scope of this view. For
            submission views, this is the list of evaluation queues that the view
            is associated with.
        view_entity_type: The API model string for the type of view. This is used to
            determine the default columns that are added to the table. Must be defined as
            a `ViewEntityType` enum.

    Example: Create a new SubmissionView.
        ```python
        from synapseclient import Synapse
        from synapseclient.models import SubmissionView

        syn = Synapse()
        syn.login()

        my_submission_view = SubmissionView(
            name="My Submission View",
            parent_id="syn1234",
            scope_ids=["syn5678", "syn6789"],  # IDs of evaluation queues
        ).store()
        print(my_submission_view)
        ```
    """

    id: Optional[str] = None
    """
    The unique immutable ID for this entity. A new ID will be generated for new Entities.
    Once issued, this ID is guaranteed to never change or be re-issued.
    """

    name: Optional[str] = None
    """
    The name of this entity. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes, and parentheses.
    """

    description: Optional[str] = None
    """
    The description of this entity. Must be 1000 characters or less.
    """

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
    Since the E-Tag changes every time an entity is updated, it is used to detect when a client's
    current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """
    The date this entity was created.
    """

    modified_on: Optional[str] = field(default=None, compare=False)
    """
    The date this entity was last modified.
    """

    created_by: Optional[str] = field(default=None, compare=False)
    """
    The ID of the user that created this entity.
    """

    modified_by: Optional[str] = field(default=None, compare=False)
    """
    The ID of the user that last modified this entity.
    """

    parent_id: Optional[str] = None
    """
    The ID of the Entity that is the parent of this Entity.
    """

    version_number: Optional[int] = field(default=None, compare=False)
    """
    The version number issued to this version on the object.
    """

    version_label: Optional[str] = None
    """
    The version label for this entity.
    """

    version_comment: Optional[str] = None
    """
    The version comment for this entity.
    """

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """
    If this is the latest version of the object.
    """

    columns: Optional[
        Union[List[Column], OrderedDict[str, Column], Dict[str, Column]]
    ] = field(default_factory=OrderedDict, compare=False)
    """
    The columns of this submission view. This is an ordered dictionary where the key is the
    name of the column and the value is the Column object. When creating a new instance
    of a SubmissionView object you may pass any of the following types as the `columns` argument:

    - A list of Column objects
    - A dictionary where the key is the name of the column and the value is the Column object
    - An OrderedDict where the key is the name of the column and the value is the Column object

    The order of the columns will be the order they are stored in Synapse. If you need
    to reorder the columns the recommended approach is to use the `.reorder_column()`
    method. Additionally, you may add, and delete columns using the `.add_column()`,
    and `.delete_column()` methods on your view class instance.

    You may modify the attributes of the Column object to change the column
    type, name, or other attributes. For example, suppose you'd like to change a
    column from a INTEGER to a DOUBLE. You can do so by changing the column type
    attribute of the Column object. The next time you store the view the column
    will be updated in Synapse with the new type.

    ```python
    from synapseclient import Synapse
    from synapseclient.models import SubmissionView, Column, ColumnType

    syn = Synapse()
    syn.login()

    submission_view = SubmissionView(id="syn1234").get()
    submission_view.columns["my_column"].column_type = ColumnType.DOUBLE
    submission_view.store()
    ```

    Note that the keys in this dictionary should match the column names as they are in
    Synapse. However, know that the name attribute of the Column object is used for
    all interactions with the Synapse API. The OrderedDict key is purely for the usage
    of this interface. For example, if you wish to rename a column you may do so by
    changing the name attribute of the Column object. The key in the OrderedDict does
    not need to be changed. The next time you store the view the column will be updated
    in Synapse with the new name and the key in the OrderedDict will be updated.
    """

    _columns_to_delete: Optional[Dict[str, Column]] = field(default_factory=dict)
    """
    Columns to delete when the submission view is stored. The key in this dict is the ID of the
    column to delete. The value is the Column object that represents the column to
    delete.
    """

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analogous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

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
    """Additional metadata associated with the submission view. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`"""

    _last_persistent_instance: Optional["SubmissionView"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    is_search_enabled: Optional[bool] = None
    """
    When creating or updating a table or view, specifies if full-text search should be enabled.
    Note that enabling full-text search might slow down the indexing of the table or view.
    """

    scope_ids: List[str] = field(default_factory=list)
    """
    The list of container IDs that define the scope of this view. For submission views,
    this is the list of evaluation queues that the view is associated with.
    """

    view_entity_type: ViewEntityType = ViewEntityType.SUBMISSION_VIEW
    """The API model string for the type of view. This is used to determine the default columns that are
    added to the table. Must be defined as a `ViewEntityType` enum.
    """

    def __post_init__(self):
        self.columns = self._convert_columns_to_ordered_dict(columns=self.columns)

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance
            or self._last_persistent_instance != self
            or (not self._last_persistent_instance.scope_ids and self.scope_ids)
            or self._last_persistent_instance.scope_ids != self.scope_ids
        )

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store information about a SubmissionView including the annotations, columns,
        and scope. Updates to the `scope_ids` attribute will be cause the rows of the
        view to be updated. `scope_ids` is a list of evaluation queues that the view is
        associated with.

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
            The SubmissionView instance stored in synapse.

        Example: Create a new submission view.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                submission_view = SubmissionView(
                    name="My Submission View",
                    scope_ids=["syn9876543"],  # ID of an evaluation queue
                    parent_id="syn1234",
                )
                submission_view = await submission_view.store_async()
                print(f"Created Submission View with ID: {submission_view.id}")

            asyncio.run(main())
            ```

        Example: Update the scope of an existing submission view.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                submission_view = await SubmissionView(id="syn1234").get_async()
                submission_view.scope_ids = ["syn9876543", "syn8765432"]  # Add a second evaluation queue
                submission_view = await submission_view.store_async()
                print("Updated Submission View scope.")

            asyncio.run(main())
            ```
        """
        return await super().store_async(
            dry_run=dry_run,
            job_timeout=job_timeout,
            synapse_client=synapse_client,
        )

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """Retrieve a SubmissionView from Synapse.

        Arguments:
            include_columns: Whether to include the columns in the returned view.
                Defaults to True. This is useful when updating the columns.
            include_activity: Whether to include the activity in the returned view.
                Defaults to False. Setting this to True will include the activity
                record associated with this view.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The SubmissionView instance retrieved from Synapse.

        Example: Retrieving a submission view by ID.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                submission_view = await SubmissionView(id="syn1234").get_async()
                print(submission_view)

            asyncio.run(main())
            ```

        Example: Getting a submission view with its columns and activity.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                submission_view = await SubmissionView(id="syn1234").get_async(
                    include_columns=True,
                    include_activity=True
                )
                print(submission_view.columns)
                print(submission_view.activity)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete a SubmissionView from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a submission view.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                submission_view = SubmissionView(id="syn1234")
                await submission_view.delete_async()
                print("Deleted Submission View.")

            asyncio.run(main())
            ```
        """
        return await super().delete_async(synapse_client=synapse_client)

    async def snapshot_async(
        self,
        *,
        comment: Optional[str] = None,
        label: Optional[str] = None,
        include_activity: bool = True,
        associate_activity_to_new_version: bool = True,
        synapse_client: Optional[Synapse] = None,
    ) -> "TableUpdateTransaction":
        """Creates a snapshot of the `SubmissionView` entity.
        Synapse handles snapshot creation differently for `Table`- and `View`-like
        entities. `View` snapshots are created using the asyncronous job API.

        Making a snapshot of a view allows you to create an immutable version of the
        view at the time of the snapshot. This is useful to create checkpoints in time
        that you may go back and reference, or use in a publication. Snapshots are
        immutable and cannot be changed. They may only be deleted.

        Arguments:
            comment: A unique comment to associate with the snapshot.
            label: A unique label to associate with the snapshot. If this is not a
                unique label an exception will be raised when you store this to Synapse.
            include_activity: If True the activity will be included in snapshot if it
                exists. In order to include the activity, the activity must have already
                been stored in Synapse by using the `activity` attribute on the View
                and calling the `store_async()` method on the View instance. Adding an
                activity to a snapshot of a table is meant to capture the provenance of
                the data at the time of the snapshot. Defaults to True.
            associate_activity_to_new_version: If True the activity will be associated
                with the new version of the table. If False the activity will not be
                associated with the new version of the table. Defaults to True.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A `TableUpdateTransaction` object which includes the version number of the snapshot.

        Example: Creating a snapshot of a view with an activity
            Create a snapshot of a view and include the activity. The activity must have been stored in
            Synapse by using the `activity` attribute on the SubmissionView and calling the `store_async()`
            method on the SubmissionView instance. Adding an activity to a snapshot of a view is meant
            to capture the provenance of the data at the time of the snapshot.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView, Activity, UsedURL

            syn = Synapse()
            syn.login()

            async def main():
                view = await SubmissionView(id="syn4567").get_async()
                view.activity = Activity(
                    name="Activity for snapshot",
                    used=[UsedURL(name="Data Source", url="https://example.org")]
                )
                await view.store_async() # Store the activity

                snapshot = await view.snapshot_async(
                    label="Q1 2025",
                    comment="Submissions reviewed in Lab A",
                    include_activity=True,
                    associate_activity_to_new_version=True
                )
                print(snapshot)

            asyncio.run(main())
            ```

        Example: Creating a snapshot of a view without an activity
            Create a snapshot of a view without including the activity. This is used in
            cases where we do not have any Provenance to associate with the snapshot and
            we do not want to persist any activity that may be present on the view to
            the new version of the view.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionView

            syn = Synapse()
            syn.login()

            async def main():
                view = await SubmissionView(id="syn4567").get_async()
                snapshot = await view.snapshot_async(
                    label="Q1 2025",
                    comment="Submissions reviewed in Lab A",
                    include_activity=False,
                    associate_activity_to_new_version=False
                )
                print(snapshot)

            asyncio.run(main())
            ```
        """
        return await super().snapshot_async(
            comment=comment,
            label=label,
            include_activity=include_activity,
            associate_activity_to_new_version=associate_activity_to_new_version,
            synapse_client=synapse_client,
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
        self._last_persistent_instance.scope_ids = (
            deepcopy(self.scope_ids) if self.scope_ids else []
        )

    def fill_from_dict(self, entity, set_annotations: bool = True) -> "Self":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            synapse_table: The data coming from the Synapse API

        Returns:
            The SubmissionView object instance.
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
        self.scope_ids = [item for item in entity.get("scopeIds", [])]

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
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": concrete_types.SUBMISSION_VIEW,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "columnIds": (
                [
                    column.id
                    for column in self._last_persistent_instance.columns.values()
                ]
                if self._last_persistent_instance
                and self._last_persistent_instance.columns
                else []
            ),
            "isSearchEnabled": self.is_search_enabled,
            "scopeIds": ([item for item in self.scope_ids] if self.scope_ids else []),
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result
