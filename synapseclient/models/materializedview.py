from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Activity
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    DeleteMixin,
    GetMixin,
    QueryMixin,
    TableBase,
    ViewStoreMixin,
)


class MaterializedViewSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for MaterializedView operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store non-row information about a MaterializedView including the annotations.

        Note: Columns in a MaterializedView are determined by the `defining_sql` attribute. To update
        the columns, you must update the `defining_sql` and store the view.

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
            The MaterializedView instance stored in synapse.

        Example: Create a new materialized view with a defining SQL query.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(
                name="My Materialized View",
                description="A test materialized view",
                parent_id="syn12345",
                defining_sql="SELECT * FROM syn67890"
            )
            materialized_view = materialized_view.store()
            print(f"Created Materialized View with ID: {materialized_view.id}")
            ```

        Example: Update the defining SQL of an existing materialized view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(id="syn12345").get()
            materialized_view.defining_sql = "SELECT column1, column2 FROM syn67890"
            materialized_view = materialized_view.store()
            print("Updated Materialized View defining SQL.")
            ```

        Example: Retrieve and update annotations for a materialized view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(id="syn12345").get()
            materialized_view.annotations["key1"] = ["value1"]
            materialized_view.annotations["key2"] = ["value2"]
            materialized_view.store()
            print("Updated annotations for Materialized View.")
            ```

        Example: Create a materialized view with a JOIN clause.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            defining_sql = '''
            SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
            FROM syn12345 t1
            JOIN syn67890 t2
            ON t1.id = t2.foreign_id
            '''

            materialized_view = MaterializedView(
                name="Join Materialized View",
                description="A materialized view with a JOIN clause",
                parent_id="syn11111",
                defining_sql=defining_sql,
            )
            materialized_view = materialized_view.store()
            print(f"Created Materialized View with ID: {materialized_view.id}")
            ```

        Example: Create a materialized view with a LEFT JOIN clause.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            defining_sql = '''
            SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
            FROM syn12345 t1
            LEFT JOIN syn67890 t2
            ON t1.id = t2.foreign_id
            '''

            materialized_view = MaterializedView(
                name="Left Join Materialized View",
                description="A materialized view with a LEFT JOIN clause",
                parent_id="syn11111",
                defining_sql=defining_sql,
            )
            materialized_view = materialized_view.store()
            print(f"Created Materialized View with ID: {materialized_view.id}")
            ```

        Example: Create a materialized view with a RIGHT JOIN clause.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            defining_sql = '''
            SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
            FROM syn12345 t1
            RIGHT JOIN syn67890 t2
            ON t1.id = t2.foreign_id
            '''

            materialized_view = MaterializedView(
                name="Right Join Materialized View",
                description="A materialized view with a RIGHT JOIN clause",
                parent_id="syn11111",
                defining_sql=defining_sql,
            )
            materialized_view = materialized_view.store()
            print(f"Created Materialized View with ID: {materialized_view.id}")
            ```

        Example: Create a materialized view with a UNION clause.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            defining_sql = '''
            SELECT column1 AS new_column1, column2 AS new_column2
            FROM syn12345
            UNION
            SELECT column1 AS new_column1, column2 AS new_column2
            FROM syn67890
            '''

            materialized_view = MaterializedView(
                name="Union Materialized View",
                description="A materialized view with a UNION clause",
                parent_id="syn11111",
                defining_sql=defining_sql,
            )
            materialized_view = materialized_view.store()
            print(f"Created Materialized View with ID: {materialized_view.id}")
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
        Get the metadata about the MaterializedView from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the MaterializedView
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The MaterializedView instance stored in synapse.

        Example: Getting metadata about a MaterializedView using id
            Get a MaterializedView by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(id="syn4567").get(include_activity=True)
            print(materialized_view)

            # Columns are retrieved by default
            print(materialized_view.columns)
            print(materialized_view.activity)
            ```

        Example: Getting metadata about a MaterializedView using name and parent_id
            Get a MaterializedView by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(name="my_materialized_view", parent_id="syn1234").get(include_columns=True, include_activity=True)
            print(materialized_view)
            print(materialized_view.columns)
            print(materialized_view.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the materialized view from synapse. This is not version specific. If you'd like
        to delete a specific version of the materialized view you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Delete a materialized view.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            syn = Synapse()
            syn.login()

            materialized_view = MaterializedView(id="syn12345")
            materialized_view.delete()
            print("Deleted Materialized View.")
            ```
        """
        return None


@dataclass
@async_to_sync
class MaterializedView(
    MaterializedViewSynchronousProtocol,
    AccessControllable,
    TableBase,
    ViewStoreMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
):
    """
    A materialized view is a type of table that is automatically built from a Synapse
    SQL query. Its content is read only and based off the `defining_sql` attribute.
    The SQL of the materialized view may contain JOIN clauses on multiple tables.

    A `MaterializedView` object represents this concept in Synapse:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/MaterializedView.html>

    Attributes:
        id: The unique immutable ID for this entity. Once issued, this ID is
            guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses.
        description: The description of this entity. Must be 1000 characters or less.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is
            updated it is used to detect when a client's current representation of an
            entity is out-of-date.
        created_on: The date this entity was created.
        modified_on: The date this entity was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ
            format.
        created_by: The ID of the user that created this entity.
        modified_by: The ID of the user that last modified this entity.
        parent_id: The ID of the Entity that is the parent of this entity.
        version_number: The version number issued to this version on the object.
        version_label: The version label for this entity.
        version_comment: The version comment for this entity.
        is_latest_version: If this is the latest version of the object.
        columns: (Read Only) The columns of a materialized view are dynamic based on
            the select statement of the definingSQL. This list of columnIds is for
            read-only purposes.
        is_search_enabled: When creating or updating a table or view specifies if full
            text search should be enabled.
        defining_sql: The synapse SQL statement that defines the data in the
            materialized view.
        annotations: Additional metadata associated with the entityview. The key is
            the name of your desired annotations. The value is an object containing a
            list of values (use empty list to represent no values for key) and the
            value type associated with all values in the list. To remove all
            annotations set this to an empty dict `{}` or None and store the entity.
        activity: The Activity model represents the main record of Provenance in
            Synapse.

    Example: Create a new materialized view with a defining SQL query.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        materialized_view = MaterializedView(
            name="My Materialized View",
            description="A test materialized view",
            parent_id="syn12345",
            defining_sql="SELECT * FROM syn67890"
        )
        materialized_view = materialized_view.store()
        print(f"Created Materialized View with ID: {materialized_view.id}")
        ```

    Example: Update the defining SQL of an existing materialized view.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        materialized_view = MaterializedView(id="syn12345").get()
        materialized_view.defining_sql = "SELECT column1, column2 FROM syn67890"
        materialized_view = materialized_view.store()
        print("Updated Materialized View defining SQL.")
        ```

    Example: Delete a materialized view.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        materialized_view = MaterializedView(id="syn12345")
        materialized_view.delete()
        print("Deleted Materialized View.")
        ```

    Example: Query data from a materialized view.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import query

        syn = Synapse()
        syn.login()

        query_result = query("SELECT * FROM syn66080386")
        print(query_result)
        ```

    Example: Retrieve and update annotations for a materialized view.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        materialized_view = MaterializedView(id="syn12345").get()
        materialized_view.annotations["key1"] = ["value1"]
        materialized_view.annotations["key2"] = ["value2"]
        materialized_view.store()
        print("Updated annotations for Materialized View.")
        ```

    Example: Create a materialized view with a JOIN clause.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        defining_sql = '''
        SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
        FROM syn12345 t1
        JOIN syn67890 t2
        ON t1.id = t2.foreign_id
        '''

        materialized_view = MaterializedView(
            name="Join Materialized View",
            description="A materialized view with a JOIN clause",
            parent_id="syn11111",
            defining_sql=defining_sql,
        )
        materialized_view = materialized_view.store()
        print(f"Created Materialized View with ID: {materialized_view.id}")
        ```

    Example: Create a materialized view with a LEFT JOIN clause.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        defining_sql = '''
        SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
        FROM syn12345 t1
        LEFT JOIN syn67890 t2
        ON t1.id = t2.foreign_id
        '''

        materialized_view = MaterializedView(
            name="Left Join Materialized View",
            description="A materialized view with a LEFT JOIN clause",
            parent_id="syn11111",
            defining_sql=defining_sql,
        )
        materialized_view = materialized_view.store()
        print(f"Created Materialized View with ID: {materialized_view.id}")
        ```

    Example: Create a materialized view with a RIGHT JOIN clause.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        defining_sql = '''
        SELECT t1.column1 AS new_column1, t2.column2 AS new_column2
        FROM syn12345 t1
        RIGHT JOIN syn67890 t2
        ON t1.id = t2.foreign_id
        '''

        materialized_view = MaterializedView(
            name="Right Join Materialized View",
            description="A materialized view with a RIGHT JOIN clause",
            parent_id="syn11111",
            defining_sql=defining_sql,
        )
        materialized_view = materialized_view.store()
        print(f"Created Materialized View with ID: {materialized_view.id}")
        ```

    Example: Create a materialized view with a UNION clause.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import MaterializedView

        syn = Synapse()
        syn.login()

        defining_sql = '''
        SELECT column1 AS new_column1, column2 AS new_column2
        FROM syn12345
        UNION
        SELECT column1 AS new_column1, column2 AS new_column2
        FROM syn67890
        '''

        materialized_view = MaterializedView(
            name="Union Materialized View",
            description="A materialized view with a UNION clause",
            parent_id="syn11111",
            defining_sql=defining_sql,
        )
        materialized_view = materialized_view.store()
        print(f"Created Materialized View with ID: {materialized_view.id}")
        ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. Once issued, this ID is
    guaranteed to never change or be re-issued."""

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less. Names may only
    contain: letters, numbers, spaces, underscores, hyphens, periods, plus
    signs, apostrophes, and parentheses."""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is
    updated it is used to detect when a client's current representation of an
    entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """The date this entity was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The date this entity was last modified. In YYYY-MM-DD-Thh:mm:ss.sssZ
    format."""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this entity."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this entity."""

    version_number: Optional[int] = field(default=None, compare=False)
    """The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this entity."""

    version_comment: Optional[str] = None
    """The version comment for this entity."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """If this is the latest version of the object."""

    columns: Optional[OrderedDict] = field(default_factory=OrderedDict, compare=False)
    """(Read Only) The columns of a materialized view are dynamic based on
    the select statement of the definingSQL. This list of columnIds is for
    read-only purposes."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a table or view specifies if full text search
    should be enabled."""

    defining_sql: Optional[str] = None
    """The synapse SQL statement that defines the data in the materialized
    view."""

    _last_persistent_instance: Optional["MaterializedView"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

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
    """Additional metadata associated with the entityview. The key is the name
    of your desired annotations. The value is an object containing a list of
    values (use empty list to represent no values for key) and the value type
    associated with all values in the list. To remove all annotations set this
    to an empty dict `{}` or None and store the entity."""

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in
    Synapse."""

    @property
    def has_changed(self) -> bool:
        """Checks if the object has changed since the last persistent instance."""
        return self._last_persistent_instance != self

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = replace(self)
        self._last_persistent_instance.activity = (
            replace(self.activity) if self.activity and self.activity.id else None
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

    def fill_from_dict(
        self, entity: Dict, set_annotations: bool = True
    ) -> "MaterializedView":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse API

        Returns:
            The MaterializedView object instance.
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
        self.defining_sql = entity.get("definingSQL", None)

        if set_annotations:
            self.annotations = entity.get("annotations", {})

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
            "concreteType": concrete_types.MATERIALIZED_VIEW,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "isSearchEnabled": self.is_search_enabled,
            "definingSQL": self.defining_sql,
        }
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    async def store_async(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Asynchronously store non-row information about a MaterializedView including the annotations.

        Note: Columns in a MaterializedView are determined by the `defining_sql` attribute. To update
        the columns, you must update the `defining_sql` and store the view.

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
            The MaterializedView instance stored in synapse.

        Example: Create a new materialized view with a defining SQL query.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            async def main():
                syn = Synapse()
                await syn.login_async()

                materialized_view = MaterializedView(
                    name="My Materialized View",
                    description="A test materialized view",
                    parent_id="syn12345",
                    defining_sql="SELECT * FROM syn67890"
                )
                materialized_view = await materialized_view.store_async()
                print(f"Created Materialized View with ID: {materialized_view.id}")

            asyncio.run(main())
            ```
        """
        return await super().store_async(
            dry_run=dry_run, job_timeout=job_timeout, synapse_client=synapse_client
        )

    async def get_async(
        self,
        include_columns: bool = True,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Asynchronously get the metadata about the MaterializedView from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the MaterializedView
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The MaterializedView instance stored in synapse.

        Example: Retrieve a materialized view by ID.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            async def main():
                syn = Synapse()
                await syn.login_async()

                materialized_view = await MaterializedView(id="syn12345").get_async()
                print(materialized_view)

            asyncio.run(main())
            ```
        """
        return await super().get_async(
            include_columns=include_columns,
            include_activity=include_activity,
            synapse_client=synapse_client,
        )

    async def delete_async(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Asynchronously delete the materialized view from synapse. This is not version specific. If you'd like
        to delete a specific version of the materialized view you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Example: Delete a materialized view.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import MaterializedView

            async def main():
                syn = Synapse()
                await syn.login_async()

                materialized_view = MaterializedView(id="syn12345")
                await materialized_view.delete_async()
                print("Deleted Materialized View.")

            asyncio.run(main())
            ```
        """
        await super().delete_async(synapse_client=synapse_client)
