import re
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Activity, Column
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    DeleteMixin,
    GetMixin,
    QueryMixin,
    TableBase,
    TableStoreMixin,
)


class VirtualTableSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for VirtualTable operations."""

    def store(
        self,
        dry_run: bool = False,
        *,
        job_timeout: int = 600,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store metadata about a VirtualTable including the annotations.

        Note: Columns and rows in a VirtualTable are determined by the `defining_sql` attribute. To update
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
            The VirtualTable instance stored in synapse.

        Raises:
            ValueError: If the defining_sql contains JOIN or UNION operations,
                which are not supported in VirtualTables.

        Example: Create a new virtual table with a defining SQL query.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            syn = Synapse()
            syn.login()

            virtual_table = VirtualTable(
                name="My Virtual Table",
                description="A test virtual table",
                parent_id="syn12345",
                defining_sql="SELECT * FROM syn67890"
            )
            virtual_table = virtual_table.store()
            print(f"Created Virtual Table with ID: {virtual_table.id}")
            ```

        Example: Update the defining SQL of an existing virtual table.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            syn = Synapse()
            syn.login()

            virtual_table = VirtualTable(id="syn12345").get()
            virtual_table.defining_sql = "SELECT column1, column2 FROM syn67890"
            virtual_table = virtual_table.store()
            print("Updated Virtual Table defining SQL.")
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
        Get the metadata about the VirtualTable from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the VirtualTable
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The VirtualTable instance stored in synapse.

        Example: Getting metadata about a VirtualTable using id
            Get a VirtualTable by ID and print out the columns and activity. `include_columns`
            defaults to True and `include_activity` defaults to False. When you need to
            update existing columns or activity these need to be set to True during the
            `get` call, then you'll make the changes, and finally call the
            `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            syn = Synapse()
            syn.login()

            virtual_table = VirtualTable(id="syn4567").get(include_activity=True)
            print(virtual_table)

            # Columns are retrieved by default
            print(virtual_table.columns)
            print(virtual_table.activity)
            ```

        Example: Getting metadata about a VirtualTable using name and parent_id
            Get a VirtualTable by name/parent_id and print out the columns and activity.
            `include_columns` defaults to True and `include_activity` defaults to
            False. When you need to update existing columns or activity these need to
            be set to True during the `get` call, then you'll make the changes,
            and finally call the `.store()` method.

            ```python
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            syn = Synapse()
            syn.login()

            virtual_table = VirtualTable(name="my_virtual_table", parent_id="syn1234").get(
                include_columns=True,
                include_activity=True
            )
            print(virtual_table)
            print(virtual_table.columns)
            print(virtual_table.activity)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """Delete the virtual table from synapse. This is not version specific. If you'd like
        to delete a specific version of the virtual table you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a virtual table.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            syn = Synapse()
            syn.login()

            virtual_table = VirtualTable(id="syn12345")
            virtual_table.delete()
            print("Deleted Virtual Table.")
            ```
        """
        return None


@dataclass
@async_to_sync
class VirtualTable(
    VirtualTableSynchronousProtocol,
    AccessControllable,
    TableBase,
    TableStoreMixin,
    DeleteMixin,
    GetMixin,
    QueryMixin,
):
    """
    A virtual table is a type of table that is dynamically built from a Synapse
    SQL query. Its content is read only and based off the `defining_sql` attribute.
    The SQL of the virtual table may NOT contain JOIN or UNION clauses and must
    reference a table that has a non-empty schema.

    A `VirtualTable` object represents this `VirtualTable` API model in Synapse:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/VirtualTable.html>

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
        columns: (Read Only) The columns of a virtual table are dynamic based on
            the select statement of the definingSQL. This list of columnIds is for
            read-only purposes.
        is_search_enabled: When creating or updating a table or view specifies if full
            text search should be enabled.
        defining_sql: The synapse SQL statement that defines the data in the
            virtual table. This field may NOT contain JOIN or UNION clauses.
            If a JOIN or UNION clause is present, a `ValueError` will be raised
            when the `store` method is called.
        annotations: Additional metadata associated with the entityview. The key is
            the name of your desired annotations. The value is an object containing a
            list of values (use empty list to represent no values for key) and the
            value type associated with all values in the list. To remove all
            annotations set this to an empty dict `{}` or None and store the entity.
        activity: The Activity model represents the main record of Provenance in
            Synapse.

    Example: Create a new virtual table with a defining SQL query.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import VirtualTable

        syn = Synapse()
        syn.login()

        virtual_table = VirtualTable(
            name="My Virtual Table",
            description="A test virtual table",
            parent_id="syn12345",
            defining_sql="SELECT * FROM syn67890"
        )
        virtual_table = virtual_table.store()
        print(f"Created Virtual Table with ID: {virtual_table.id}")
        ```

    Example: Update the defining SQL of an existing virtual table.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import VirtualTable

        syn = Synapse()
        syn.login()

        virtual_table = VirtualTable(id="syn12345").get()
        virtual_table.defining_sql = "SELECT column1, column2 FROM syn67890"
        virtual_table = virtual_table.store()
        print("Updated Virtual Table defining SQL.")
        ```

    Example: Delete a virtual table.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import VirtualTable

        syn = Synapse()
        syn.login()

        virtual_table = VirtualTable(id="syn12345")
        virtual_table.delete()
        print("Deleted Virtual Table.")
        ```

    Example: Query data from a virtual table.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import query

        syn = Synapse()
        syn.login()

        query_result = query("SELECT * FROM syn66080386")
        print(query_result)
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

    columns: Optional[OrderedDict[str, Column]] = field(
        default_factory=OrderedDict, compare=False
    )
    """(Read Only) The columns of a virtual table are dynamic based on
    the select statement of the definingSQL. This list of columnIds is for
    read-only purposes."""

    is_search_enabled: Optional[bool] = None
    """When creating or updating a table or view specifies if full text search
    should be enabled."""

    defining_sql: Optional[str] = None
    """The synapse SQL statement that defines the data in the virtual
    table. This field may NOT contain JOIN or UNION clauses. If a JOIN or UNION
    clause is present, a `ValueError` will be raised when the `store`
    method is called."""

    _last_persistent_instance: Optional["VirtualTable"] = field(
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
        self, entity: Dict[str, Any], set_annotations: bool = True
    ) -> "VirtualTable":
        """
        Converts the data coming from the Synapse API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse API

        Returns:
            The VirtualTable object instance.
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

    def to_synapse_request(self) -> Dict[str, Any]:
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
            "concreteType": concrete_types.VIRTUAL_TABLE,
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
        Asynchronously store metadata about a VirtualTable including the annotations.

        Note: Columns and rows in a VirtualTable are determined by the `defining_sql` attribute. To update
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
            The VirtualTable instance stored in synapse.

        Raises:
            ValueError: If the defining_sql contains JOIN or UNION operations,
                which are not supported in VirtualTables.

        Example: Create a new virtual table with a defining SQL query.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            async def main():
                syn = Synapse()
                await syn.login_async()

                virtual_table = VirtualTable(
                    name="My Virtual Table",
                    description="A test virtual table",
                    parent_id="syn12345",
                    defining_sql="SELECT * FROM syn67890"
                )
                virtual_table = await virtual_table.store_async()
                print(f"Created Virtual Table with ID: {virtual_table.id}")

            asyncio.run(main())
            ```
        """
        # Check for unsupported operations in defining_sql

        if self.defining_sql:
            sql_upper = self.defining_sql.upper()
            join_union_pattern = r"(?:^|\s)(?:JOIN|UNION)(?:\s|$)"

            if re.search(join_union_pattern, sql_upper):
                raise ValueError(
                    "VirtualTables do not support JOIN or UNION operations in the defining_sql. "
                    "If you need to combine data from multiple tables, consider using a MaterializedView instead."
                )

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
        Asynchronously get the metadata about the VirtualTable from synapse.

        Arguments:
            include_columns: If True, will include fully filled column objects in the
                `.columns` attribute. Defaults to True.
            include_activity: If True the activity will be included in the VirtualTable
                if it exists. Defaults to False.

            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The VirtualTable instance stored in synapse.

        Example: Retrieve a virtual table by ID.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            async def main():
                syn = Synapse()
                await syn.login_async()

                virtual_table = await VirtualTable(id="syn12345").get_async()
                print(virtual_table)

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
        Asynchronously delete the virtual table from synapse. This is not version specific. If you'd like
        to delete a specific version of the virtual table you must use the
        [synapseclient.api.delete_entity][] function directly.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a virtual table.
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import VirtualTable

            async def main():
                syn = Synapse()
                await syn.login_async()

                virtual_table = VirtualTable(id="syn12345")
                await virtual_table.delete_async()
                print("Deleted Virtual Table.")

            asyncio.run(main())
            ```
        """
        await super().delete_async(synapse_client=synapse_client)
