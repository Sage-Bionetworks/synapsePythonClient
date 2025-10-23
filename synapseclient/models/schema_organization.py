"""
This file contains the SchemaOrganization and JSONSchema classes.
These are used to manage Organization and JSON Schema entities in Synapse.
"""

import re
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Generator, Optional, Protocol

from synapseclient import Synapse
from synapseclient.api import (
    create_organization,
    delete_json_schema,
    delete_organization,
    get_json_schema_body,
    get_organization,
    get_organization_acl,
    list_json_schema_versions,
    list_json_schemas,
    list_organizations_sync,
    update_organization_acl,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.constants.concrete_types import CREATE_SCHEMA_REQUEST
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo

SYNAPSE_SCHEMA_URL = f"{Synapse().repoEndpoint}/schema/type/registered/"


class SchemaOrganizationProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(self, synapse_client: Optional["Synapse"] = None) -> "SchemaOrganization":
        """
        Gets the metadata from Synapse for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Get an existing SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("dpetest")
            org.get()
            print(org)
            ```

        """
        return self

    def store(self, synapse_client: Optional["Synapse"] = None) -> "SchemaOrganization":
        """
        Stores this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Store the SchemaOrganization in Synapse
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.store()
            print(org)
            ```

        """
        return self

    def delete(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete the SchemaOrganization from Synapse
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.delete()
            ```
        """
        return None

    def get_json_schemas(
        self, synapse_client: Optional["Synapse"] = None
    ) -> Generator["JSONSchema", None, None]:
        """
        Gets the JSON Schemas that are part of this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns: A Generator containing the JSONSchemas that belong to this organization

        Example: Get the JSONSchemas that are part of this SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("dpetest")
            js_generator = org.get_json_schemas()
            for item in js_generator:
                print(item)
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=self.get_json_schemas_async,
            synapse_client=synapse_client,
        )

    def get_acl(self, synapse_client: Optional["Synapse"] = None) -> dict[str, Any]:
        """
        Gets the ACL for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A dictionary in the form of this response:
              [AccessControlList]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

        Example: Get the ACL for the SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.get_acl()
            ```
        """
        return {}

    def update_acl(
        self,
        principal_id: int,
        access_type: list[str],
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Updates the ACL for a principal for this organization

        Arguments:
            principal_id: the id of the principal whose permissions are to be updated
            access_type: List of permission types (e.g., ["READ", "CREATE", "DELETE"])
                see:
                  [ACCESS_TYPE]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/ACCESS_TYPE.html
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Update the ACL for the SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.update_acl(
                principal_id=1,
                access_type=["READ"]
            )
            ```
        """
        return None


@dataclass()
@async_to_sync
class SchemaOrganization(SchemaOrganizationProtocol):
    """
    Represents an [Organization](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html).
    """

    name: Optional[str] = None
    """The name of the organization"""

    id: Optional[str] = None
    """The ID of the organization"""

    created_on: Optional[str] = None
    """The date this organization was created"""

    created_by: Optional[str] = None
    """The ID of the user that created this organization"""

    def __post_init__(self) -> None:
        if self.name:
            _check_name(self.name)

    async def get_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "SchemaOrganization":
        """
        Gets the metadata from Synapse for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Raises:
            ValueError: If the name has not been set

        Example: Get an existing SchemaOrganization
            &nbsp;

            ```python

            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def get_org():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("dpetest")
                await org.get_async()
                return org

            org = asyncio.run(get_org())
            print(org.name)
            print(org.id)
            ```
        """
        if not self.name:
            raise ValueError("SchemaOrganization must have a name")
        response = await get_organization(self.name, synapse_client=synapse_client)
        self.fill_from_dict(response)
        return self

    async def store_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "SchemaOrganization":
        """
        Stores this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Raises:
            ValueError: If the name has not been set

        Example: Store a new SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def store_org():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("my.new.org")
                await org.store_async()
                return org

            org = asyncio.run(store_org())
            print(org.name)
            print(org.id)
            ```
        """
        if not self.name:
            raise ValueError("SchemaOrganization must have a name")
        response = await create_organization(self.name, synapse_client=synapse_client)
        self.fill_from_dict(response)
        return self

    async def delete_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete a SchemaOrganization

            Delete using a name

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def delete_org():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("my.org")
                await org.delete_async()

            asyncio.run(delete_org())
            ```

            Delete using an id

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def delete_org():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization(id=1075)
                await org.delete_async()

            asyncio.run(delete_org())
        """
        if not self.id:
            await self.get_async(synapse_client=synapse_client)
        await delete_organization(self.id, synapse_client=synapse_client)

    @skip_async_to_sync
    async def get_json_schemas_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator["JSONSchema", None]:
        """
        Gets the JSONSchemas that are part of this organization

        Returns: An AsyncGenerator of JSONSchemas

        Raises:
            ValueError: If the name has not been set

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Get a list of JSONSchemas that belong to the SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def get_schemas():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("dpetest")
                js_generator = org.get_json_schemas_async()
                async for item in js_generator:
                    print(item)

            asyncio.run(get_schemas())
            ```

        """
        if not self.name:
            raise ValueError("SchemaOrganization must have a name")
        response = list_json_schemas(self.name, synapse_client=synapse_client)
        async for item in response:
            yield JSONSchema().fill_from_dict(item)

    async def get_acl_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> dict[str, Any]:
        """
        Gets the ACL for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A dictionary in the form of this response:
              [AccessControlList]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

        Example: Get the ACL for a SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def get_acl():

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("dpetest")
                acl = await org.get_acl_async()
                return acl

            acl = asyncio.run(get_acl())
            etag = acl["etag"]
            print(etag)
            resource_access = acl["resourceAccess"]
            for item in resource_access:
                principal_id = item["principalId"]
                print((principal_id))
                access_types = item["accessType"]
                print(access_types)
            ```
        """
        if not self.id:
            await self.get_async(synapse_client=synapse_client)
        response = await get_organization_acl(self.id, synapse_client=synapse_client)
        return response

    async def update_acl_async(
        self,
        principal_id: int,
        access_type: list[str],
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Updates the ACL for a principal for this organization

        Arguments:
            principal_id: the id of the principal whose permissions are to be updated
            access_type: List of permission types (e.g., ["READ", "CREATE", "DELETE"])
                see:
                  [ACCESS_TYPE]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/ACCESS_TYPE.html
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

            Example: Update the ACL for a SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            async def update_acl() -> None:

                syn = Synapse()
                syn.login()

                org = SchemaOrganization("dpetest")
                await org.update_acl_async(
                    principal_id=1,
                    access_type=["READ"]
                )

            asyncio.run(update_acl())

        """
        acl = await self.get_acl_async(synapse_client=synapse_client)

        resource_access: list[dict[str, Any]] = acl["resourceAccess"]
        etag = acl["etag"]

        principal_id_match = False
        for permissions in resource_access:
            if permissions["principalId"] == principal_id:
                permissions["accessType"] = access_type
                principal_id_match = True

        if not principal_id_match:
            resource_access.append(
                {"principalId": principal_id, "accessType": access_type}
            )

        await update_organization_acl(
            organization_id=self.id,
            resource_access=resource_access,
            etag=etag,
            synapse_client=synapse_client,
        )

    def fill_from_dict(self, response: dict[str, Any]) -> "SchemaOrganization":
        """
        Fills in this classes attributes using a Synapse API response

        Args:
            response: A response from this endpoint:
              [Organization]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html

        Returns:
            Itself
        """
        self.name = response.get("name")
        self.id = response.get("id")
        self.created_on = response.get("createdOn")
        self.created_by = response.get("createdBy")
        return self


class JSONSchemaProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(self, synapse_client: Optional["Synapse"] = None) -> "JSONSchema":
        """
        Gets this JSONSchemas metadata

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Raises:
            ValueError: This JSONSchema doesn't exist in its organization

        Example: Get an Existing JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            js.get()
            print(js)
            ```
        """
        return self

    def store(
        self,
        schema_body: dict[str, Any],
        version: Optional[str] = None,
        dry_run: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> "JSONSchema":
        """
        Stores this JSONSchema in Synapse

        Arguments:
            schema_body: The body of the JSONSchema to store
            version: The version of the JSONSchema body to store
            dry_run: Whether or not to do a dry-run
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Store a JSON Schema in Synapse
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            schema = JSONSchema(organization_name="my.org", name="test.schema")
            schema_body = {
                {
                    "properties": {
                        "Component": {
                            "description": "TBD",
                            "not": {
                                "type": "null"
                            },
                            "title": "Component"
                        }
                    }
                }
            }
            schema.store(schema_body = schema_body)
            ```
        """
        return self

    def delete(
        self, version: Optional[str] = None, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """
        Deletes this JSONSchema

        Arguments:
            version: Defaults to None.
            - If a version is supplied, that version will be deleted.
            - If no version is supplied the whole schema will be deleted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete from Synapse

            Delete the entire schema

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            js.delete()
            ```

            Delete a specific version from Synapse

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            js.delete(version="0.0.1")
            ```
        """
        return None

    def get_versions(
        self, synapse_client: Optional["Synapse"] = None
    ) -> Generator["JSONSchemaVersionInfo", None, None]:
        """
        Gets all versions of this JSONSchema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A Generator containing the JSONSchemaVersionInfo for each version of this schema

        Example: Get all versions of the JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            schema = JSONSchema(organization_name="dpetest", name="test.schematic.Biospecimen")
            version_generator = schema.get_versions()
            for item in version_generator:
                print(item)
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=self.get_versions_async,
            synapse_client=synapse_client,
        )

    def get_body(
        self, version: Optional[str] = None, synapse_client: Optional["Synapse"] = None
    ) -> dict[str, Any]:
        """
        Gets the body of this JSONSchema.

        Arguments:
            version: Defaults to None.
            - If a version is supplied, that versions body will be returned.
            - If no version is supplied the most recent version will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            The JSON Schema body

        Example: Get the JSONSchema body from Synapse
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            # Get latest version
            latest = js.get_body()
            print(latest)
            # Get specific version
            first = js.get_body("0.0.1")
            print(first)
            ```
        """
        return {}


@dataclass()
@async_to_sync
class JSONSchema(JSONSchemaProtocol):
    """
    Represents a:
      [JSON Schema](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaInfo.html)

    Attributes:
        name: The name of the schema
        organization_name: The name of the organization the schema belongs to
        organization_id: the id of the organization the schema belongs to
        id: The ID of the schema
        created_on: The date this schema was created
        created_by: The ID of the user that created this schema
        uri: The schema identifier in format: <organization_name>-<schema_name>
    """

    name: Optional[str] = None
    """The name of the schema"""

    organization_name: Optional[str] = None
    """The name of the organization the schema belongs to"""

    organization_id: Optional[int] = None
    """The id of the organization the schema belongs to"""

    id: Optional[str] = None
    """The ID of the schema"""

    created_on: Optional[str] = None
    """The date this schema was created"""

    created_by: Optional[str] = None
    """The ID of the user that created this schema"""

    uri: Optional[str] = field(init=False)
    """The schema identifier in format: <organization_name>-<schema_name>"""

    def __post_init__(self) -> None:
        if self.name:
            _check_name(self.name)
        if self.organization_name:
            _check_name(self.organization_name)
        if self.name and self.organization_name:
            self.uri = f"{self.organization_name}-{self.name}"
        else:
            self.uri = None

    async def get_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchema":
        """
        Gets the metadata for this JSONSchema from Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Raises:
            ValueError: This JSONSchema doesn't have a name
            ValueError: This JSONSchema doesn't have an organization name
            ValueError: This JSONSchema doesn't exist in its organization

        Returns:
            Itself

        Example: Get an existing JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def get_schema():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="dpetest", name="test.schematic.Biospecimen")
                await schema.get_async()
                return schema

            schema = asyncio.run(get_schema())
            print(schema.uri)
            ```
        """
        if not self.name:
            raise ValueError("JSONSchema must have a name")
        if not self.organization_name:
            raise ValueError("JSONSchema must have a organization_name")

        # Check that the org exists,
        #  if it doesn't list_json_schemas will unhelpfully return an empty generator.
        org = SchemaOrganization(self.organization_name)
        await org.get_async(synapse_client=synapse_client)

        org_schemas = list_json_schemas(
            self.organization_name, synapse_client=synapse_client
        )
        async for schema in org_schemas:
            if schema["schemaName"] == self.name:
                self.fill_from_dict(schema)
                return self
        raise ValueError(
            (
                f"Organization: '{self.organization_name}' does not contain a schema with "
                f"name: '{self.name}'"
            )
        )

    async def store_async(
        self,
        schema_body: dict[str, Any],
        version: Optional[str] = None,
        dry_run: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> "JSONSchema":
        """
        Stores this JSONSchema in Synapse

        Arguments:
            schema_body: The body of the JSONSchema to store
            version: The version of the JSONSchema body to store
            dry_run: Whether or not to do a dry-run
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Store a JSON Schema in Synapse
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def store_schema():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="my.org", name="test.schema")
                schema_body = {
                    {
                        "properties": {
                            "Component": {
                                "description": "TBD",
                                "not": {
                                    "type": "null"
                                },
                                "title": "Component"
                            }
                        }
                    }
                }
                await schema.store_async(schema_body = schema_body)

            asyncio.run(store_schema())
            ```
        """
        if not self.name:
            raise ValueError("JSONSchema must have a name")
        if not self.organization_name:
            raise ValueError("JSONSchema must have a organization_name")

        request = CreateSchemaRequest(
            schema=schema_body,
            name=self.name,
            organization_name=self.organization_name,
            version=version,
            dry_run=dry_run,
        )
        completed_request: CreateSchemaRequest = await request.send_job_and_wait_async(
            synapse_client=synapse_client
        )
        new_version_info = completed_request.new_version_info
        self.organization_id = new_version_info.organization_id
        self.id = new_version_info.id
        self.created_by = new_version_info.created_by
        self.created_on = new_version_info.created_on
        return self

    async def delete_async(
        self, version: Optional[str] = None, synapse_client: Optional["Synapse"] = None
    ) -> None:
        """
        If a version is supplied the specific version is deleted from Synapse.
        Otherwise the entire schema is deleted from Synapse

        Arguments:
            version: Defaults to None.
            - If a version is supplied, that version will be deleted.
            - If no version is supplied the whole schema will be deleted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete an existing JSONSchema

            Delete the whole schema

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def delete_schema():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="my.org", name="test.schema")
                await schema.delete_async()

            asyncio.run(delete_schema())
            ```

            Delete a specific version of the schema

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def delete_schema():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="my.org", name="test.schema")
                await schema.delete_async(version = "0.0.1")

            asyncio.run(delete_schema())
            ```
        """
        if not self.name:
            raise ValueError("JSONSchema must have a name")
        if not self.organization_name:
            raise ValueError("JSONSchema must have a organization_name")

        uri = self.uri
        if version:
            self._check_semantic_version(version)
            uri = f"{uri}-{version}"

        await delete_json_schema(uri, synapse_client=synapse_client)

    @skip_async_to_sync
    async def get_versions_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> AsyncGenerator[JSONSchemaVersionInfo, None]:
        """
        Gets all versions of this JSONSchema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A generator containing each version of this schema

        Example: Get all the versions of the JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def get_versions():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="dpetest", name="test.schematic.Biospecimen")
                version_generator = schema.get_versions_async()
                async for item in version_generator:
                    print(item)

            asyncio.run(get_versions())
            ```
        """
        all_schemas = list_json_schema_versions(
            self.organization_name, self.name, synapse_client=synapse_client
        )
        async for schema in all_schemas:
            # Schema "versions" without a semantic version will be returned from the API call,
            # but will be filtered out by this method.
            # Only those with a semantic version will be returned.
            if "semanticVersion" in schema:
                yield self._create_json_schema_version_from_response(schema)

    async def get_body_async(
        self, version: Optional[str] = None, synapse_client: Optional["Synapse"] = None
    ) -> dict[str, Any]:
        """
        Gets the body of this JSONSchema

        Arguments:
            version: Defaults to None.
            - If a version is supplied, that versions body will be returned.
            - If no version is supplied the most recent version will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            The JSON Schema body

        Example: Get the body of the JSONSchema

            Get latest version

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def get_body():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="dpetest", name="test.schematic.Biospecimen")
                body = await schema.get_body_async()
                return body

            body = asyncio.run(get_body())
            print(body)
            ```

            Get specific version

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            async def get_body():

                syn = Synapse()
                syn.login()

                schema = JSONSchema(organization_name="dpetest", name="test.schematic.Biospecimen")
                body = await schema.get_body_async(version="0.0.1")
                return body

            body = asyncio.run(get_body())
            print(body)
            ```

        """
        if not self.name:
            raise ValueError("JSONSchema must have a name")
        if not self.organization_name:
            raise ValueError("JSONSchema must have a organization_name")

        uri = self.uri
        if version:
            self._check_semantic_version(version)
            uri = f"{uri}-{version}"
        response = await get_json_schema_body(uri, synapse_client=synapse_client)
        return response

    def fill_from_dict(self, response: dict[str, Any]) -> "JSONSchema":
        """
        Fills in this classes attributes using a Synapse API response

        Arguments:
            response: This Synapse API object:
              [JsonSchema](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)

        Returns:
            Itself
        """
        self.organization_id = response.get("organizationId")
        self.organization_name = response.get("organizationName")
        self.id = response.get("schemaId")
        self.name = response.get("schemaName")
        self.created_on = response.get("createdOn")
        self.created_by = response.get("createdBy")
        self.uri = f"{self.organization_name}-{self.name}"
        return self

    @classmethod
    def from_uri(cls, uri: str) -> "JSONSchema":
        """
        Creates a JSONSchema from a URI.
        The URI can either be a semantic version or not
        - non-semantic: ORGANIZATION.NAME-SCHEMA.NAME
        - semantic: ORGANIZATION.NAME-SCHEMA.NAME-MAJOR.MINOR.PATCH

        Arguments:
            uri: The URI to the JSON Schema in Synapse

        Raises:
            ValueError:  If the URI isn't in the correct form.

        Returns:
            A JSONSchema object

        Example: Create a JSONSchema from a URI
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema

            # Non-semantic URI
            js1 = JSONSchema.from_uri("my.org-my.schema")

            # Semantic URI
            js2 = JSONSchema.from_uri("my.org-my.schema-0.0.1")
            ```

        """
        uri_parts = uri.split("-")
        if len(uri_parts) > 3 or len(uri_parts) < 2:
            msg = (
                "The URI must be in the form of "
                "'<ORGANIZATION>-<NAME>' or '<ORGANIZATION>-<NAME>-<VERSION>': "
                f"{uri}"
            )
            raise ValueError(msg)
        return JSONSchema(name=uri_parts[1], organization_name=uri_parts[0])

    @staticmethod
    def _create_json_schema_version_from_response(
        response: dict[str, Any]
    ) -> JSONSchemaVersionInfo:
        """
        Creates a JSONSchemaVersionInfo object from a Synapse API response

        Arguments:
            response: This Synapse API object:
                [JsonSchemaVersionInfo](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html)

        Returns:
            A JSONSchemaVersionInfo object
        """
        return JSONSchemaVersionInfo(
            organization_id=response.get("organizationId"),
            organization_name=response.get("organizationName"),
            schema_id=response.get("schemaId"),
            id=response.get("$id"),
            schema_name=response.get("schemaName"),
            version_id=response.get("versionId"),
            semantic_version=response.get("semanticVersion"),
            json_sha256_hex=response.get("jsonSHA256Hex"),
            created_on=response.get("createdOn"),
            created_by=response.get("createdBy"),
        )

    def _check_semantic_version(self, version: str) -> None:
        """
        Checks that the semantic version is correctly formatted

        Arguments:
            version: A semantic version(ie. `1.0.0`) to be checked

        Raises:
            ValueError: If the string is not a correct semantic version
        """
        if version == "0.0.0":
            raise ValueError("Schema version must start at '0.0.1' or higher")
        if not re.match(r"^(\d+)\.(\d+)\.(\d+)$", version):
            raise ValueError(
                (
                    "Schema version must be a semantic version with no letters "
                    "and a major, minor and patch version, such as 0.0.1: "
                    f"{version}"
                )
            )


@dataclass
class CreateSchemaRequest(AsynchronousCommunicator):
    """
    This class is for creating a [CreateSchemaRequest]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/CreateSchemaRequest.html
    """

    schema: dict[str, Any]
    """The JSON Schema to be stored"""

    name: str
    """The name of the schema being stored"""

    organization_name: str
    """The name of the organization the schema to store the schema in"""

    version: Optional[str] = None
    """The version to store the schema as if given"""

    dry_run: bool = False
    """Whether or not to do the request as a dry-run"""

    concrete_type: str = field(init=False)
    """The concrete type of the request"""

    uri: str = field(init=False)
    """The URI of this schema"""

    id: str = field(init=False)
    """The ID/URL of this schema"""

    new_version_info: JSONSchemaVersionInfo = None
    """Info from the API response"""

    def __post_init__(self) -> None:
        self.concrete_type = CREATE_SCHEMA_REQUEST
        _check_name(self.name)
        _check_name(self.organization_name)
        uri = f"{self.organization_name}-{self.name}"
        if self.version:
            self._check_semantic_version(self.version)
            uri = f"{uri}-{self.version}"
        self.uri = uri
        self.id = f"{SYNAPSE_SCHEMA_URL}{uri}"
        self.schema["$id"] = self.id

    def to_synapse_request(self) -> dict[str, Any]:
        """
        Create a CreateSchemaRequest from attributes
        [CreateSchemaRequest]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/CreateSchemaRequest.html
        """

        result = {
            "concreteType": self.concrete_type,
            "schema": self.schema,
            "dryRun": self.dry_run,
        }

        return result

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "CreateSchemaRequest":
        """
        Set attributes from
        [CreateSchemaResponse]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/CreateSchemaResponse.html
        """
        self.new_version_info = self._create_json_schema_version_from_response(
            synapse_response.get("newVersionInfo")
        )
        self.schema = synapse_response.get("validationSchema")

        return self

    def _check_semantic_version(self, version: str) -> None:
        """
        Checks that the semantic version is correctly formatted

        Args:
            version: A semantic version(ie. `1.0.0`) to be checked

        Raises:
            ValueError: If the string is not a correct semantic version
        """
        if not re.match(r"^(\d+)\.(\d+)\.(\d*)$", version) or version == "0.0.0":
            raise ValueError(
                (
                    "Schema version must be a semantic version starting at 0.0.1 with no letters "
                    "and a major, minor and patch version "
                    f"{version}"
                )
            )

    @staticmethod
    def _create_json_schema_version_from_response(
        response: dict[str, Any]
    ) -> JSONSchemaVersionInfo:
        """
        Creates a JSONSchemaVersionInfo object from a Synapse API response

        Arguments:
            response: This Synapse API object:
                [JsonSchemaVersionInfo]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html

        Returns:
            A JSONSchemaVersionInfo object
        """
        return JSONSchemaVersionInfo(
            organization_id=response.get("organizationId"),
            organization_name=response.get("organizationName"),
            schema_id=response.get("schemaId"),
            id=response.get("$id"),
            schema_name=response.get("schemaName"),
            version_id=response.get("versionId"),
            semantic_version=response.get("semanticVersion"),
            json_sha256_hex=response.get("jsonSHA256Hex"),
            created_on=response.get("createdOn"),
            created_by=response.get("createdBy"),
        )


def list_json_schema_organizations(
    synapse_client: Optional["Synapse"] = None,
) -> list[SchemaOrganization]:
    """
    Lists all the Schema Organizations currently in Synapse

    Arguments:
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

    Returns:
        A list of SchemaOrganizations

    Example:
        from synapseclient.models.schema_organization import list_json_schema_organizations
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

        all_orgs = list_json_schema_organizations()
        for item in all_orgs:
            print(item)
    """
    all_orgs = [
        SchemaOrganization().fill_from_dict(org)
        for org in list_organizations_sync(synapse_client=synapse_client)
    ]
    return all_orgs


def _check_name(name: str) -> None:
    """
    Checks that the input name is a valid Synapse Organization or JSONSchema name
    - Length requirement of 6 ≤ x ≤ 250
    - Names do not contain the string sagebionetworks (case insensitive)
    - May contain periods (each part is separated by periods)
    - Each part must start with a letter
    - Each part contains only letters and numbers

    Arguments:
        name: The name of the organization to be checked

    Raises:
        ValueError: When the name isn't valid
    """
    if not 6 <= len(name) <= 250:
        raise ValueError(f"The name must be of length 6 to 250 characters: {name}")
    if re.search("sagebionetworks", name.lower()):
        raise ValueError(f"The name must not contain 'sagebionetworks' : {name}")
    parts = name.split(".")
    for part in parts:
        if not re.match(r"^([A-Za-z])([A-Za-z]|\d|)*$", part):
            raise ValueError(
                (
                    "Name may be separated by periods, "
                    "but each part must start with a letter and contain "
                    f"only letters and numbers: {name}"
                )
            )
